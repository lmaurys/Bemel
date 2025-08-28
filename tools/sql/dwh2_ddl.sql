-- Azure SQL DDL for Dwh2 star schema based on UniversalTransaction (AR) and UniversalShipment (CSL)
-- Conventions: Schema Dwh2, Dim/Fact prefixes, PascalCase names, SK as IDENTITY, UpdatedAt on all tables

-- Create schema if not exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Dwh2')
    EXEC('CREATE SCHEMA Dwh2');
GO

/*
  Reset: remove ALL existing tables in schema Dwh2 so only the model remains.
  - First drop all foreign keys within Dwh2
  - Then drop all tables in Dwh2
  This is destructive. Ensure backups as needed before running in production.
*/
DECLARE @sql NVARCHAR(MAX);
-- Drop FKs in Dwh2
SET @sql = N'';
SELECT @sql = @sql + N'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name)
             + N' DROP CONSTRAINT ' + QUOTENAME(fk.name) + N';' + NCHAR(10)
FROM sys.foreign_keys fk
JOIN sys.tables t ON fk.parent_object_id = t.object_id
WHERE SCHEMA_NAME(t.schema_id) = 'Dwh2';
IF LEN(@sql) > 0 EXEC sp_executesql @sql;

-- Drop tables in Dwh2
SET @sql = N'';
SELECT @sql = @sql + N'DROP TABLE ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name) + N';' + NCHAR(10)
FROM sys.tables t
WHERE SCHEMA_NAME(t.schema_id) = 'Dwh2';
IF LEN(@sql) > 0 EXEC sp_executesql @sql;
GO

/* Dimensions */

-- Date dimension (surrogate DateKey and calendar attributes)
CREATE TABLE Dwh2.DimDate (
  DateKey            INT            NOT NULL PRIMARY KEY, -- e.g., 20250709
  [Date]             DATE           NOT NULL UNIQUE,
  [Year]             INT            NOT NULL,
  [Quarter]          TINYINT        NOT NULL,
  [Month]            TINYINT        NOT NULL,
  DayOfMonth         TINYINT        NOT NULL,
  DayOfWeek          TINYINT        NOT NULL,
  MonthName          NVARCHAR(20)   NOT NULL,
  DayName            NVARCHAR(20)   NOT NULL,
  UpdatedAt          DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimCountry (
  CountryKey   INT IDENTITY(1,1) PRIMARY KEY,
  Code         NVARCHAR(10)  NOT NULL UNIQUE,
  [Name]       NVARCHAR(200) NOT NULL,
  UpdatedAt    DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimCurrency (
  CurrencyKey  INT IDENTITY(1,1) PRIMARY KEY,
  Code         NVARCHAR(10)  NOT NULL UNIQUE,
  [Description] NVARCHAR(200) NULL,
  UpdatedAt    DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimPort (
  PortKey      INT IDENTITY(1,1) PRIMARY KEY,
  Code         NVARCHAR(20)  NOT NULL UNIQUE,
  [Name]       NVARCHAR(200) NOT NULL,
  CountryKey   INT NULL,
  UpdatedAt    DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT FK_DimPort_DimCountry FOREIGN KEY (CountryKey) REFERENCES Dwh2.DimCountry (CountryKey)
);
GO

CREATE TABLE Dwh2.DimCompany (
  CompanyKey   INT IDENTITY(1,1) PRIMARY KEY,
  Code         NVARCHAR(50)  NOT NULL UNIQUE,
  [Name]       NVARCHAR(200) NOT NULL,
  CountryKey   INT NULL,
  UpdatedAt    DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT FK_DimCompany_DimCountry FOREIGN KEY (CountryKey) REFERENCES Dwh2.DimCountry (CountryKey)
);
GO

CREATE TABLE Dwh2.DimUser (
  UserKey      INT IDENTITY(1,1) PRIMARY KEY,
  Code         NVARCHAR(50)  NOT NULL UNIQUE,
  [Name]       NVARCHAR(200) NOT NULL,
  UpdatedAt    DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimDepartment (
  DepartmentKey INT IDENTITY(1,1) PRIMARY KEY,
  Code          NVARCHAR(50)  NOT NULL UNIQUE,
  [Name]        NVARCHAR(200) NOT NULL,
  UpdatedAt     DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimEventType (
  EventTypeKey  INT IDENTITY(1,1) PRIMARY KEY,
  Code          NVARCHAR(50)  NOT NULL UNIQUE,
  [Description] NVARCHAR(200) NOT NULL,
  UpdatedAt     DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimActionPurpose (
  ActionPurposeKey INT IDENTITY(1,1) PRIMARY KEY,
  Code             NVARCHAR(50)  NOT NULL UNIQUE,
  [Description]    NVARCHAR(200) NOT NULL,
  UpdatedAt        DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimRecipientRole (
  RecipientRoleKey INT IDENTITY(1,1) PRIMARY KEY,
  Code             NVARCHAR(50)  NOT NULL UNIQUE,
  [Description]    NVARCHAR(200) NOT NULL,
  UpdatedAt        DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimAccountGroup (
  AccountGroupKey  INT IDENTITY(1,1) PRIMARY KEY,
  Code             NVARCHAR(50)  NOT NULL UNIQUE,
  [Description]    NVARCHAR(200) NOT NULL,
  [Type]           NVARCHAR(10)  NOT NULL, -- e.g., 'AR'/'AP'
  UpdatedAt        DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimScreeningStatus (
  ScreeningStatusKey INT IDENTITY(1,1) PRIMARY KEY,
  Code               NVARCHAR(50)  NOT NULL UNIQUE,
  [Description]      NVARCHAR(200) NOT NULL,
  UpdatedAt          DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimServiceLevel (
  ServiceLevelKey  INT IDENTITY(1,1) PRIMARY KEY,
  Code             NVARCHAR(50)  NOT NULL UNIQUE,
  [Description]    NVARCHAR(200) NOT NULL,
  ServiceLevelType NVARCHAR(20)  NOT NULL, -- e.g., 'AWB','Gateway'
  UpdatedAt        DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimContainerMode (
  ContainerModeKey INT IDENTITY(1,1) PRIMARY KEY,
  Code             NVARCHAR(50)  NOT NULL UNIQUE,
  [Description]    NVARCHAR(200) NULL,
  UpdatedAt        DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimPaymentMethod (
  PaymentMethodKey INT IDENTITY(1,1) PRIMARY KEY,
  Code             NVARCHAR(50)  NOT NULL UNIQUE,
  [Description]    NVARCHAR(200) NOT NULL,
  UpdatedAt        DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimUnit (
  UnitKey        INT IDENTITY(1,1) PRIMARY KEY,
  Code           NVARCHAR(20)  NOT NULL UNIQUE,
  [Description]  NVARCHAR(200) NULL,
  UnitType       NVARCHAR(20)  NOT NULL, -- e.g., 'Volume','Weight','CO2e','Packs'
  UpdatedAt      DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimBranch (
  BranchKey      INT IDENTITY(1,1) PRIMARY KEY,
  Code           NVARCHAR(50)  NOT NULL UNIQUE,
  [Name]         NVARCHAR(200) NOT NULL,
  AddressType    NVARCHAR(50)  NULL,
  Address1       NVARCHAR(200) NULL,
  Address2       NVARCHAR(200) NULL,
  AddressOverride NVARCHAR(200) NULL,
  AddressShortCode NVARCHAR(50) NULL,
  City           NVARCHAR(100) NULL,
  [State]        NVARCHAR(100) NULL,
  Postcode       NVARCHAR(20)  NULL,
  CountryKey     INT NULL,
  PortKey        INT NULL,
  Email          NVARCHAR(200) NULL,
  Fax            NVARCHAR(50)  NULL,
  Phone          NVARCHAR(50)  NULL,
  ScreeningStatusKey INT NULL,
  UpdatedAt      DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT FK_DimBranch_DimCountry FOREIGN KEY (CountryKey) REFERENCES Dwh2.DimCountry (CountryKey),
  CONSTRAINT FK_DimBranch_DimPort FOREIGN KEY (PortKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_DimBranch_DimScreeningStatus FOREIGN KEY (ScreeningStatusKey) REFERENCES Dwh2.DimScreeningStatus (ScreeningStatusKey)
);
GO

CREATE TABLE Dwh2.DimOrganization (
  OrganizationKey INT IDENTITY(1,1) PRIMARY KEY,
  OrganizationCode NVARCHAR(50) NOT NULL UNIQUE,
  CompanyName    NVARCHAR(200) NULL,
  GovRegNum      NVARCHAR(100) NULL,
  GovRegNumTypeCode NVARCHAR(50) NULL,
  GovRegNumTypeDescription NVARCHAR(200) NULL,
  AddressType    NVARCHAR(50)  NULL,
  Address1       NVARCHAR(200) NULL,
  Address2       NVARCHAR(200) NULL,
  AddressOverride NVARCHAR(200) NULL,
  AddressShortCode NVARCHAR(50) NULL,
  City           NVARCHAR(100) NULL,
  [State]        NVARCHAR(100) NULL,
  Postcode       NVARCHAR(20)  NULL,
  CountryKey     INT NULL,
  PortKey        INT NULL,
  Email          NVARCHAR(200) NULL,
  Fax            NVARCHAR(50)  NULL,
  Phone          NVARCHAR(50)  NULL,
  UpdatedAt      DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT FK_DimOrganization_DimCountry FOREIGN KEY (CountryKey) REFERENCES Dwh2.DimCountry (CountryKey),
  CONSTRAINT FK_DimOrganization_DimPort FOREIGN KEY (PortKey) REFERENCES Dwh2.DimPort (PortKey)
);
GO

/* Organization Registration Numbers as a dimension out-rigger (multiple per organization/address type) */
IF NOT EXISTS (
  SELECT 1 FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id
  WHERE o.name = 'DimOrganizationRegistrationNumber' AND s.name = 'Dwh2' AND o.type = 'U'
)
BEGIN
  CREATE TABLE Dwh2.DimOrganizationRegistrationNumber (
    OrganizationRegistrationNumberKey INT IDENTITY(1,1) PRIMARY KEY,
    OrganizationKey INT NOT NULL,
    AddressType NVARCHAR(50) NULL,
    TypeCode NVARCHAR(50) NULL,
    TypeDescription NVARCHAR(200) NULL,
    CountryOfIssueCode NVARCHAR(10) NULL,
    CountryOfIssueName NVARCHAR(200) NULL,
    [Value] NVARCHAR(300) NOT NULL,
    UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_DimOrgRegNum_Organization FOREIGN KEY (OrganizationKey) REFERENCES Dwh2.DimOrganization (OrganizationKey)
  );
  CREATE INDEX IX_DimOrgRegNum_Org ON Dwh2.DimOrganizationRegistrationNumber (OrganizationKey);
END
GO

CREATE TABLE Dwh2.DimJob (
  JobDimKey      INT IDENTITY(1,1) PRIMARY KEY,
  JobType        NVARCHAR(50) NOT NULL,
  JobKey         NVARCHAR(50) NOT NULL,
  UpdatedAt      DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT UQ_DimJob UNIQUE (JobType, JobKey)
);
GO

CREATE TABLE Dwh2.DimEnterprise (
  EnterpriseKey  INT IDENTITY(1,1) PRIMARY KEY,
  EnterpriseId   NVARCHAR(50) NOT NULL UNIQUE,
  UpdatedAt      DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimServer (
  ServerKey      INT IDENTITY(1,1) PRIMARY KEY,
  ServerId       NVARCHAR(50) NOT NULL UNIQUE,
  UpdatedAt      DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimDataProvider (
  DataProviderKey INT IDENTITY(1,1) PRIMARY KEY,
  ProviderCode    NVARCHAR(100) NOT NULL UNIQUE,
  UpdatedAt       DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE Dwh2.DimCo2eStatus (
  Co2eStatusKey  INT IDENTITY(1,1) PRIMARY KEY,
  Code           NVARCHAR(50) NOT NULL UNIQUE,
  [Description]  NVARCHAR(200) NOT NULL,
  UpdatedAt      DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

/* Facts */

/* Control: one row per processed file (AR/CSL) parsed from filename */
IF OBJECT_ID('Dwh2.FactFileIngestion','U') IS NOT NULL DROP TABLE Dwh2.FactFileIngestion;
CREATE TABLE Dwh2.FactFileIngestion (
  FactFileIngestionKey INT IDENTITY(1,1) PRIMARY KEY,
  Source NVARCHAR(20) NOT NULL, -- 'CSL' | 'AR'
  FileName NVARCHAR(300) NOT NULL,
  FileDateKey INT NULL,
  FileTime TIME(3) NULL,
  LoadedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT FK_FactFileIngestion_FileDate FOREIGN KEY (FileDateKey) REFERENCES Dwh2.DimDate (DateKey)
);
GO

IF NOT EXISTS (
  SELECT 1 FROM sys.indexes WHERE name = 'UX_FactFileIngestion_FileName' AND object_id = OBJECT_ID('Dwh2.FactFileIngestion')
)
CREATE UNIQUE INDEX UX_FactFileIngestion_FileName
  ON Dwh2.FactFileIngestion (FileName);
GO

CREATE TABLE Dwh2.FactAccountsReceivableTransaction (
  FactAccountsReceivableTransactionKey INT IDENTITY(1,1) PRIMARY KEY,

  -- Context dims
  CompanyKey        INT NULL,
  BranchKey         INT NULL,
  DepartmentKey     INT NULL,
  EventTypeKey      INT NULL,
  ActionPurposeKey  INT NULL,
  UserKey           INT NULL,
  EnterpriseKey     INT NULL,
  ServerKey         INT NULL,
  DataProviderKey   INT NULL,

  -- Dates (join to DimDate)
  TransactionDateKey INT NULL,
  PostDateKey         INT NULL,
  DueDateKey          INT NULL,
  TriggerDateKey      INT NULL,
  TriggerDateTime     DATETIME2(3) NULL,

  -- Other dims
  AccountGroupKey    INT NULL,
  LocalCurrencyKey   INT NULL,
  OSCurrencyKey      INT NULL,
  OrganizationKey    INT NULL,
  PlaceOfIssuePortKey INT NULL,
  JobDimKey          INT NULL,

  -- Business identifiers / attributes
  DataSourceType           NVARCHAR(50)  NULL,
  DataSourceKey            NVARCHAR(100) NULL,
  [Number]                 NVARCHAR(50) NOT NULL,
  Ledger                   NVARCHAR(50) NULL,
  [Category]               NVARCHAR(100) NULL,
  InvoiceTerm              NVARCHAR(100) NULL,
  InvoiceTermDays          INT NULL,
  JobInvoiceNumber         NVARCHAR(50) NULL,
  CheckDrawer              NVARCHAR(200) NULL,
  CheckNumberOrPaymentRef  NVARCHAR(100) NULL,
  DrawerBank               NVARCHAR(200) NULL,
  DrawerBranch             NVARCHAR(200) NULL,
  ReceiptOrDirectDebitNumber NVARCHAR(100) NULL,
  RequisitionStatus        NVARCHAR(100) NULL,
  TransactionReference     NVARCHAR(200) NULL,
  TransactionType          NVARCHAR(100) NULL,
  AgreedPaymentMethod      NVARCHAR(100) NULL,
  ComplianceSubType        NVARCHAR(100) NULL,
  CreateTime               NVARCHAR(50)  NULL,
  CreateUser               NVARCHAR(100) NULL,
  EventReference           NVARCHAR(200) NULL,
  [Timestamp]              NVARCHAR(50)  NULL,
  TriggerCount             INT           NULL,
  TriggerDescription       NVARCHAR(200) NULL,
  TriggerType              NVARCHAR(100) NULL,
  NumberOfSupportingDocuments INT       NULL,

  -- Measures
  LocalExVATAmount         DECIMAL(18,4) NULL,
  LocalVATAmount           DECIMAL(18,4) NULL,
  LocalTaxTransactionsAmount DECIMAL(18,4) NULL,
  LocalTotal               DECIMAL(18,4) NULL,
  OSExGSTVATAmount         DECIMAL(18,4) NULL,
  OSGSTVATAmount           DECIMAL(18,4) NULL,
  OSTaxTransactionsAmount  DECIMAL(18,4) NULL,
  OSTotal                  DECIMAL(18,4) NULL,
  OutstandingAmount        DECIMAL(18,4) NULL,
  ExchangeRate             DECIMAL(18,6) NULL,

  -- Flags
  IsCancelled              BIT NULL,
  IsCreatedByMatchingProcess BIT NULL,
  IsPrinted                BIT NULL,

  -- Text attributes where coded dims may not be available
  PlaceOfIssueText         NVARCHAR(200) NULL,

  UpdatedAt                DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT UQ_FactAR_Number UNIQUE ([Number]),
  CONSTRAINT FK_FactAR_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactAR_Branch FOREIGN KEY (BranchKey) REFERENCES Dwh2.DimBranch (BranchKey),
  CONSTRAINT FK_FactAR_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactAR_EventType FOREIGN KEY (EventTypeKey) REFERENCES Dwh2.DimEventType (EventTypeKey),
  CONSTRAINT FK_FactAR_ActionPurpose FOREIGN KEY (ActionPurposeKey) REFERENCES Dwh2.DimActionPurpose (ActionPurposeKey),
  CONSTRAINT FK_FactAR_User FOREIGN KEY (UserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactAR_Enterprise FOREIGN KEY (EnterpriseKey) REFERENCES Dwh2.DimEnterprise (EnterpriseKey),
  CONSTRAINT FK_FactAR_Server FOREIGN KEY (ServerKey) REFERENCES Dwh2.DimServer (ServerKey),
  CONSTRAINT FK_FactAR_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey),
  CONSTRAINT FK_FactAR_AccountGroup FOREIGN KEY (AccountGroupKey) REFERENCES Dwh2.DimAccountGroup (AccountGroupKey),
  CONSTRAINT FK_FactAR_LocalCurrency FOREIGN KEY (LocalCurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactAR_OSCurrency FOREIGN KEY (OSCurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactAR_Organization FOREIGN KEY (OrganizationKey) REFERENCES Dwh2.DimOrganization (OrganizationKey),
  CONSTRAINT FK_FactAR_PlaceOfIssuePort FOREIGN KEY (PlaceOfIssuePortKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactAR_Job FOREIGN KEY (JobDimKey) REFERENCES Dwh2.DimJob (JobDimKey),
  CONSTRAINT FK_FactAR_TransactionDate FOREIGN KEY (TransactionDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactAR_PostDate FOREIGN KEY (PostDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactAR_DueDate FOREIGN KEY (DueDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactAR_TriggerDate FOREIGN KEY (TriggerDateKey) REFERENCES Dwh2.DimDate (DateKey)
);
GO

CREATE TABLE Dwh2.FactShipment (
  FactShipmentKey        INT IDENTITY(1,1) PRIMARY KEY,

  -- Context dims
  CompanyKey             INT NULL,
  BranchKey              INT NULL,
  DepartmentKey          INT NULL,
  EventTypeKey           INT NULL,
  ActionPurposeKey       INT NULL,
  UserKey                INT NULL,
  EnterpriseKey          INT NULL,
  ServerKey              INT NULL,
  DataProviderKey        INT NULL,

  TriggerDateKey         INT NULL,
  TriggerDateTime        DATETIME2(3) NULL,

  -- Shipment identifiers via DataSource
  ConsolJobKey           INT NULL,
  ShipmentJobKey         INT NULL,

  -- Ports / places
  PlaceOfDeliveryKey     INT NULL,
  PlaceOfIssueKey        INT NULL,
  PlaceOfReceiptKey      INT NULL,
  PortFirstForeignKey    INT NULL,
  PortLastForeignKey     INT NULL,
  PortOfDischargeKey     INT NULL,
  PortOfFirstArrivalKey  INT NULL,
  PortOfLoadingKey       INT NULL,
  EventBranchHomePortKey INT NULL,

  -- Dims
  AWBServiceLevelKey     INT NULL,
  GatewayServiceLevelKey INT NULL,
  ShipmentTypeKey        INT NULL,
  ReleaseTypeKey         INT NULL,
  ScreeningStatusKey     INT NULL,
  PaymentMethodKey       INT NULL,
  FreightRateCurrencyKey INT NULL,
  TotalVolumeUnitKey     INT NULL,
  TotalWeightUnitKey     INT NULL,
  CO2eUnitKey            INT NULL,
  PacksUnitKey           INT NULL,
  ContainerModeKey       INT NULL,
  Co2eStatusKey          INT NULL,

  -- Attributes
  AgentsReference                 NVARCHAR(200) NULL,
  BookingConfirmationReference    NVARCHAR(100) NULL,
  CarrierContractNumber           NVARCHAR(100) NULL,
  ElectronicBillOfLadingReference NVARCHAR(100) NULL,
  CarrierBookingOfficeCode        NVARCHAR(50)  NULL,
  CarrierBookingOfficeName        NVARCHAR(200) NULL,

  -- Measures
  ContainerCount                  INT NULL,
  ChargeableRate                  DECIMAL(18,4) NULL,
  DocumentedChargeable            DECIMAL(18,4) NULL,
  DocumentedVolume                DECIMAL(18,6) NULL,
  DocumentedWeight                DECIMAL(18,4) NULL,
  FreightRate                     DECIMAL(18,4) NULL,
  GreenhouseGasEmissionCO2e       DECIMAL(18,4) NULL,
  ManifestedChargeable            DECIMAL(18,4) NULL,
  ManifestedVolume                DECIMAL(18,6) NULL,
  ManifestedWeight                DECIMAL(18,4) NULL,
  MaximumAllowablePackageHeight   DECIMAL(18,3) NULL,
  MaximumAllowablePackageLength   DECIMAL(18,3) NULL,
  MaximumAllowablePackageWidth    DECIMAL(18,3) NULL,
  NoCopyBills                     INT NULL,
  NoOriginalBills                 INT NULL,
  OuterPacks                      INT NULL,
  TotalNoOfPacks                  INT NULL,
  TotalPreallocatedChargeable     DECIMAL(18,4) NULL,
  TotalPreallocatedVolume         DECIMAL(18,6) NULL,
  TotalPreallocatedWeight         DECIMAL(18,4) NULL,
  TotalVolume                     DECIMAL(18,6) NULL,
  TotalWeight                     DECIMAL(18,4) NULL,

  -- Flags
  IsCFSRegistered                 BIT NULL,
  IsDirectBooking                 BIT NULL,
  IsForwardRegistered             BIT NULL,
  IsHazardous                     BIT NULL,
  IsNeutralMaster                 BIT NULL,
  RequiresTemperatureControl      BIT NULL,

  UpdatedAt                       DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  -- FKs
  CONSTRAINT FK_FactShipment_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactShipment_Branch FOREIGN KEY (BranchKey) REFERENCES Dwh2.DimBranch (BranchKey),
  CONSTRAINT FK_FactShipment_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactShipment_EventType FOREIGN KEY (EventTypeKey) REFERENCES Dwh2.DimEventType (EventTypeKey),
  CONSTRAINT FK_FactShipment_ActionPurpose FOREIGN KEY (ActionPurposeKey) REFERENCES Dwh2.DimActionPurpose (ActionPurposeKey),
  CONSTRAINT FK_FactShipment_User FOREIGN KEY (UserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactShipment_Enterprise FOREIGN KEY (EnterpriseKey) REFERENCES Dwh2.DimEnterprise (EnterpriseKey),
  CONSTRAINT FK_FactShipment_Server FOREIGN KEY (ServerKey) REFERENCES Dwh2.DimServer (ServerKey),
  CONSTRAINT FK_FactShipment_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey),
  CONSTRAINT FK_FactShipment_TriggerDate FOREIGN KEY (TriggerDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactShipment_ConsolJob FOREIGN KEY (ConsolJobKey) REFERENCES Dwh2.DimJob (JobDimKey),
  CONSTRAINT FK_FactShipment_ShipmentJob FOREIGN KEY (ShipmentJobKey) REFERENCES Dwh2.DimJob (JobDimKey),
  CONSTRAINT FK_FactShipment_PlaceOfDelivery FOREIGN KEY (PlaceOfDeliveryKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactShipment_PlaceOfIssue FOREIGN KEY (PlaceOfIssueKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactShipment_PlaceOfReceipt FOREIGN KEY (PlaceOfReceiptKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactShipment_PortFirstForeign FOREIGN KEY (PortFirstForeignKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactShipment_PortLastForeign FOREIGN KEY (PortLastForeignKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactShipment_PortOfDischarge FOREIGN KEY (PortOfDischargeKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactShipment_PortOfFirstArrival FOREIGN KEY (PortOfFirstArrivalKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactShipment_PortOfLoading FOREIGN KEY (PortOfLoadingKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactShipment_EventBranchHomePort FOREIGN KEY (EventBranchHomePortKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactShipment_AWBServiceLevel FOREIGN KEY (AWBServiceLevelKey) REFERENCES Dwh2.DimServiceLevel (ServiceLevelKey),
  CONSTRAINT FK_FactShipment_GatewayServiceLevel FOREIGN KEY (GatewayServiceLevelKey) REFERENCES Dwh2.DimServiceLevel (ServiceLevelKey),
  CONSTRAINT FK_FactShipment_ShipmentType FOREIGN KEY (ShipmentTypeKey) REFERENCES Dwh2.DimServiceLevel (ServiceLevelKey),
  CONSTRAINT FK_FactShipment_ReleaseType FOREIGN KEY (ReleaseTypeKey) REFERENCES Dwh2.DimServiceLevel (ServiceLevelKey),
  CONSTRAINT FK_FactShipment_ScreeningStatus FOREIGN KEY (ScreeningStatusKey) REFERENCES Dwh2.DimScreeningStatus (ScreeningStatusKey),
  CONSTRAINT FK_FactShipment_PaymentMethod FOREIGN KEY (PaymentMethodKey) REFERENCES Dwh2.DimPaymentMethod (PaymentMethodKey),
  CONSTRAINT FK_FactShipment_FreightRateCurrency FOREIGN KEY (FreightRateCurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactShipment_TotalVolumeUnit FOREIGN KEY (TotalVolumeUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactShipment_TotalWeightUnit FOREIGN KEY (TotalWeightUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactShipment_CO2eUnit FOREIGN KEY (CO2eUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactShipment_PacksUnit FOREIGN KEY (PacksUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactShipment_ContainerMode FOREIGN KEY (ContainerModeKey) REFERENCES Dwh2.DimContainerMode (ContainerModeKey),
  CONSTRAINT FK_FactShipment_Co2eStatus FOREIGN KEY (Co2eStatusKey) REFERENCES Dwh2.DimCo2eStatus (Co2eStatusKey)
);
GO

/* Optional: useful nonclustered indexes for fact table foreign keys */
CREATE INDEX IX_FactAR_Number ON Dwh2.FactAccountsReceivableTransaction ([Number]);
CREATE INDEX IX_FactAR_Dates ON Dwh2.FactAccountsReceivableTransaction (TransactionDateKey, PostDateKey, DueDateKey);
GO


/* Bridge for Recipient Roles per AR */
IF NOT EXISTS (
  SELECT 1 FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id
  WHERE o.name = 'BridgeFactARRecipientRole' AND s.name = 'Dwh2' AND o.type = 'U'
)
BEGIN
  CREATE TABLE Dwh2.BridgeFactARRecipientRole (
    FactAccountsReceivableTransactionKey INT NOT NULL,
    RecipientRoleKey INT NOT NULL,
    UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_BridgeFactARRecipientRole PRIMARY KEY (FactAccountsReceivableTransactionKey, RecipientRoleKey),
    CONSTRAINT FK_BridgeFactARRecipientRole_Fact FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
    CONSTRAINT FK_BridgeFactARRecipientRole_Role FOREIGN KEY (RecipientRoleKey) REFERENCES Dwh2.DimRecipientRole (RecipientRoleKey)
  );
END
GO

/* Message numbers attached to AR or CSL (unified) */
IF OBJECT_ID('Dwh2.FactMessageNumber','U') IS NOT NULL DROP TABLE Dwh2.FactMessageNumber;
CREATE TABLE Dwh2.FactMessageNumber (
  FactMessageNumberKey INT IDENTITY(1,1) PRIMARY KEY,
  -- exactly one parent must be set
  FactShipmentKey INT NULL,
  FactAccountsReceivableTransactionKey INT NULL,

  Source NVARCHAR(20) NOT NULL, -- 'CSL' | 'AR'
  [Value] NVARCHAR(200) NOT NULL,
  [Type] NVARCHAR(50) NULL,

  CompanyKey INT NULL,
  DepartmentKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT CK_FactMessageNumber_OneParent CHECK (
    (CASE WHEN FactShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactAccountsReceivableTransactionKey IS NOT NULL THEN 1 ELSE 0 END) = 1
  ),
  CONSTRAINT FK_FactMessageNumber_FactShipment
    FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
  CONSTRAINT FK_FactMessageNumber_FactAR
    FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactMessageNumber_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactMessageNumber_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactMessageNumber_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactMessageNumber_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey)
);
GO

CREATE INDEX IX_FactShipment_Ports ON Dwh2.FactShipment (PortOfLoadingKey, PortOfDischargeKey, PlaceOfReceiptKey, PlaceOfDeliveryKey);
CREATE INDEX IX_FactShipment_Service ON Dwh2.FactShipment (AWBServiceLevelKey, GatewayServiceLevelKey, ShipmentTypeKey);
GO

-- Ensure no duplicates per ShipmentJobKey
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes WHERE name = 'UX_FactShipment_ShipmentJobKey' AND object_id = OBJECT_ID('Dwh2.FactShipment')
)
CREATE UNIQUE INDEX UX_FactShipment_ShipmentJobKey
  ON Dwh2.FactShipment (ShipmentJobKey)
  WHERE ShipmentJobKey IS NOT NULL;
GO

/* Exceptions raised on Shipments (CSL) or AR (UniversalTransaction) */
IF OBJECT_ID('Dwh2.FactException','U') IS NOT NULL DROP TABLE Dwh2.FactException;
CREATE TABLE Dwh2.FactException (
  FactExceptionKey INT IDENTITY(1,1) PRIMARY KEY,
  -- exactly one parent must be set
  FactShipmentKey INT NULL,
  FactAccountsReceivableTransactionKey INT NULL,

  Source NVARCHAR(20) NOT NULL, -- 'CSL' | 'AR'
  ExceptionId NVARCHAR(100) NULL,
  Code NVARCHAR(50) NULL,
  [Type] NVARCHAR(100) NULL,
  Severity NVARCHAR(50) NULL,
  [Status] NVARCHAR(50) NULL,
  [Description] NVARCHAR(1000) NULL,
  IsResolved BIT NULL,
  -- Additional attributes from ExceptionCollection
  Actioned BIT NULL,
  ActionedDateKey INT NULL,
  ActionedTime NVARCHAR(12) NULL,
  Category NVARCHAR(100) NULL,
  EventDateKey INT NULL,
  EventTime NVARCHAR(12) NULL,
  DurationHours INT NULL,
  LocationCode NVARCHAR(100) NULL,
  LocationName NVARCHAR(200) NULL,
  Notes NVARCHAR(1000) NULL,
  StaffCode NVARCHAR(100) NULL,
  StaffName NVARCHAR(200) NULL,

  RaisedDateKey INT NULL,
  RaisedTime NVARCHAR(12) NULL,
  ResolvedDateKey INT NULL,
  ResolvedTime NVARCHAR(12) NULL,

  CompanyKey INT NULL,
  DepartmentKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT CK_FactException_OneParent CHECK (
    (CASE WHEN FactShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactAccountsReceivableTransactionKey IS NOT NULL THEN 1 ELSE 0 END) = 1
  ),
  CONSTRAINT FK_FactException_FactShipment
    FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
  CONSTRAINT FK_FactException_FactAR
    FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactException_ActionedDate FOREIGN KEY (ActionedDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactException_EventDate FOREIGN KEY (EventDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactException_RaisedDate FOREIGN KEY (RaisedDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactException_ResolvedDate FOREIGN KEY (ResolvedDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactException_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactException_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactException_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactException_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey)
);
GO

/* Helpful indexes for exceptions */
CREATE INDEX IX_FactException_Shipment ON Dwh2.FactException(FactShipmentKey) WHERE FactShipmentKey IS NOT NULL;
CREATE INDEX IX_FactException_AR ON Dwh2.FactException(FactAccountsReceivableTransactionKey) WHERE FactAccountsReceivableTransactionKey IS NOT NULL;
CREATE INDEX IX_FactException_RaisedDate ON Dwh2.FactException(RaisedDateKey);
GO

-- (moved) FactEventDate created after FactSubShipment to satisfy FK dependencies
/* Bridges: link multiple organizations to a fact (AR and Shipment) */
IF NOT EXISTS (
  SELECT 1 FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id
  WHERE o.name = 'BridgeFactAROrganization' AND s.name = 'Dwh2' AND o.type = 'U'
)
BEGIN
  CREATE TABLE Dwh2.BridgeFactAROrganization (
    FactAccountsReceivableTransactionKey INT NOT NULL,
    OrganizationKey INT NOT NULL,
    AddressType NVARCHAR(50) NOT NULL CONSTRAINT DF_BridgeFactAROrganization_AddressType DEFAULT N'',
    UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_BridgeFactAROrganization PRIMARY KEY (FactAccountsReceivableTransactionKey, OrganizationKey, AddressType),
    CONSTRAINT FK_BridgeFactAROrganization_Fact FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
    CONSTRAINT FK_BridgeFactAROrganization_Org FOREIGN KEY (OrganizationKey) REFERENCES Dwh2.DimOrganization (OrganizationKey)
  );
END
GO

IF NOT EXISTS (
  SELECT 1 FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id
  WHERE o.name = 'BridgeFactShipmentOrganization' AND s.name = 'Dwh2' AND o.type = 'U'
)
BEGIN
  CREATE TABLE Dwh2.BridgeFactShipmentOrganization (
    FactShipmentKey INT NOT NULL,
    OrganizationKey INT NOT NULL,
    AddressType NVARCHAR(50) NOT NULL CONSTRAINT DF_BridgeFactShipmentOrganization_AddressType DEFAULT N'',
    UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_BridgeFactShipmentOrganization PRIMARY KEY (FactShipmentKey, OrganizationKey, AddressType),
    CONSTRAINT FK_BridgeFactShipmentOrganization_Fact FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
    CONSTRAINT FK_BridgeFactShipmentOrganization_Org FOREIGN KEY (OrganizationKey) REFERENCES Dwh2.DimOrganization (OrganizationKey)
  );
END
GO

-- Additional integrity and performance indexes

-- DimOrganizationRegistrationNumber uniqueness: per Organization + AddressType + Value
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes WHERE name = 'UX_OrgRegNum_OrgAddrVal' AND object_id = OBJECT_ID('Dwh2.DimOrganizationRegistrationNumber')
)
CREATE UNIQUE INDEX UX_OrgRegNum_OrgAddrVal
  ON Dwh2.DimOrganizationRegistrationNumber (OrganizationKey, AddressType, [Value])
  WHERE AddressType IS NOT NULL;
GO
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes WHERE name = 'UX_OrgRegNum_OrgVal_NullAddr' AND object_id = OBJECT_ID('Dwh2.DimOrganizationRegistrationNumber')
)
CREATE UNIQUE INDEX UX_OrgRegNum_OrgVal_NullAddr
  ON Dwh2.DimOrganizationRegistrationNumber (OrganizationKey, [Value])
  WHERE AddressType IS NULL;
GO

-- FactMessageNumber: avoid duplicate message numbers per parent + Type
CREATE UNIQUE INDEX UX_FactMessageNumber_AR_TypeValue
  ON Dwh2.FactMessageNumber (FactAccountsReceivableTransactionKey, [Type], [Value])
  WHERE FactAccountsReceivableTransactionKey IS NOT NULL AND [Type] IS NOT NULL;
GO
CREATE UNIQUE INDEX UX_FactMessageNumber_AR_Value_NullType
  ON Dwh2.FactMessageNumber (FactAccountsReceivableTransactionKey, [Value])
  WHERE FactAccountsReceivableTransactionKey IS NOT NULL AND [Type] IS NULL;
GO
CREATE UNIQUE INDEX UX_FactMessageNumber_Shipment_TypeValue
  ON Dwh2.FactMessageNumber (FactShipmentKey, [Type], [Value])
  WHERE FactShipmentKey IS NOT NULL AND [Type] IS NOT NULL;
GO
CREATE UNIQUE INDEX UX_FactMessageNumber_Shipment_Value_NullType
  ON Dwh2.FactMessageNumber (FactShipmentKey, [Value])
  WHERE FactShipmentKey IS NOT NULL AND [Type] IS NULL;
GO
CREATE INDEX IX_FactMessageNumber_Shipment ON Dwh2.FactMessageNumber(FactShipmentKey) WHERE FactShipmentKey IS NOT NULL;
CREATE INDEX IX_FactMessageNumber_AR ON Dwh2.FactMessageNumber(FactAccountsReceivableTransactionKey) WHERE FactAccountsReceivableTransactionKey IS NOT NULL;
GO

-- BridgeFactARRecipientRole indexes
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes WHERE name = 'IX_BridgeFactARRecipientRole_Fact' AND object_id = OBJECT_ID('Dwh2.BridgeFactARRecipientRole')
)
CREATE INDEX IX_BridgeFactARRecipientRole_Fact ON Dwh2.BridgeFactARRecipientRole (FactAccountsReceivableTransactionKey, RecipientRoleKey);
GO
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes WHERE name = 'IX_BridgeFactARRecipientRole_Role' AND object_id = OBJECT_ID('Dwh2.BridgeFactARRecipientRole')
)
CREATE INDEX IX_BridgeFactARRecipientRole_Role ON Dwh2.BridgeFactARRecipientRole (RecipientRoleKey);
GO

-- BridgeFactAROrganization indexes
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes WHERE name = 'IX_BridgeFactAROrganization_Fact' AND object_id = OBJECT_ID('Dwh2.BridgeFactAROrganization')
)
CREATE INDEX IX_BridgeFactAROrganization_Fact ON Dwh2.BridgeFactAROrganization (FactAccountsReceivableTransactionKey, AddressType, OrganizationKey);
GO
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes WHERE name = 'IX_BridgeFactAROrganization_Org' AND object_id = OBJECT_ID('Dwh2.BridgeFactAROrganization')
)
CREATE INDEX IX_BridgeFactAROrganization_Org ON Dwh2.BridgeFactAROrganization (OrganizationKey);
GO

-- BridgeFactShipmentOrganization indexes
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes WHERE name = 'IX_BridgeFactShipmentOrganization_Fact' AND object_id = OBJECT_ID('Dwh2.BridgeFactShipmentOrganization')
)
CREATE INDEX IX_BridgeFactShipmentOrganization_Fact ON Dwh2.BridgeFactShipmentOrganization (FactShipmentKey, AddressType, OrganizationKey);
GO
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes WHERE name = 'IX_BridgeFactShipmentOrganization_Org' AND object_id = OBJECT_ID('Dwh2.BridgeFactShipmentOrganization')
)
CREATE INDEX IX_BridgeFactShipmentOrganization_Org ON Dwh2.BridgeFactShipmentOrganization (OrganizationKey);
GO

/* Milestones attached to Shipments (CSL), SubShipments (CSL), or AR (UniversalTransaction) */
IF OBJECT_ID('Dwh2.FactMilestone','U') IS NOT NULL DROP TABLE Dwh2.FactMilestone;
CREATE TABLE Dwh2.FactMilestone (
  FactMilestoneKey INT IDENTITY(1,1) PRIMARY KEY,
  -- exactly one parent must be set
  FactShipmentKey INT NULL,
  FactSubShipmentKey INT NULL,
  FactAccountsReceivableTransactionKey INT NULL,

  Source NVARCHAR(20) NOT NULL, -- 'CSL' | 'AR'
  EventCode NVARCHAR(50) NULL,
  [Description] NVARCHAR(500) NULL,
  [Sequence] INT NULL,

  ActualDateKey INT NULL,
  ActualTime NVARCHAR(12) NULL,
  EstimatedDateKey INT NULL,
  EstimatedTime NVARCHAR(12) NULL,
  ConditionReference NVARCHAR(200) NULL,
  ConditionType NVARCHAR(100) NULL,

  CompanyKey INT NULL,
  DepartmentKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT CK_FactMilestone_OneParent CHECK (
    (CASE WHEN FactShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactSubShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactAccountsReceivableTransactionKey IS NOT NULL THEN 1 ELSE 0 END) = 1
  ),
  CONSTRAINT FK_FactMilestone_FactShipment FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
  -- FK to SubShipment is added later after Dwh2.FactSubShipment is created
  CONSTRAINT FK_FactMilestone_FactAR FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactMilestone_ActualDate FOREIGN KEY (ActualDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactMilestone_EstimatedDate FOREIGN KEY (EstimatedDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactMilestone_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactMilestone_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactMilestone_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactMilestone_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey)
);
GO

CREATE INDEX IX_FactMilestone_Shipment ON Dwh2.FactMilestone(FactShipmentKey) WHERE FactShipmentKey IS NOT NULL;
CREATE INDEX IX_FactMilestone_AR ON Dwh2.FactMilestone(FactAccountsReceivableTransactionKey) WHERE FactAccountsReceivableTransactionKey IS NOT NULL;
GO

/* Sub-shipments nested under Shipments (CSL) or present in AR context */
IF OBJECT_ID('Dwh2.FactSubShipment','U') IS NOT NULL DROP TABLE Dwh2.FactSubShipment;
CREATE TABLE Dwh2.FactSubShipment (
  FactSubShipmentKey INT IDENTITY(1,1) PRIMARY KEY,
  -- exactly one parent must be set
  FactShipmentKey INT NULL,
  FactAccountsReceivableTransactionKey INT NULL,

  -- Ports / places
  EventBranchHomePortKey INT NULL,
  PortOfLoadingKey INT NULL,
  PortOfDischargeKey INT NULL,
  PortOfFirstArrivalKey INT NULL,
  PortOfDestinationKey INT NULL,
  PortOfOriginKey INT NULL,

  -- Dims
  ServiceLevelKey INT NULL,
  ShipmentTypeKey INT NULL,
  ReleaseTypeKey INT NULL,
  ContainerModeKey INT NULL,
  FreightRateCurrencyKey INT NULL,
  GoodsValueCurrencyKey INT NULL,
  InsuranceValueCurrencyKey INT NULL,
  TotalVolumeUnitKey INT NULL,
  TotalWeightUnitKey INT NULL,
  PacksUnitKey INT NULL,
  CO2eUnitKey INT NULL,

  -- Identifiers/attributes
  WayBillNumber NVARCHAR(100) NULL,
  WayBillTypeCode NVARCHAR(50) NULL,
  WayBillTypeDescription NVARCHAR(200) NULL,
  VesselName NVARCHAR(200) NULL,
  VoyageFlightNo NVARCHAR(100) NULL,
  LloydsIMO NVARCHAR(50) NULL,
  TransportMode NVARCHAR(50) NULL,

  -- Measures
  ContainerCount INT NULL,
  ActualChargeable DECIMAL(18,4) NULL,
  DocumentedChargeable DECIMAL(18,4) NULL,
  DocumentedVolume DECIMAL(18,6) NULL,
  DocumentedWeight DECIMAL(18,4) NULL,
  GoodsValue DECIMAL(18,4) NULL,
  InsuranceValue DECIMAL(18,4) NULL,
  FreightRate DECIMAL(18,4) NULL,
  TotalVolume DECIMAL(18,6) NULL,
  TotalWeight DECIMAL(18,4) NULL,
  TotalNoOfPacks INT NULL,
  OuterPacks INT NULL,
  GreenhouseGasEmissionCO2e DECIMAL(18,4) NULL,

  -- Flags
  IsBooking BIT NULL,
  IsCancelled BIT NULL,
  IsCFSRegistered BIT NULL,
  IsDirectBooking BIT NULL,
  IsForwardRegistered BIT NULL,
  IsHighRisk BIT NULL,
  IsNeutralMaster BIT NULL,
  IsShipping BIT NULL,
  IsSplitShipment BIT NULL,

  CompanyKey INT NULL,
  DepartmentKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT CK_FactSubShipment_OneParent CHECK (
    (CASE WHEN FactShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactAccountsReceivableTransactionKey IS NOT NULL THEN 1 ELSE 0 END) = 1
  ),
  CONSTRAINT FK_FactSubShipment_FactShipment FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
  CONSTRAINT FK_FactSubShipment_FactAR FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactSubShipment_EventBranchHomePort FOREIGN KEY (EventBranchHomePortKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactSubShipment_Ports1 FOREIGN KEY (PortOfLoadingKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactSubShipment_Ports2 FOREIGN KEY (PortOfDischargeKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactSubShipment_Ports3 FOREIGN KEY (PortOfFirstArrivalKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactSubShipment_Ports4 FOREIGN KEY (PortOfDestinationKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactSubShipment_Ports5 FOREIGN KEY (PortOfOriginKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactSubShipment_ServiceLevel FOREIGN KEY (ServiceLevelKey) REFERENCES Dwh2.DimServiceLevel (ServiceLevelKey),
  CONSTRAINT FK_FactSubShipment_ShipmentType FOREIGN KEY (ShipmentTypeKey) REFERENCES Dwh2.DimServiceLevel (ServiceLevelKey),
  CONSTRAINT FK_FactSubShipment_ReleaseType FOREIGN KEY (ReleaseTypeKey) REFERENCES Dwh2.DimServiceLevel (ServiceLevelKey),
  CONSTRAINT FK_FactSubShipment_ContainerMode FOREIGN KEY (ContainerModeKey) REFERENCES Dwh2.DimContainerMode (ContainerModeKey),
  CONSTRAINT FK_FactSubShipment_FreightRateCurrency FOREIGN KEY (FreightRateCurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactSubShipment_GoodsValueCurrency FOREIGN KEY (GoodsValueCurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactSubShipment_InsuranceValueCurrency FOREIGN KEY (InsuranceValueCurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactSubShipment_TotalVolumeUnit FOREIGN KEY (TotalVolumeUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactSubShipment_TotalWeightUnit FOREIGN KEY (TotalWeightUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactSubShipment_PacksUnit FOREIGN KEY (PacksUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactSubShipment_CO2eUnit FOREIGN KEY (CO2eUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactSubShipment_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactSubShipment_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactSubShipment_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactSubShipment_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey)
);
GO

CREATE INDEX IX_FactSubShipment_Shipment ON Dwh2.FactSubShipment(FactShipmentKey) WHERE FactShipmentKey IS NOT NULL;
CREATE INDEX IX_FactSubShipment_AR ON Dwh2.FactSubShipment(FactAccountsReceivableTransactionKey) WHERE FactAccountsReceivableTransactionKey IS NOT NULL;
GO

-- Add FK and index for FactMilestone -> FactSubShipment now that FactSubShipment exists
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_FactMilestone_FactSubShipment')
ALTER TABLE Dwh2.FactMilestone ADD CONSTRAINT FK_FactMilestone_FactSubShipment FOREIGN KEY (FactSubShipmentKey) REFERENCES Dwh2.FactSubShipment (FactSubShipmentKey);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactMilestone_SubShipment' AND object_id = OBJECT_ID('Dwh2.FactMilestone'))
CREATE INDEX IX_FactMilestone_SubShipment ON Dwh2.FactMilestone(FactSubShipmentKey) WHERE FactSubShipmentKey IS NOT NULL;
GO

/* DateCollection entries for Shipments (CSL) or AR (UniversalTransaction) */
IF OBJECT_ID('Dwh2.FactEventDate','U') IS NOT NULL DROP TABLE Dwh2.FactEventDate;
CREATE TABLE Dwh2.FactEventDate (
  FactEventDateKey INT IDENTITY(1,1) PRIMARY KEY,
  -- exactly one parent must be set
  FactShipmentKey INT NULL,
  FactSubShipmentKey INT NULL,
  FactAccountsReceivableTransactionKey INT NULL,

  Source NVARCHAR(20) NOT NULL, -- 'CSL' | 'AR'
  DateTypeCode NVARCHAR(50) NULL,
  DateTypeDescription NVARCHAR(200) NULL,
  DateKey INT NULL,
  [Time] NVARCHAR(12) NULL,
  DateTimeText NVARCHAR(50) NULL,
  IsEstimate BIT NULL,
  [Value] NVARCHAR(200) NULL,
  TimeZone NVARCHAR(50) NULL,

  CompanyKey INT NULL,
  DepartmentKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT CK_FactEventDate_OneParent CHECK (
    (CASE WHEN FactShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactSubShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactAccountsReceivableTransactionKey IS NOT NULL THEN 1 ELSE 0 END) = 1
  ),
  CONSTRAINT FK_FactEventDate_FactShipment FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
  CONSTRAINT FK_FactEventDate_FactSubShipment FOREIGN KEY (FactSubShipmentKey) REFERENCES Dwh2.FactSubShipment (FactSubShipmentKey),
  CONSTRAINT FK_FactEventDate_FactAR FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactEventDate_Date FOREIGN KEY (DateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactEventDate_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactEventDate_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactEventDate_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactEventDate_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey)
);
GO

CREATE INDEX IX_FactEventDate_Shipment ON Dwh2.FactEventDate(FactShipmentKey) WHERE FactShipmentKey IS NOT NULL;
CREATE INDEX IX_FactEventDate_SubShipment ON Dwh2.FactEventDate(FactSubShipmentKey) WHERE FactSubShipmentKey IS NOT NULL;
CREATE INDEX IX_FactEventDate_AR ON Dwh2.FactEventDate(FactAccountsReceivableTransactionKey) WHERE FactAccountsReceivableTransactionKey IS NOT NULL;
CREATE INDEX IX_FactEventDate_Date ON Dwh2.FactEventDate(DateKey);
CREATE INDEX IX_FactEventDate_Type ON Dwh2.FactEventDate(DateTypeCode);
GO

/* Transport legs: can hang off Shipment, SubShipment, or AR */
IF OBJECT_ID('Dwh2.FactTransportLeg','U') IS NOT NULL DROP TABLE Dwh2.FactTransportLeg;
CREATE TABLE Dwh2.FactTransportLeg (
  FactTransportLegKey INT IDENTITY(1,1) PRIMARY KEY,
  -- exactly one parent must be set
  FactShipmentKey INT NULL,
  FactSubShipmentKey INT NULL,
  FactAccountsReceivableTransactionKey INT NULL,

  PortOfLoadingKey INT NULL,
  PortOfDischargeKey INT NULL,
  [Order] INT NULL,
  TransportMode NVARCHAR(50) NULL,
  VesselName NVARCHAR(200) NULL,
  VesselLloydsIMO NVARCHAR(50) NULL,
  VoyageFlightNo NVARCHAR(100) NULL,
  CarrierBookingReference NVARCHAR(100) NULL,
  BookingStatusCode NVARCHAR(50) NULL,
  BookingStatusDescription NVARCHAR(200) NULL,

  CarrierOrganizationKey INT NULL,
  CreditorOrganizationKey INT NULL,

  ActualArrivalDateKey INT NULL,
  ActualArrivalTime NVARCHAR(12) NULL,
  ActualDepartureDateKey INT NULL,
  ActualDepartureTime NVARCHAR(12) NULL,
  EstimatedArrivalDateKey INT NULL,
  EstimatedArrivalTime NVARCHAR(12) NULL,
  EstimatedDepartureDateKey INT NULL,
  EstimatedDepartureTime NVARCHAR(12) NULL,
  ScheduledArrivalDateKey INT NULL,
  ScheduledArrivalTime NVARCHAR(12) NULL,
  ScheduledDepartureDateKey INT NULL,
  ScheduledDepartureTime NVARCHAR(12) NULL,

  CompanyKey INT NULL,
  DepartmentKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT CK_FactTransportLeg_OneParent CHECK (
    (CASE WHEN FactShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactSubShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactAccountsReceivableTransactionKey IS NOT NULL THEN 1 ELSE 0 END) = 1
  ),
  CONSTRAINT FK_FactTransportLeg_FactShipment FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
  CONSTRAINT FK_FactTransportLeg_FactSubShipment FOREIGN KEY (FactSubShipmentKey) REFERENCES Dwh2.FactSubShipment (FactSubShipmentKey),
  CONSTRAINT FK_FactTransportLeg_FactAR FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactTransportLeg_Ports1 FOREIGN KEY (PortOfLoadingKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactTransportLeg_Ports2 FOREIGN KEY (PortOfDischargeKey) REFERENCES Dwh2.DimPort (PortKey),
  CONSTRAINT FK_FactTransportLeg_CarrierOrg FOREIGN KEY (CarrierOrganizationKey) REFERENCES Dwh2.DimOrganization (OrganizationKey),
  CONSTRAINT FK_FactTransportLeg_CreditorOrg FOREIGN KEY (CreditorOrganizationKey) REFERENCES Dwh2.DimOrganization (OrganizationKey),
  CONSTRAINT FK_FactTransportLeg_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactTransportLeg_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactTransportLeg_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactTransportLeg_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey),
  CONSTRAINT FK_FactTransportLeg_Dates1 FOREIGN KEY (ActualArrivalDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactTransportLeg_Dates2 FOREIGN KEY (ActualDepartureDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactTransportLeg_Dates3 FOREIGN KEY (EstimatedArrivalDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactTransportLeg_Dates4 FOREIGN KEY (EstimatedDepartureDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactTransportLeg_Dates5 FOREIGN KEY (ScheduledArrivalDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactTransportLeg_Dates6 FOREIGN KEY (ScheduledDepartureDateKey) REFERENCES Dwh2.DimDate (DateKey)
);
GO

CREATE INDEX IX_FactTransportLeg_Shipment ON Dwh2.FactTransportLeg(FactShipmentKey) WHERE FactShipmentKey IS NOT NULL;
CREATE INDEX IX_FactTransportLeg_SubShipment ON Dwh2.FactTransportLeg(FactSubShipmentKey) WHERE FactSubShipmentKey IS NOT NULL;
CREATE INDEX IX_FactTransportLeg_AR ON Dwh2.FactTransportLeg(FactAccountsReceivableTransactionKey) WHERE FactAccountsReceivableTransactionKey IS NOT NULL;
GO

/* Charge lines: can hang off Shipment (rare), SubShipment, or AR */
IF OBJECT_ID('Dwh2.FactChargeLine','U') IS NOT NULL DROP TABLE Dwh2.FactChargeLine;
CREATE TABLE Dwh2.FactChargeLine (
  FactChargeLineKey INT IDENTITY(1,1) PRIMARY KEY,
  -- exactly one parent must be set
  FactShipmentKey INT NULL,
  FactSubShipmentKey INT NULL,
  FactAccountsReceivableTransactionKey INT NULL,

  Source NVARCHAR(20) NOT NULL, -- 'CSL' | 'AR'

  BranchKey INT NULL,
  DepartmentKey INT NULL,

  ChargeCode NVARCHAR(50) NULL,
  ChargeCodeDescription NVARCHAR(200) NULL,
  ChargeCodeGroup NVARCHAR(50) NULL,
  ChargeCodeGroupDescription NVARCHAR(200) NULL,
  [Description] NVARCHAR(500) NULL,
  DisplaySequence INT NOT NULL,

  CreditorOrganizationKey INT NULL,
  DebtorOrganizationKey INT NULL,

  CostAPInvoiceNumber NVARCHAR(100) NULL,
  CostDueDateKey INT NULL,
  CostDueTime NVARCHAR(12) NULL,
  CostExchangeRate DECIMAL(18,6) NULL,
  CostInvoiceDateKey INT NULL,
  CostInvoiceTime NVARCHAR(12) NULL,
  CostIsPosted BIT NULL,
  CostLocalAmount DECIMAL(18,4) NULL,
  CostOSAmount DECIMAL(18,4) NULL,
  CostOSCurrencyKey INT NULL,
  CostOSGSTVATAmount DECIMAL(18,4) NULL,

  SellExchangeRate DECIMAL(18,6) NULL,
  SellGSTVATTaxCode NVARCHAR(50) NULL,
  SellGSTVATDescription NVARCHAR(200) NULL,
  SellInvoiceType NVARCHAR(10) NULL,
  SellIsPosted BIT NULL,
  SellLocalAmount DECIMAL(18,4) NULL,
  SellOSAmount DECIMAL(18,4) NULL,
  SellOSCurrencyKey INT NULL,
  SellOSGSTVATAmount DECIMAL(18,4) NULL,
  SellPostedTransactionNumber NVARCHAR(50) NULL,
  SellPostedTransactionType NVARCHAR(10) NULL,
  SellTransactionDateKey INT NULL,
  SellTransactionTime NVARCHAR(12) NULL,
  SellDueDateKey INT NULL,
  SellDueTime NVARCHAR(12) NULL,
  SellFullyPaidDateKey INT NULL,
  SellFullyPaidTime NVARCHAR(12) NULL,
  SellOutstandingAmount DECIMAL(18,4) NULL,

  SupplierReference NVARCHAR(100) NULL,
  SellReference NVARCHAR(100) NULL,

  CompanyKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT CK_FactChargeLine_OneParent CHECK (
    (CASE WHEN FactShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactSubShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactAccountsReceivableTransactionKey IS NOT NULL THEN 1 ELSE 0 END) = 1
  ),
  CONSTRAINT FK_FactChargeLine_FactShipment FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
  CONSTRAINT FK_FactChargeLine_FactSubShipment FOREIGN KEY (FactSubShipmentKey) REFERENCES Dwh2.FactSubShipment (FactSubShipmentKey),
  CONSTRAINT FK_FactChargeLine_FactAR FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactChargeLine_Branch FOREIGN KEY (BranchKey) REFERENCES Dwh2.DimBranch (BranchKey),
  CONSTRAINT FK_FactChargeLine_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactChargeLine_CostOSCurrency FOREIGN KEY (CostOSCurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactChargeLine_SellOSCurrency FOREIGN KEY (SellOSCurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactChargeLine_CreditorOrg FOREIGN KEY (CreditorOrganizationKey) REFERENCES Dwh2.DimOrganization (OrganizationKey),
  CONSTRAINT FK_FactChargeLine_DebtorOrg FOREIGN KEY (DebtorOrganizationKey) REFERENCES Dwh2.DimOrganization (OrganizationKey),
  CONSTRAINT FK_FactChargeLine_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactChargeLine_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactChargeLine_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey),
  CONSTRAINT FK_FactChargeLine_Dates1 FOREIGN KEY (CostDueDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactChargeLine_Dates2 FOREIGN KEY (CostInvoiceDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactChargeLine_Dates3 FOREIGN KEY (SellTransactionDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactChargeLine_Dates4 FOREIGN KEY (SellDueDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactChargeLine_Dates5 FOREIGN KEY (SellFullyPaidDateKey) REFERENCES Dwh2.DimDate (DateKey)
);
GO

-- Avoid duplicate charge lines per parent + sequence + charge code
CREATE UNIQUE INDEX UX_FactChargeLine_AR ON Dwh2.FactChargeLine (FactAccountsReceivableTransactionKey, DisplaySequence, ChargeCode)
  WHERE FactAccountsReceivableTransactionKey IS NOT NULL;
GO
CREATE UNIQUE INDEX UX_FactChargeLine_SubShipment ON Dwh2.FactChargeLine (FactSubShipmentKey, DisplaySequence, ChargeCode)
  WHERE FactSubShipmentKey IS NOT NULL;
GO
CREATE UNIQUE INDEX UX_FactChargeLine_Shipment ON Dwh2.FactChargeLine (FactShipmentKey, DisplaySequence, ChargeCode)
  WHERE FactShipmentKey IS NOT NULL;
GO

/* JobCosting header metrics: can hang off Shipment, SubShipment, or AR */
IF OBJECT_ID('Dwh2.FactJobCosting','U') IS NOT NULL DROP TABLE Dwh2.FactJobCosting;
CREATE TABLE Dwh2.FactJobCosting (
  FactJobCostingKey INT IDENTITY(1,1) PRIMARY KEY,
  -- exactly one parent must be set
  FactShipmentKey INT NULL,
  FactSubShipmentKey INT NULL,
  FactAccountsReceivableTransactionKey INT NULL,

  Source NVARCHAR(20) NOT NULL, -- 'CSL' | 'AR'

  BranchKey INT NULL,
  DepartmentKey INT NULL,
  HomeBranchKey INT NULL,
  OperationsStaffKey INT NULL,
  CurrencyKey INT NULL,

  ClientContractNumber NVARCHAR(100) NULL,

  AccrualNotRecognized DECIMAL(18,4) NULL,
  AccrualRecognized DECIMAL(18,4) NULL,
  AgentRevenue DECIMAL(18,4) NULL,
  LocalClientRevenue DECIMAL(18,4) NULL,
  OtherDebtorRevenue DECIMAL(18,4) NULL,
  TotalAccrual DECIMAL(18,4) NULL,
  TotalCost DECIMAL(18,4) NULL,
  TotalJobProfit DECIMAL(18,4) NULL,
  TotalRevenue DECIMAL(18,4) NULL,
  TotalWIP DECIMAL(18,4) NULL,
  WIPNotRecognized DECIMAL(18,4) NULL,
  WIPRecognized DECIMAL(18,4) NULL,

  CompanyKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT CK_FactJobCosting_OneParent CHECK (
    (CASE WHEN FactShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactSubShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactAccountsReceivableTransactionKey IS NOT NULL THEN 1 ELSE 0 END) = 1
  ),
  CONSTRAINT FK_FactJobCosting_FactShipment FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
  CONSTRAINT FK_FactJobCosting_FactSubShipment FOREIGN KEY (FactSubShipmentKey) REFERENCES Dwh2.FactSubShipment (FactSubShipmentKey),
  CONSTRAINT FK_FactJobCosting_FactAR FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactJobCosting_Branch FOREIGN KEY (BranchKey) REFERENCES Dwh2.DimBranch (BranchKey),
  CONSTRAINT FK_FactJobCosting_HomeBranch FOREIGN KEY (HomeBranchKey) REFERENCES Dwh2.DimBranch (BranchKey),
  CONSTRAINT FK_FactJobCosting_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactJobCosting_OperationsStaff FOREIGN KEY (OperationsStaffKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactJobCosting_Currency FOREIGN KEY (CurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactJobCosting_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactJobCosting_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactJobCosting_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey)
);
GO

-- Ensure one row per parent
CREATE UNIQUE INDEX UX_FactJobCosting_AR ON Dwh2.FactJobCosting (FactAccountsReceivableTransactionKey)
  WHERE FactAccountsReceivableTransactionKey IS NOT NULL;
GO
CREATE UNIQUE INDEX UX_FactJobCosting_SubShipment ON Dwh2.FactJobCosting (FactSubShipmentKey)
  WHERE FactSubShipmentKey IS NOT NULL;
GO
CREATE UNIQUE INDEX UX_FactJobCosting_Shipment ON Dwh2.FactJobCosting (FactShipmentKey)
  WHERE FactShipmentKey IS NOT NULL;
GO

/* Additional references: can hang off Shipment, SubShipment, or AR */
IF OBJECT_ID('Dwh2.FactAdditionalReference','U') IS NOT NULL DROP TABLE Dwh2.FactAdditionalReference;
CREATE TABLE Dwh2.FactAdditionalReference (
  FactAdditionalReferenceKey INT IDENTITY(1,1) PRIMARY KEY,
  -- exactly one parent must be set
  FactShipmentKey INT NULL,
  FactSubShipmentKey INT NULL,
  FactAccountsReceivableTransactionKey INT NULL,

  Source NVARCHAR(20) NOT NULL, -- 'CSL' | 'AR'

  TypeCode NVARCHAR(50) NULL,
  TypeDescription NVARCHAR(200) NULL,
  ReferenceNumber NVARCHAR(200) NULL,
  ContextInformation NVARCHAR(500) NULL,
  CountryOfIssueKey INT NULL,
  IssueDateKey INT NULL,
  IssueTime NVARCHAR(12) NULL,

  CompanyKey INT NULL,
  DepartmentKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT CK_FactAdditionalReference_OneParent CHECK (
    (CASE WHEN FactShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactSubShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactAccountsReceivableTransactionKey IS NOT NULL THEN 1 ELSE 0 END) = 1
  ),
  CONSTRAINT FK_FactAdditionalReference_FactShipment FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
  CONSTRAINT FK_FactAdditionalReference_FactSubShipment FOREIGN KEY (FactSubShipmentKey) REFERENCES Dwh2.FactSubShipment (FactSubShipmentKey),
  CONSTRAINT FK_FactAdditionalReference_FactAR FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactAdditionalReference_CountryOfIssue FOREIGN KEY (CountryOfIssueKey) REFERENCES Dwh2.DimCountry (CountryKey),
  CONSTRAINT FK_FactAdditionalReference_IssueDate FOREIGN KEY (IssueDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactAdditionalReference_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactAdditionalReference_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactAdditionalReference_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactAdditionalReference_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey)
);
GO

/* Packing lines: can hang off Shipment, SubShipment, or AR */
IF OBJECT_ID('Dwh2.FactPackingLine','U') IS NOT NULL DROP TABLE Dwh2.FactPackingLine;
CREATE TABLE Dwh2.FactPackingLine (
  FactPackingLineKey INT IDENTITY(1,1) PRIMARY KEY,
  -- exactly one parent must be set
  FactShipmentKey INT NULL,
  FactSubShipmentKey INT NULL,
  FactAccountsReceivableTransactionKey INT NULL,

  Source NVARCHAR(20) NOT NULL, -- 'CSL' | 'AR'

  CommodityCode NVARCHAR(50) NULL,
  CommodityDescription NVARCHAR(200) NULL,
  ContainerLink INT NULL,
  ContainerNumber NVARCHAR(50) NULL,
  ContainerPackingOrder INT NULL,
  CountryOfOriginCode NVARCHAR(10) NULL,
  DetailedDescription NVARCHAR(1000) NULL,
  EndItemNo INT NULL,
  ExportReferenceNumber NVARCHAR(100) NULL,
  GoodsDescription NVARCHAR(1000) NULL,
  HarmonisedCode NVARCHAR(50) NULL,

  Height DECIMAL(18,3) NULL,
  Length DECIMAL(18,3) NULL,
  Width DECIMAL(18,3) NULL,
  LengthUnitKey INT NULL,

  ImportReferenceNumber NVARCHAR(100) NULL,
  ItemNo INT NULL,

  LastKnownCFSStatusCode NVARCHAR(50) NULL,
  LastKnownCFSStatusDateKey INT NULL,
  LastKnownCFSStatusTime NVARCHAR(12) NULL,

  LinePrice DECIMAL(18,4) NULL,
  [Link] INT NULL,
  LoadingMeters DECIMAL(18,3) NULL,
  MarksAndNos NVARCHAR(500) NULL,
  OutturnComment NVARCHAR(500) NULL,
  OutturnDamagedQty INT NULL,
  OutturnedHeight DECIMAL(18,3) NULL,
  OutturnedLength DECIMAL(18,3) NULL,
  OutturnedVolume DECIMAL(18,3) NULL,
  OutturnedWeight DECIMAL(18,3) NULL,
  OutturnedWidth DECIMAL(18,3) NULL,
  OutturnPillagedQty INT NULL,
  OutturnQty INT NULL,
  PackingLineID NVARCHAR(100) NULL,
  PackQty INT NULL,
  PackTypeUnitKey INT NULL,
  ReferenceNumber NVARCHAR(100) NULL,
  RequiresTemperatureControl BIT NULL,
  Volume DECIMAL(18,3) NULL,
  VolumeUnitKey INT NULL,
  Weight DECIMAL(18,3) NULL,
  WeightUnitKey INT NULL,

  CompanyKey INT NULL,
  DepartmentKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT CK_FactPackingLine_OneParent CHECK (
    (CASE WHEN FactShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactSubShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactAccountsReceivableTransactionKey IS NOT NULL THEN 1 ELSE 0 END) = 1
  ),
  CONSTRAINT FK_FactPackingLine_FactShipment FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
  CONSTRAINT FK_FactPackingLine_FactSubShipment FOREIGN KEY (FactSubShipmentKey) REFERENCES Dwh2.FactSubShipment (FactSubShipmentKey),
  CONSTRAINT FK_FactPackingLine_FactAR FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactPackingLine_LengthUnit FOREIGN KEY (LengthUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactPackingLine_VolumeUnit FOREIGN KEY (VolumeUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactPackingLine_WeightUnit FOREIGN KEY (WeightUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactPackingLine_PackTypeUnit FOREIGN KEY (PackTypeUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactPackingLine_LastCFSStatusDate FOREIGN KEY (LastKnownCFSStatusDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactPackingLine_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactPackingLine_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactPackingLine_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactPackingLine_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey)
);
GO

CREATE INDEX IX_FactPackingLine_Shipment ON Dwh2.FactPackingLine(FactShipmentKey) WHERE FactShipmentKey IS NOT NULL;
CREATE INDEX IX_FactPackingLine_SubShipment ON Dwh2.FactPackingLine(FactSubShipmentKey) WHERE FactSubShipmentKey IS NOT NULL;
CREATE INDEX IX_FactPackingLine_AR ON Dwh2.FactPackingLine(FactAccountsReceivableTransactionKey) WHERE FactAccountsReceivableTransactionKey IS NOT NULL;
GO

/* Containers: can hang off Shipment or SubShipment (and AR if ever present) */
IF OBJECT_ID('Dwh2.FactContainer','U') IS NOT NULL DROP TABLE Dwh2.FactContainer;
CREATE TABLE Dwh2.FactContainer (
  FactContainerKey INT IDENTITY(1,1) PRIMARY KEY,
  -- exactly one parent must be set
  FactShipmentKey INT NULL,
  FactSubShipmentKey INT NULL,
  FactAccountsReceivableTransactionKey INT NULL,

  Source NVARCHAR(20) NOT NULL, -- 'CSL' | 'AR'

  ContainerJobID NVARCHAR(50) NULL,
  ContainerNumber NVARCHAR(50) NULL,
  [Link] INT NULL,

  ContainerTypeCode NVARCHAR(20) NULL,
  ContainerTypeDescription NVARCHAR(200) NULL,
  ContainerTypeISOCode NVARCHAR(10) NULL,
  ContainerCategoryCode NVARCHAR(20) NULL,
  ContainerCategoryDescription NVARCHAR(200) NULL,
  DeliveryMode NVARCHAR(50) NULL,
  FCL_LCL_AIR_Code NVARCHAR(10) NULL,
  FCL_LCL_AIR_Description NVARCHAR(100) NULL,
  ContainerCount INT NULL,

  Seal NVARCHAR(50) NULL,
  SealPartyTypeCode NVARCHAR(20) NULL,
  SecondSeal NVARCHAR(50) NULL,
  SecondSealPartyTypeCode NVARCHAR(20) NULL,
  ThirdSeal NVARCHAR(50) NULL,
  ThirdSealPartyTypeCode NVARCHAR(20) NULL,
  StowagePosition NVARCHAR(50) NULL,

  LengthUnitKey INT NULL,
  VolumeUnitKey INT NULL,
  WeightUnitKey INT NULL,

  TotalHeight DECIMAL(18,3) NULL,
  TotalLength DECIMAL(18,3) NULL,
  TotalWidth DECIMAL(18,3) NULL,
  TareWeight DECIMAL(18,3) NULL,
  GrossWeight DECIMAL(18,3) NULL,
  GoodsWeight DECIMAL(18,3) NULL,
  VolumeCapacity DECIMAL(18,3) NULL,
  WeightCapacity DECIMAL(18,3) NULL,
  DunnageWeight DECIMAL(18,3) NULL,
  OverhangBack DECIMAL(18,3) NULL,
  OverhangFront DECIMAL(18,3) NULL,
  OverhangHeight DECIMAL(18,3) NULL,
  OverhangLeft DECIMAL(18,3) NULL,
  OverhangRight DECIMAL(18,3) NULL,
  HumidityPercent INT NULL,
  AirVentFlow DECIMAL(18,3) NULL,
  AirVentFlowRateUnitCode NVARCHAR(20) NULL,

  NonOperatingReefer BIT NULL,
  IsCFSRegistered BIT NULL,
  IsControlledAtmosphere BIT NULL,
  IsDamaged BIT NULL,
  IsEmptyContainer BIT NULL,
  IsSealOk BIT NULL,
  IsShipperOwned BIT NULL,
  ArrivalPickupByRail BIT NULL,
  DepartureDeliveryByRail BIT NULL,

  ArrivalSlotDateKey INT NULL,
  ArrivalSlotTime NVARCHAR(12) NULL,
  DepartureSlotDateKey INT NULL,
  DepartureSlotTime NVARCHAR(12) NULL,
  EmptyReadyForReturnDateKey INT NULL,
  EmptyReadyForReturnTime NVARCHAR(12) NULL,
  FCLWharfGateInDateKey INT NULL,
  FCLWharfGateInTime NVARCHAR(12) NULL,
  FCLWharfGateOutDateKey INT NULL,
  FCLWharfGateOutTime NVARCHAR(12) NULL,
  FCLStorageCommencesDateKey INT NULL,
  FCLStorageCommencesTime NVARCHAR(12) NULL,
  LCLUnpackDateKey INT NULL,
  LCLUnpackTime NVARCHAR(12) NULL,
  PackDateKey INT NULL,
  PackTime NVARCHAR(12) NULL,

  CompanyKey INT NULL,
  DepartmentKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT CK_FactContainer_OneParent CHECK (
    (CASE WHEN FactShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactSubShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactAccountsReceivableTransactionKey IS NOT NULL THEN 1 ELSE 0 END) = 1
  ),
  CONSTRAINT FK_FactContainer_FactShipment FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
  CONSTRAINT FK_FactContainer_FactSubShipment FOREIGN KEY (FactSubShipmentKey) REFERENCES Dwh2.FactSubShipment (FactSubShipmentKey),
  CONSTRAINT FK_FactContainer_FactAR FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactContainer_LengthUnit FOREIGN KEY (LengthUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactContainer_VolumeUnit FOREIGN KEY (VolumeUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactContainer_WeightUnit FOREIGN KEY (WeightUnitKey) REFERENCES Dwh2.DimUnit (UnitKey),
  CONSTRAINT FK_FactContainer_ArrivalSlotDate FOREIGN KEY (ArrivalSlotDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactContainer_DepartureSlotDate FOREIGN KEY (DepartureSlotDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactContainer_EmptyReadyDate FOREIGN KEY (EmptyReadyForReturnDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactContainer_FCLWharfInDate FOREIGN KEY (FCLWharfGateInDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactContainer_FCLWharfOutDate FOREIGN KEY (FCLWharfGateOutDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactContainer_FCLStorageCommencesDate FOREIGN KEY (FCLStorageCommencesDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactContainer_LCLUnpackDate FOREIGN KEY (LCLUnpackDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactContainer_PackDate FOREIGN KEY (PackDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactContainer_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactContainer_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactContainer_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactContainer_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey)
);
GO

CREATE INDEX IX_FactContainer_Shipment ON Dwh2.FactContainer(FactShipmentKey) WHERE FactShipmentKey IS NOT NULL;
CREATE INDEX IX_FactContainer_SubShipment ON Dwh2.FactContainer(FactSubShipmentKey) WHERE FactSubShipmentKey IS NOT NULL;
CREATE INDEX IX_FactContainer_AR ON Dwh2.FactContainer(FactAccountsReceivableTransactionKey) WHERE FactAccountsReceivableTransactionKey IS NOT NULL;
GO

CREATE INDEX IX_FactAdditionalReference_Shipment ON Dwh2.FactAdditionalReference(FactShipmentKey) WHERE FactShipmentKey IS NOT NULL;
CREATE INDEX IX_FactAdditionalReference_SubShipment ON Dwh2.FactAdditionalReference(FactSubShipmentKey) WHERE FactSubShipmentKey IS NOT NULL;
CREATE INDEX IX_FactAdditionalReference_AR ON Dwh2.FactAdditionalReference(FactAccountsReceivableTransactionKey) WHERE FactAccountsReceivableTransactionKey IS NOT NULL;
GO

/* Posting Journal for AR (header lines) */
IF OBJECT_ID('Dwh2.FactPostingJournal','U') IS NOT NULL DROP TABLE Dwh2.FactPostingJournal;
CREATE TABLE Dwh2.FactPostingJournal (
  FactPostingJournalKey INT IDENTITY(1,1) PRIMARY KEY,
  FactAccountsReceivableTransactionKey INT NOT NULL,

  BranchKey INT NULL,
  DepartmentKey INT NULL,
  LocalCurrencyKey INT NULL,
  OSCurrencyKey INT NULL,
  ChargeCurrencyKey INT NULL,

  Sequence INT NULL,
  [Description] NVARCHAR(500) NULL,

  ChargeCode NVARCHAR(50) NULL,
  ChargeTypeCode NVARCHAR(50) NULL,
  ChargeTypeDescription NVARCHAR(200) NULL,
  ChargeClassCode NVARCHAR(50) NULL,
  ChargeClassDescription NVARCHAR(200) NULL,
  ChargeCodeDescription NVARCHAR(200) NULL,

  ChargeExchangeRate DECIMAL(18,6) NULL,
  ChargeTotalAmount DECIMAL(18,4) NULL,
  ChargeTotalExVATAmount DECIMAL(18,4) NULL,

  GLAccountCode NVARCHAR(100) NULL,
  GLAccountDescription NVARCHAR(200) NULL,
  GLPostDateKey INT NULL,
  GLPostTime NVARCHAR(12) NULL,

  JobDimKey INT NULL,
  JobRecognitionDateKey INT NULL,
  JobRecognitionTime NVARCHAR(12) NULL,

  LocalAmount DECIMAL(18,4) NULL,
  LocalGSTVATAmount DECIMAL(18,4) NULL,
  LocalTotalAmount DECIMAL(18,4) NULL,

  OrganizationKey INT NULL,

  OSAmount DECIMAL(18,4) NULL,
  OSGSTVATAmount DECIMAL(18,4) NULL,
  OSTotalAmount DECIMAL(18,4) NULL,

  RevenueRecognitionType NVARCHAR(10) NULL,
  TaxDateKey INT NULL,
  TransactionCategory NVARCHAR(10) NULL,
  TransactionType NVARCHAR(10) NULL,

  VATTaxCode NVARCHAR(50) NULL,
  VATTaxDescription NVARCHAR(200) NULL,
  VATTaxRate DECIMAL(9,4) NULL,
  VATTaxTypeCode NVARCHAR(50) NULL,

  IsFinalCharge BIT NULL,

  CompanyKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT FK_FactPostingJournal_FactAR FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactPostingJournal_Branch FOREIGN KEY (BranchKey) REFERENCES Dwh2.DimBranch (BranchKey),
  CONSTRAINT FK_FactPostingJournal_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactPostingJournal_LocalCurrency FOREIGN KEY (LocalCurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactPostingJournal_OSCurrency FOREIGN KEY (OSCurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactPostingJournal_ChargeCurrency FOREIGN KEY (ChargeCurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactPostingJournal_Organization FOREIGN KEY (OrganizationKey) REFERENCES Dwh2.DimOrganization (OrganizationKey),
  CONSTRAINT FK_FactPostingJournal_GLPostDate FOREIGN KEY (GLPostDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactPostingJournal_Job FOREIGN KEY (JobDimKey) REFERENCES Dwh2.DimJob (JobDimKey),
  CONSTRAINT FK_FactPostingJournal_JobRecDate FOREIGN KEY (JobRecognitionDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactPostingJournal_TaxDate FOREIGN KEY (TaxDateKey) REFERENCES Dwh2.DimDate (DateKey),
  CONSTRAINT FK_FactPostingJournal_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactPostingJournal_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactPostingJournal_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey)
);
GO

-- Prevent duplicates per AR + Sequence when available
CREATE UNIQUE INDEX UX_FactPostingJournal_AR_Seq ON Dwh2.FactPostingJournal (FactAccountsReceivableTransactionKey, Sequence)
  WHERE Sequence IS NOT NULL;
GO

/* Posting Journal Details (child of header line) */
IF OBJECT_ID('Dwh2.FactPostingJournalDetail','U') IS NOT NULL DROP TABLE Dwh2.FactPostingJournalDetail;
CREATE TABLE Dwh2.FactPostingJournalDetail (
  FactPostingJournalDetailKey INT IDENTITY(1,1) PRIMARY KEY,
  FactPostingJournalKey INT NOT NULL,

  CreditGLAccountCode NVARCHAR(100) NULL,
  CreditGLAccountDescription NVARCHAR(200) NULL,
  DebitGLAccountCode NVARCHAR(100) NULL,
  DebitGLAccountDescription NVARCHAR(200) NULL,

  PostingAmount DECIMAL(18,4) NULL,
  PostingCurrencyKey INT NULL,
  PostingDateKey INT NULL,
  PostingTime NVARCHAR(12) NULL,
  PostingPeriod NVARCHAR(10) NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT FK_FactPostingJournalDetail_Header FOREIGN KEY (FactPostingJournalKey) REFERENCES Dwh2.FactPostingJournal (FactPostingJournalKey),
  CONSTRAINT FK_FactPostingJournalDetail_Currency FOREIGN KEY (PostingCurrencyKey) REFERENCES Dwh2.DimCurrency (CurrencyKey),
  CONSTRAINT FK_FactPostingJournalDetail_Date FOREIGN KEY (PostingDateKey) REFERENCES Dwh2.DimDate (DateKey)
);
GO

CREATE INDEX IX_FactPostingJournalDetail_Header ON Dwh2.FactPostingJournalDetail (FactPostingJournalKey);
GO


/* Notes attached to Shipments (CSL) or AR (UniversalTransaction) */
IF OBJECT_ID('Dwh2.FactNote','U') IS NOT NULL DROP TABLE Dwh2.FactNote;
CREATE TABLE Dwh2.FactNote (
  FactNoteKey INT IDENTITY(1,1) PRIMARY KEY,
  -- exactly one parent must be set
  FactShipmentKey INT NULL,
  FactAccountsReceivableTransactionKey INT NULL,

  Source NVARCHAR(20) NOT NULL, -- 'CSL' | 'AR'

  [Description] NVARCHAR(200) NULL,
  IsCustomDescription BIT NULL,
  NoteText NVARCHAR(MAX) NULL,
  NoteContextCode NVARCHAR(50) NULL,
  NoteContextDescription NVARCHAR(200) NULL,
  VisibilityCode NVARCHAR(50) NULL,
  VisibilityDescription NVARCHAR(200) NULL,
  Content NVARCHAR(50) NULL, -- from NoteCollection @ Content attribute when present

  CompanyKey INT NULL,
  DepartmentKey INT NULL,
  EventUserKey INT NULL,
  DataProviderKey INT NULL,

  UpdatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  CONSTRAINT CK_FactNote_OneParent CHECK (
    (CASE WHEN FactShipmentKey IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN FactAccountsReceivableTransactionKey IS NOT NULL THEN 1 ELSE 0 END) = 1
  ),
  CONSTRAINT FK_FactNote_FactShipment FOREIGN KEY (FactShipmentKey) REFERENCES Dwh2.FactShipment (FactShipmentKey),
  CONSTRAINT FK_FactNote_FactAR FOREIGN KEY (FactAccountsReceivableTransactionKey) REFERENCES Dwh2.FactAccountsReceivableTransaction (FactAccountsReceivableTransactionKey),
  CONSTRAINT FK_FactNote_Company FOREIGN KEY (CompanyKey) REFERENCES Dwh2.DimCompany (CompanyKey),
  CONSTRAINT FK_FactNote_Department FOREIGN KEY (DepartmentKey) REFERENCES Dwh2.DimDepartment (DepartmentKey),
  CONSTRAINT FK_FactNote_EventUser FOREIGN KEY (EventUserKey) REFERENCES Dwh2.DimUser (UserKey),
  CONSTRAINT FK_FactNote_DataProvider FOREIGN KEY (DataProviderKey) REFERENCES Dwh2.DimDataProvider (DataProviderKey)
);
GO

CREATE INDEX IX_FactNote_Shipment ON Dwh2.FactNote(FactShipmentKey) WHERE FactShipmentKey IS NOT NULL;
CREATE INDEX IX_FactNote_AR ON Dwh2.FactNote(FactAccountsReceivableTransactionKey) WHERE FactAccountsReceivableTransactionKey IS NOT NULL;
GO


