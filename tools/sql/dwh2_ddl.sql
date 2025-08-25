-- Azure SQL DDL for Dwh2 star schema based on UniversalTransaction (AR) and UniversalShipment (CSL)
-- Conventions: Schema Dwh2, Dim/Fact prefixes, PascalCase names, SK as IDENTITY, UpdatedAt on all tables

-- Create schema if not exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Dwh2')
    EXEC('CREATE SCHEMA Dwh2');
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

  -- Flags
  IsCancelled              BIT NULL,
  IsCreatedByMatchingProcess BIT NULL,
  IsPrinted                BIT NULL,

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

CREATE INDEX IX_FactShipment_Ports ON Dwh2.FactShipment (PortOfLoadingKey, PortOfDischargeKey, PlaceOfReceiptKey, PlaceOfDeliveryKey);
CREATE INDEX IX_FactShipment_Service ON Dwh2.FactShipment (AWBServiceLevelKey, GatewayServiceLevelKey, ShipmentTypeKey);
GO

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
