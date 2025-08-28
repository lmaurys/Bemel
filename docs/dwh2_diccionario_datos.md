# Diccionario de datos – Dwh2 (ES)

Este documento describe el modelo analítico Dwh2 (Azure SQL) generado a partir de los XML de CargoWise:
- AR: UniversalTransaction (Cuentas por Cobrar)
- CSL: UniversalShipment (Embarques)

Convenciones generales
- Esquema: Dwh2
- Dimensiones con prefijo Dim*, hechos con prefijo Fact* y puentes Bridge*
- Claves sustitutas Identity (…Key) salvo DimDate.DateKey (INT YYYYMMDD)
- Todas las tablas tienen columna UpdatedAt (UTC)

Notas de integridad
- Unicidad FactShipment por ShipmentJobKey (índice único filtrado)
- Unicidad FactAccountsReceivableTransaction por Number
- Outrigger de registros fiscales: DimOrganizationRegistrationNumber (múltiples por organización)
- DimDate debe estar poblada para las fechas utilizadas por los hechos

---

## Dimensiones

### Dwh2.DimDate
Breve: Calendario de referencia.
- PK: DateKey
- Unicidad: Date (DATE)

Columnas:
- DateKey (INT, NOT NULL): Clave YYYYMMDD.
- Date (DATE, NOT NULL): Fecha calendario.
- Year (INT, NOT NULL): Año numérico.
- Quarter (TINYINT, NOT NULL): Trimestre (1–4).
- Month (TINYINT, NOT NULL): Mes (1–12).
- DayOfMonth (TINYINT, NOT NULL): Día del mes (1–31).
- DayOfWeek (TINYINT, NOT NULL): Día de la semana (1–7).
- MonthName (NVARCHAR(20), NOT NULL): Nombre del mes.
- DayName (NVARCHAR(20), NOT NULL): Nombre del día.
- UpdatedAt (DATETIME2, NOT NULL): Última actualización UTC.

### Dwh2.DimCountry
Breve: Países (código y nombre).
- PK: CountryKey

Columnas: Code (código), Name (nombre), UpdatedAt.

### Dwh2.DimCurrency
Breve: Monedas.
- PK: CurrencyKey
- Unicidad: Code

Columnas: Code, Description, UpdatedAt.

### Dwh2.DimPort
Breve: Puertos/lugares (incluye país inferido cuando aplica).
- PK: PortKey
- FK: CountryKey → DimCountry
- Unicidad: Code

Columnas: Code, Name, CountryKey, UpdatedAt.

### Dwh2.DimCompany
Breve: Compañías internas (empresa).
- PK: CompanyKey
- FK: CountryKey → DimCountry
- Unicidad: Code

Columnas: Code, Name, CountryKey, UpdatedAt.

### Dwh2.DimUser
Breve: Usuarios (creador/modificador en transacciones/eventos).
- PK: UserKey
- Unicidad: Code

Columnas: Code, Name, UpdatedAt.

### Dwh2.DimDepartment
Breve: Departamentos/áreas.
- PK: DepartmentKey
- Unicidad: Code

Columnas: Code, Name, UpdatedAt.

### Dwh2.DimEventType
Breve: Tipos de evento.
- PK: EventTypeKey
- Unicidad: Code

Columnas: Code, Description, UpdatedAt.

### Dwh2.DimActionPurpose
Breve: Propósito/acción del evento.
- PK: ActionPurposeKey
- Unicidad: Code

Columnas: Code, Description, UpdatedAt.

### Dwh2.DimRecipientRole
Breve: Roles de recipientes (aplica a AR).
- PK: RecipientRoleKey
- Unicidad: Code

Columnas: Code, Description, UpdatedAt.

### Dwh2.DimAccountGroup
Breve: Grupos contables (AR/AP).
- PK: AccountGroupKey
- Unicidad: Code

Columnas: Code, Description, Type (AR/AP), UpdatedAt.

### Dwh2.DimScreeningStatus
Breve: Estado de screening/compliance.
- PK: ScreeningStatusKey
- Unicidad: Code

Columnas: Code, Description, UpdatedAt.

### Dwh2.DimServiceLevel
Breve: Niveles de servicio (AWB, Gateway, tipos/release en CSL).
- PK: ServiceLevelKey
- Unicidad: Code

Columnas: Code, Description, ServiceLevelType, UpdatedAt.

### Dwh2.DimContainerMode
Breve: Modalidad de contenedores/transporte.
- PK: ContainerModeKey
- Unicidad: Code

Columnas: Code, Description, UpdatedAt.

### Dwh2.DimPaymentMethod
Breve: Métodos de pago.
- PK: PaymentMethodKey
- Unicidad: Code

Columnas: Code, Description, UpdatedAt.

### Dwh2.DimUnit
Breve: Unidades (Volumen, Peso, CO2e, Paquetes).
- PK: UnitKey
- Unicidad: Code

Columnas: Code, Description, UnitType, UpdatedAt.

### Dwh2.DimBranch
Breve: Sucursales (datos de dirección y estatus).
- PK: BranchKey
- FK: CountryKey → DimCountry, PortKey → DimPort, ScreeningStatusKey → DimScreeningStatus
- Unicidad: Code

Columnas principales: Code, Name, AddressType, Address1/2, AddressOverride, AddressShortCode, City, State, Postcode, CountryKey, PortKey, Email, Fax, Phone, ScreeningStatusKey, UpdatedAt.

### Dwh2.DimOrganization
Breve: Organizaciones/terceros (clientes, agentes, consignatarios, etc.).
- PK: OrganizationKey
- FK: CountryKey → DimCountry, PortKey → DimPort
- Unicidad: OrganizationCode

Columnas principales: OrganizationCode, CompanyName, GovRegNum(+TypeCode/Description), dirección (AddressType, Address1/2, Override, ShortCode, City, State, Postcode), CountryKey, PortKey, Email, Fax, Phone, UpdatedAt.

### Dwh2.DimOrganizationRegistrationNumber
Breve: Números de registro fiscal por organización (outrigger, 0..n por AddressType).
- PK: OrganizationRegistrationNumberKey
- FK: OrganizationKey → DimOrganization
- Índices únicos: (OrganizationKey, AddressType, Value) y (OrganizationKey, Value) cuando AddressType es NULL

Columnas: OrganizationKey, AddressType, TypeCode, TypeDescription, CountryOfIssueCode/Name, Value, UpdatedAt.

### Dwh2.DimJob
Breve: Catálogo de jobs del origen (Consol/Shipment).
- PK: JobDimKey
- Unicidad: (JobType, JobKey)

Columnas: JobType, JobKey, UpdatedAt.

### Dwh2.DimEnterprise
Breve: Identificador de Enterprise (origen CW).
- PK: EnterpriseKey
- Unicidad: EnterpriseId

Columnas: EnterpriseId, UpdatedAt.

### Dwh2.DimServer
Breve: Identificador de servidor (origen CW).
- PK: ServerKey
- Unicidad: ServerId

Columnas: ServerId, UpdatedAt.

### Dwh2.DimDataProvider
Breve: Proveedor de datos (mapeo por lote/origen).
- PK: DataProviderKey
- Unicidad: ProviderCode

Columnas: ProviderCode, UpdatedAt.

### Dwh2.DimCo2eStatus
Breve: Estado del cálculo/estimación de CO2e.
- PK: Co2eStatusKey
- Unicidad: Code

Columnas: Code, Description, UpdatedAt.

---

## Hechos

### Dwh2.FactAccountsReceivableTransaction
Breve: Transacciones de CxC (AR) provenientes de UniversalTransaction.
- PK: FactAccountsReceivableTransactionKey
- Unicidad: Number (UQ)
- FKs: Company, Branch, Department, EventType, ActionPurpose, User, Enterprise, Server, DataProvider, AccountGroup, LocalCurrency, OSCurrency, Organization, PlaceOfIssuePort, JobDim, TransactionDate, PostDate, DueDate, TriggerDate

Columnas (resumen):
- Contexto: CompanyKey, BranchKey, DepartmentKey, EventTypeKey, ActionPurposeKey, UserKey, EnterpriseKey, ServerKey, DataProviderKey
- Fechas: TransactionDateKey, PostDateKey, DueDateKey, TriggerDateKey, TriggerDateTime
- Otras dims: AccountGroupKey, LocalCurrencyKey, OSCurrencyKey, OrganizationKey, PlaceOfIssuePortKey, JobDimKey
- Identificadores: DataSourceType, DataSourceKey, Number (único), Ledger, Category, InvoiceTerm(+Days), JobInvoiceNumber
- Pagos/cheques: CheckDrawer, CheckNumberOrPaymentRef, DrawerBank, DrawerBranch, ReceiptOrDirectDebitNumber
- Estados: RequisitionStatus, TransactionReference, TransactionType, AgreedPaymentMethod, ComplianceSubType
- Auditoría/evento: CreateTime, CreateUser, EventReference, Timestamp, TriggerCount/Description/Type, NumberOfSupportingDocuments
- Medidas: LocalExVATAmount, LocalVATAmount, LocalTaxTransactionsAmount, LocalTotal, OSExGSTVATAmount, OSGSTVATAmount, OSTaxTransactionsAmount, OSTotal, OutstandingAmount, ExchangeRate
- Flags: IsCancelled, IsCreatedByMatchingProcess, IsPrinted
- Texto libre: PlaceOfIssueText
- UpdatedAt

Uso típico: análisis de saldos, montos por periodo, moneda, organización y grupo contable.

### Dwh2.FactShipment
Breve: Embarques de UniversalShipment (CSL) y sus métricas/logística.
- PK: FactShipmentKey
- Unicidad: ShipmentJobKey (índice único filtrado)
- FKs: Company, Branch, Department, EventType, ActionPurpose, User, Enterprise, Server, DataProvider, TriggerDate, ConsolJob, ShipmentJob, puertos (PlaceOfDelivery/Issue/Receipt, PortFirst/LastForeign, PortOfDischarge/FirstArrival/Loading, EventBranchHomePort), ServiceLevel (AWB/Gateway/ShipmentType/ReleaseType), ScreeningStatus, PaymentMethod, Currency, Unit (volumen, peso, CO2e, packs), ContainerMode, Co2eStatus

Columnas (resumen):
- Contexto/tiempo: CompanyKey, BranchKey, DepartmentKey, EventTypeKey, ActionPurposeKey, UserKey, EnterpriseKey, ServerKey, DataProviderKey, TriggerDateKey, TriggerDateTime
- Jobs: ConsolJobKey, ShipmentJobKey
- Puertos/lugares: PlaceOfDeliveryKey, PlaceOfIssueKey, PlaceOfReceiptKey, PortFirstForeignKey, PortLastForeignKey, PortOfDischargeKey, PortOfFirstArrivalKey, PortOfLoadingKey, EventBranchHomePortKey
- Dimensiones: AWBServiceLevelKey, GatewayServiceLevelKey, ShipmentTypeKey, ReleaseTypeKey, ScreeningStatusKey, PaymentMethodKey, FreightRateCurrencyKey, TotalVolumeUnitKey, TotalWeightUnitKey, CO2eUnitKey, PacksUnitKey, ContainerModeKey, Co2eStatusKey
- Atributos: AgentsReference, BookingConfirmationReference, CarrierContractNumber, ElectronicBillOfLadingReference, CarrierBookingOfficeCode/Name
- Medidas: ContainerCount, ChargeableRate, DocumentedChargeable, DocumentedVolume, DocumentedWeight, FreightRate, GreenhouseGasEmissionCO2e, ManifestedChargeable, ManifestedVolume, ManifestedWeight, MaximumAllowablePackageHeight/Length/Width, NoCopyBills, NoOriginalBills, OuterPacks, TotalNoOfPacks, TotalPreallocatedChargeable/Volume/Weight, TotalVolume, TotalWeight
- Flags: IsCFSRegistered, IsDirectBooking, IsForwardRegistered, IsHazardous, IsNeutralMaster, RequiresTemperatureControl
- UpdatedAt

Uso típico: análisis operacional/logístico por puertos, niveles de servicio, unidades y emisiones.

---

## Tablas puente y detalle

### Dwh2.BridgeFactARRecipientRole
Breve: Relación N:M entre AR y roles de recipientes.
- PK compuesta: (FactAccountsReceivableTransactionKey, RecipientRoleKey)
- FKs: FactAccountsReceivableTransaction, DimRecipientRole

Columnas: FactAccountsReceivableTransactionKey, RecipientRoleKey, UpdatedAt.

### Dwh2.FactMessageNumber
Breve: Números de mensaje (MessageNumberCollection) asociados a AR o CSL; unificada como en FactException.
- PK: FactMessageNumberKey
- FKs: FactShipment o FactAccountsReceivableTransaction (exactamente uno), DimCompany, DimDepartment, DimUser, DimDataProvider
- Unicidad: evita duplicados por padre + (Type, Value); maneja Type NULL con índices filtrados

Columnas: FactShipmentKey, FactAccountsReceivableTransactionKey, Source('CSL'|'AR'), Value, Type, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey, UpdatedAt.

### Dwh2.BridgeFactAROrganization
Breve: Relación N:M entre AR y organizaciones por AddressType.
- PK compuesta: (FactAccountsReceivableTransactionKey, OrganizationKey, AddressType)
- FKs: FactAccountsReceivableTransaction, DimOrganization

Columnas: FactAccountsReceivableTransactionKey, OrganizationKey, AddressType, UpdatedAt.

### Dwh2.BridgeFactShipmentOrganization
Breve: Relación N:M entre Shipment y organizaciones por AddressType.
- PK compuesta: (FactShipmentKey, OrganizationKey, AddressType)
- FKs: FactShipment, DimOrganization

Columnas: FactShipmentKey, OrganizationKey, AddressType, UpdatedAt.

---

## Índices y restricciones destacadas
- UX_FactShipment_ShipmentJobKey: único por ShipmentJobKey (no nulo)
- UQ_FactAR_Number: único por Number
- Índices de ayuda en fechas/puertos/servicios para consulta
- DimOrganizationRegistrationNumber: UX por (OrganizationKey, AddressType, Value) y variante con AddressType NULL
- FactMessageNumber: UX por (FactShipmentKey, Type, Value) o (FactShipmentKey, Value con Type NULL); idem para AR cuando el padre es FactAccountsReceivableTransaction

### Dwh2.FactException
Breve: Excepciones (ExceptionCollection) asociadas a Shipments (CSL) o AR.
- PK: FactExceptionKey
- FKs: FactShipment o FactAccountsReceivableTransaction (exactamente uno), DimDate (Raised/Resolved), DimCompany, DimDepartment, DimUser, DimDataProvider

Columnas: FactShipmentKey, FactAccountsReceivableTransactionKey, Source('CSL'|'AR'), Code, Type, Severity, Status, Description, IsResolved, RaisedDateKey, RaisedTime, ResolvedDateKey, ResolvedTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey, UpdatedAt.

### Dwh2.FactEventDate
Breve: Fechas de evento (DateCollection) asociadas a Shipments (CSL) o AR.
- PK: FactEventDateKey
- FKs: FactShipment o FactAccountsReceivableTransaction (exactamente uno), DimDate (DateKey), DimCompany, DimDepartment, DimUser, DimDataProvider

Columnas: FactShipmentKey, FactAccountsReceivableTransactionKey, Source('CSL'|'AR'), DateTypeCode, DateTypeDescription, DateKey, Time, DateTimeText, TimeZone, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey, UpdatedAt.

---

## Glosario de claves y conceptos
- …Key: Clave sustituta Identity de la dimensión/hecho
- JobType/JobKey: Identificador del job en el origen (p.ej., Shipment/Consol)
- AddressType: Rol/dirección (p.ej., Consignee, Shipper, Agent)
- ServiceLevelType: Tipo de servicio (AWB, Gateway, etc.)
- TriggerDate/Time: Fecha/hora del evento que disparó el registro

---

Sugerencias de uso
- Siempre unir hechos a DimDate por las claves de fecha antes de agregar
- Evitar duplicados respetando las claves de negocio (Number y ShipmentJobKey)
- Para país de puertos, DimPort.CountryKey puede ser inferido del código (si aplica)
