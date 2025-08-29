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


---

## Diccionario de atributos por tabla

Nota: Salvo indicación, UpdatedAt es la fecha/hora de última actualización en UTC. Las columnas con sufijo Key suelen ser enteras Identity (claves sustitutas) y las referencias con "Key" son FKs a las dimensiones o hechos indicados. La columna "Rol" resume PK/FK/UQ por atributo.

### Dwh2.DimDate

| Atributo | Tipo de Datos | Nulo | Rol | Descripción del Atributo |
|---|---|---|---|---|
| DateKey | INT | No | PK | Clave de fecha en formato YYYYMMDD. |
| Date | DATE | No | UQ | Fecha calendario. |
| Year | INT | No |  | Año numérico. |
| Quarter | TINYINT | No |  | Trimestre (1–4). |
| Month | TINYINT | No |  | Mes (1–12). |
| DayOfMonth | TINYINT | No |  | Día del mes (1–31). |
| DayOfWeek | TINYINT | No |  | Día de la semana (1–7). |
| MonthName | NVARCHAR(20) | No |  | Nombre del mes. |
| DayName | NVARCHAR(20) | No |  | Nombre del día. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización UTC. |

### Dwh2.DimCountry

| Atributo | Tipo de Datos | Nulo | Rol | Descripción |
|---|---|---|---|---|
| CountryKey | INT IDENTITY | No | PK | Clave sustituta de país. |
| Code | NVARCHAR(10) | No | UQ | Código de país (origen CW/ISO). |
| Name | NVARCHAR(200) | No |  | Nombre del país. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimCurrency

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| CurrencyKey | INT IDENTITY | No | PK | Clave sustituta de moneda. |
| Code | NVARCHAR(10) | No | UQ | Código de moneda (USD, ARS, etc.). |
| Description | NVARCHAR(200) | Sí |  | Descripción de la moneda. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimPort

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| PortKey | INT IDENTITY | No | PK | Clave sustituta del puerto/lugar. |
| Code | NVARCHAR(20) | No | UQ | Código del puerto/lugar. |
| Name | NVARCHAR(200) | No |  | Nombre del puerto/lugar. |
| CountryKey | INT | Sí | FK → DimCountry(CountryKey) | País del puerto. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimCompany

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| CompanyKey | INT IDENTITY | No | PK | Clave sustituta de compañía. |
| Code | NVARCHAR(50) | No | UQ | Código de compañía. |
| Name | NVARCHAR(200) | No |  | Nombre de compañía. |
| CountryKey | INT | Sí | FK → DimCountry(CountryKey) | País de la compañía. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimUser

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| UserKey | INT IDENTITY | No | PK | Clave sustituta de usuario. |
| Code | NVARCHAR(50) | No | UQ | Código de usuario. |
| Name | NVARCHAR(200) | No |  | Nombre para mostrar. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimDepartment

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| DepartmentKey | INT IDENTITY | No | PK | Clave sustituta de departamento. |
| Code | NVARCHAR(50) | No | UQ | Código del departamento. |
| Name | NVARCHAR(200) | No |  | Nombre del departamento. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimEventType

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| EventTypeKey | INT IDENTITY | No | PK | Clave sustituta de tipo de evento. |
| Code | NVARCHAR(50) | No | UQ | Código del tipo de evento. |
| Description | NVARCHAR(200) | No |  | Descripción del tipo. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimActionPurpose

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| ActionPurposeKey | INT IDENTITY | No | PK | Clave sustituta de propósito/acción. |
| Code | NVARCHAR(50) | No | UQ | Código del propósito/acción. |
| Description | NVARCHAR(200) | No |  | Descripción. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimRecipientRole

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| RecipientRoleKey | INT IDENTITY | No | PK | Clave sustituta de rol de destinatario. |
| Code | NVARCHAR(50) | No | UQ | Código del rol. |
| Description | NVARCHAR(200) | No |  | Descripción del rol. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimAccountGroup

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| AccountGroupKey | INT IDENTITY | No | PK | Clave sustituta de grupo contable. |
| Code | NVARCHAR(50) | No | UQ | Código de grupo contable. |
| Description | NVARCHAR(200) | No |  | Descripción. |
| Type | NVARCHAR(10) | No |  | Tipo (AR/AP). |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimScreeningStatus

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| ScreeningStatusKey | INT IDENTITY | No | PK | Clave sustituta de estatus de screening. |
| Code | NVARCHAR(50) | No | UQ | Código del estatus. |
| Description | NVARCHAR(200) | No |  | Descripción. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimServiceLevel

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| ServiceLevelKey | INT IDENTITY | No | PK | Clave sustituta de nivel de servicio. |
| Code | NVARCHAR(50) | No | UQ | Código del nivel. |
| Description | NVARCHAR(200) | No |  | Descripción. |
| ServiceLevelType | NVARCHAR(20) | No |  | Tipo (AWB, Gateway, etc.). |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimContainerMode

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| ContainerModeKey | INT IDENTITY | No | PK | Clave sustituta de modalidad. |
| Code | NVARCHAR(50) | No | UQ | Código de modalidad. |
| Description | NVARCHAR(200) | Sí |  | Descripción de modalidad. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimPaymentMethod

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| PaymentMethodKey | INT IDENTITY | No | PK | Clave sustituta de método de pago. |
| Code | NVARCHAR(50) | No | UQ | Código de método de pago. |
| Description | NVARCHAR(200) | No |  | Descripción. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimUnit

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| UnitKey | INT IDENTITY | No | PK | Clave sustituta de unidad. |
| Code | NVARCHAR(20) | No | UQ | Código de unidad (KG, M3, etc.). |
| Description | NVARCHAR(200) | Sí |  | Descripción de la unidad. |
| UnitType | NVARCHAR(20) | No |  | Tipo (Volumen, Peso, CO2e, Paquetes). |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimBranch

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| BranchKey | INT IDENTITY | No | PK | Clave sustituta de sucursal. |
| Code | NVARCHAR(50) | No | UQ | Código de sucursal. |
| Name | NVARCHAR(200) | No |  | Nombre de sucursal. |
| AddressType | NVARCHAR(50) | Sí |  | Tipo/rol de dirección. |
| Address1 | NVARCHAR(200) | Sí |  | Dirección línea 1. |
| Address2 | NVARCHAR(200) | Sí |  | Dirección línea 2. |
| AddressOverride | NVARCHAR(200) | Sí |  | Texto libre de dirección. |
| AddressShortCode | NVARCHAR(50) | Sí |  | Código compacto de dirección. |
| City | NVARCHAR(100) | Sí |  | Ciudad. |
| State | NVARCHAR(100) | Sí |  | Estado/provincia. |
| Postcode | NVARCHAR(20) | Sí |  | Código postal. |
| CountryKey | INT | Sí | FK → DimCountry(CountryKey) | País. |
| PortKey | INT | Sí | FK → DimPort(PortKey) | Puerto/localidad. |
| Email | NVARCHAR(200) | Sí |  | Email. |
| Fax | NVARCHAR(50) | Sí |  | Fax. |
| Phone | NVARCHAR(50) | Sí |  | Teléfono. |
| ScreeningStatusKey | INT | Sí | FK → DimScreeningStatus(ScreeningStatusKey) | Estatus de screening. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimOrganization

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| OrganizationKey | INT IDENTITY | No | PK | Clave sustituta de organización. |
| OrganizationCode | NVARCHAR(50) | No | UQ | Código único de organización. |
| CompanyName | NVARCHAR(200) | Sí |  | Razón social. |
| GovRegNum | NVARCHAR(100) | Sí |  | Número de registro fiscal (principal). |
| GovRegNumTypeCode | NVARCHAR(50) | Sí |  | Código de tipo de registro fiscal. |
| GovRegNumTypeDescription | NVARCHAR(200) | Sí |  | Descripción del tipo de registro fiscal. |
| AddressType | NVARCHAR(50) | Sí |  | Rol/dirección (Consignee, Shipper, etc.). |
| Address1 | NVARCHAR(200) | Sí |  | Dirección línea 1. |
| Address2 | NVARCHAR(200) | Sí |  | Dirección línea 2. |
| AddressOverride | NVARCHAR(200) | Sí |  | Texto libre de dirección. |
| AddressShortCode | NVARCHAR(50) | Sí |  | Código compacto de dirección. |
| City | NVARCHAR(100) | Sí |  | Ciudad. |
| State | NVARCHAR(100) | Sí |  | Estado/provincia. |
| Postcode | NVARCHAR(20) | Sí |  | Código postal. |
| CountryKey | INT | Sí | FK → DimCountry(CountryKey) | País. |
| PortKey | INT | Sí | FK → DimPort(PortKey) | Puerto/localidad. |
| Email | NVARCHAR(200) | Sí |  | Email. |
| Fax | NVARCHAR(50) | Sí |  | Fax. |
| Phone | NVARCHAR(50) | Sí |  | Teléfono. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimOrganizationRegistrationNumber

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| OrganizationRegistrationNumberKey | INT IDENTITY | No | PK | Clave sustituta. |
| OrganizationKey | INT | No | FK → DimOrganization(OrganizationKey) | Organización dueña del registro. |
| AddressType | NVARCHAR(50) | Sí |  | Rol/escenario de aplicación; NULL si general. |
| TypeCode | NVARCHAR(50) | Sí |  | Código de tipo de registro. |
| TypeDescription | NVARCHAR(200) | Sí |  | Descripción del tipo. |
| CountryOfIssueCode | NVARCHAR(10) | Sí |  | País de emisión (código). |
| CountryOfIssueName | NVARCHAR(200) | Sí |  | País de emisión (nombre). |
| Value | NVARCHAR(300) | No |  | Número de registro fiscal. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimJob

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| JobDimKey | INT IDENTITY | No | PK | Clave sustituta del job. |
| JobType | NVARCHAR(50) | No | UQ(part) | Tipo de job (Shipment, Consol, etc.). |
| JobKey | NVARCHAR(50) | No | UQ(part) | Identificador del job en el origen. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimEnterprise

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| EnterpriseKey | INT IDENTITY | No | PK | Clave sustituta. |
| EnterpriseId | NVARCHAR(50) | No | UQ | Identificador del Enterprise en el origen. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimServer

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| ServerKey | INT IDENTITY | No | PK | Clave sustituta. |
| ServerId | NVARCHAR(50) | No | UQ | Identificador del servidor. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimDataProvider

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| DataProviderKey | INT IDENTITY | No | PK | Clave sustituta. |
| ProviderCode | NVARCHAR(100) | No | UQ | Código del proveedor de datos/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.DimCo2eStatus

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| Co2eStatusKey | INT IDENTITY | No | PK | Clave sustituta. |
| Code | NVARCHAR(50) | No | UQ | Código/clave de estatus de CO2e. |
| Description | NVARCHAR(200) | No |  | Descripción del estatus. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactFileIngestion

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactFileIngestionKey | INT IDENTITY | No | PK | Clave sustituta del registro de ingesta. |
| Source | NVARCHAR(20) | No |  | Origen ('CSL'|'AR'). |
| FileName | NVARCHAR(300) | No | UQ | Nombre de archivo procesado. |
| FileDateKey | INT | Sí | FK → DimDate(DateKey) | Fecha extraída del nombre/archivo. |
| FileTime | TIME(3) | Sí |  | Hora extraída. |
| LoadedAt | DATETIME2(3) | No |  | Fecha/hora de carga. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactAccountsReceivableTransaction

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactAccountsReceivableTransactionKey | INT IDENTITY | No | PK | Clave sustituta del hecho AR. |
| CompanyKey | INT | Sí | FK → DimCompany(CompanyKey) | Compañía del evento. |
| BranchKey | INT | Sí | FK → DimBranch(BranchKey) | Sucursal del evento. |
| DepartmentKey | INT | Sí | FK → DimDepartment(DepartmentKey) | Departamento del evento. |
| EventTypeKey | INT | Sí | FK → DimEventType(EventTypeKey) | Tipo de evento. |
| ActionPurposeKey | INT | Sí | FK → DimActionPurpose(ActionPurposeKey) | Propósito/acción. |
| UserKey | INT | Sí | FK → DimUser(UserKey) | Usuario del evento. |
| EnterpriseKey | INT | Sí | FK → DimEnterprise(EnterpriseKey) | Enterprise de origen. |
| ServerKey | INT | Sí | FK → DimServer(ServerKey) | Servidor de origen. |
| DataProviderKey | INT | Sí | FK → DimDataProvider(DataProviderKey) | Proveedor/lote. |
| TransactionDateKey | INT | Sí | FK → DimDate(DateKey) | Fecha de transacción. |
| PostDateKey | INT | Sí | FK → DimDate(DateKey) | Fecha de contabilización. |
| DueDateKey | INT | Sí | FK → DimDate(DateKey) | Fecha de vencimiento. |
| TriggerDateKey | INT | Sí | FK → DimDate(DateKey) | Fecha disparadora. |
| TriggerDateTime | DATETIME2(3) | Sí |  | Hora disparadora. |
| AccountGroupKey | INT | Sí | FK → DimAccountGroup(AccountGroupKey) | Grupo contable. |
| LocalCurrencyKey | INT | Sí | FK → DimCurrency(CurrencyKey) | Moneda local. |
| OSCurrencyKey | INT | Sí | FK → DimCurrency(CurrencyKey) | Moneda OS. |
| OrganizationKey | INT | Sí | FK → DimOrganization(OrganizationKey) | Organización principal. |
| PlaceOfIssuePortKey | INT | Sí | FK → DimPort(PortKey) | Lugar de emisión. |
| JobDimKey | INT | Sí | FK → DimJob(JobDimKey) | Job relacionado. |
| DataSourceType | NVARCHAR(50) | Sí |  | Tipo de origen de datos. |
| DataSourceKey | NVARCHAR(100) | Sí |  | Identificador en el origen. |
| Number | NVARCHAR(50) | No | UQ | Número único de transacción. |
| Ledger | NVARCHAR(50) | Sí |  | Libro contable. |
| Category | NVARCHAR(100) | Sí |  | Categoría. |
| InvoiceTerm | NVARCHAR(100) | Sí |  | Término de factura. |
| InvoiceTermDays | INT | Sí |  | Días del término. |
| JobInvoiceNumber | NVARCHAR(50) | Sí |  | Número de factura del job. |
| CheckDrawer | NVARCHAR(200) | Sí |  | Librador del cheque/pago. |
| CheckNumberOrPaymentRef | NVARCHAR(100) | Sí |  | Número de cheque/ref. de pago. |
| DrawerBank | NVARCHAR(200) | Sí |  | Banco del librador. |
| DrawerBranch | NVARCHAR(200) | Sí |  | Sucursal bancaria del librador. |
| ReceiptOrDirectDebitNumber | NVARCHAR(100) | Sí |  | Número de recibo/débito directo. |
| RequisitionStatus | NVARCHAR(100) | Sí |  | Estatus de requisición. |
| TransactionReference | NVARCHAR(200) | Sí |  | Referencia de transacción. |
| TransactionType | NVARCHAR(100) | Sí |  | Tipo de transacción. |
| AgreedPaymentMethod | NVARCHAR(100) | Sí |  | Método de pago acordado. |
| ComplianceSubType | NVARCHAR(100) | Sí |  | Subtipo de cumplimiento. |
| CreateTime | NVARCHAR(50) | Sí |  | Fecha/hora de creación (texto). |
| CreateUser | NVARCHAR(100) | Sí |  | Usuario que creó (texto). |
| EventReference | NVARCHAR(200) | Sí |  | Referencia de evento. |
| Timestamp | NVARCHAR(50) | Sí |  | Marca de tiempo (texto). |
| TriggerCount | INT | Sí |  | Veces disparado. |
| TriggerDescription | NVARCHAR(200) | Sí |  | Descripción del disparador. |
| TriggerType | NVARCHAR(100) | Sí |  | Tipo de disparador. |
| NumberOfSupportingDocuments | INT | Sí |  | Documentos de soporte. |
| LocalExVATAmount | DECIMAL(18,4) | Sí |  | Monto sin IVA (local). |
| LocalVATAmount | DECIMAL(18,4) | Sí |  | IVA (local). |
| LocalTaxTransactionsAmount | DECIMAL(18,4) | Sí |  | Otros impuestos (local). |
| LocalTotal | DECIMAL(18,4) | Sí |  | Total (local). |
| OSExGSTVATAmount | DECIMAL(18,4) | Sí |  | Sin impuestos (OS). |
| OSGSTVATAmount | DECIMAL(18,4) | Sí |  | Impuestos (OS). |
| OSTaxTransactionsAmount | DECIMAL(18,4) | Sí |  | Otros impuestos (OS). |
| OSTotal | DECIMAL(18,4) | Sí |  | Total (OS). |
| OutstandingAmount | DECIMAL(18,4) | Sí |  | Monto pendiente. |
| ExchangeRate | DECIMAL(18,6) | Sí |  | Tipo de cambio. |
| IsCancelled | BIT | Sí |  | Indicador de cancelación. |
| IsCreatedByMatchingProcess | BIT | Sí |  | Indicador creado por matching. |
| IsPrinted | BIT | Sí |  | Indicador impreso. |
| PlaceOfIssueText | NVARCHAR(200) | Sí |  | Lugar de emisión (texto). |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactShipment

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactShipmentKey | INT IDENTITY | No | PK | Clave sustituta del hecho Shipment. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía del evento. |
| BranchKey | INT | Sí | FK → DimBranch | Sucursal del evento. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento del evento. |
| EventTypeKey | INT | Sí | FK → DimEventType | Tipo de evento. |
| ActionPurposeKey | INT | Sí | FK → DimActionPurpose | Propósito/acción. |
| UserKey | INT | Sí | FK → DimUser | Usuario del evento. |
| EnterpriseKey | INT | Sí | FK → DimEnterprise | Enterprise de origen. |
| ServerKey | INT | Sí | FK → DimServer | Servidor de origen. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| TriggerDateKey | INT | Sí | FK → DimDate | Fecha disparadora. |
| TriggerDateTime | DATETIME2(3) | Sí |  | Hora disparadora. |
| ConsolJobKey | INT | Sí | FK → DimJob | Consol relacionado. |
| ShipmentJobKey | INT | Sí | UQ, FK → DimJob | Shipment relacionado (único cuando no nulo). |
| PlaceOfDeliveryKey | INT | Sí | FK → DimPort | Lugar de entrega. |
| PlaceOfIssueKey | INT | Sí | FK → DimPort | Lugar de emisión. |
| PlaceOfReceiptKey | INT | Sí | FK → DimPort | Lugar de recibo. |
| PortFirstForeignKey | INT | Sí | FK → DimPort | Primer puerto extranjero. |
| PortLastForeignKey | INT | Sí | FK → DimPort | Último puerto extranjero. |
| PortOfDischargeKey | INT | Sí | FK → DimPort | Puerto de descarga. |
| PortOfFirstArrivalKey | INT | Sí | FK → DimPort | Primer puerto de llegada. |
| PortOfLoadingKey | INT | Sí | FK → DimPort | Puerto de carga. |
| EventBranchHomePortKey | INT | Sí | FK → DimPort | Puerto base de la sucursal. |
| AWBServiceLevelKey | INT | Sí | FK → DimServiceLevel | Nivel de servicio AWB. |
| GatewayServiceLevelKey | INT | Sí | FK → DimServiceLevel | Nivel de servicio Gateway. |
| ShipmentTypeKey | INT | Sí | FK → DimServiceLevel | Tipo de shipment. |
| ReleaseTypeKey | INT | Sí | FK → DimServiceLevel | Tipo de release. |
| ScreeningStatusKey | INT | Sí | FK → DimScreeningStatus | Estatus de screening. |
| PaymentMethodKey | INT | Sí | FK → DimPaymentMethod | Método de pago. |
| FreightRateCurrencyKey | INT | Sí | FK → DimCurrency | Moneda tarifa de flete. |
| TotalVolumeUnitKey | INT | Sí | FK → DimUnit | Unidad volumen. |
| TotalWeightUnitKey | INT | Sí | FK → DimUnit | Unidad peso. |
| CO2eUnitKey | INT | Sí | FK → DimUnit | Unidad CO2e. |
| PacksUnitKey | INT | Sí | FK → DimUnit | Unidad de paquetes. |
| ContainerModeKey | INT | Sí | FK → DimContainerMode | Modalidad contenedor. |
| Co2eStatusKey | INT | Sí | FK → DimCo2eStatus | Estatus CO2e. |
| AgentsReference | NVARCHAR(200) | Sí |  | Referencia del agente. |
| BookingConfirmationReference | NVARCHAR(100) | Sí |  | Confirmación de booking. |
| CarrierContractNumber | NVARCHAR(100) | Sí |  | Contrato con el carrier. |
| ElectronicBillOfLadingReference | NVARCHAR(100) | Sí |  | Referencia e-B/L. |
| CarrierBookingOfficeCode | NVARCHAR(50) | Sí |  | Código oficina booking. |
| CarrierBookingOfficeName | NVARCHAR(200) | Sí |  | Nombre oficina booking. |
| ContainerCount | INT | Sí |  | Cantidad de contenedores. |
| ChargeableRate | DECIMAL(18,4) | Sí |  | Tarifa aplicable. |
| DocumentedChargeable | DECIMAL(18,4) | Sí |  | Carga imputada documentada. |
| DocumentedVolume | DECIMAL(18,6) | Sí |  | Volumen documentado. |
| DocumentedWeight | DECIMAL(18,4) | Sí |  | Peso documentado. |
| FreightRate | DECIMAL(18,4) | Sí |  | Tarifa de flete. |
| GreenhouseGasEmissionCO2e | DECIMAL(18,4) | Sí |  | Emisiones CO2e. |
| ManifestedChargeable | DECIMAL(18,4) | Sí |  | Carga imputada manifestada. |
| ManifestedVolume | DECIMAL(18,6) | Sí |  | Volumen manifestado. |
| ManifestedWeight | DECIMAL(18,4) | Sí |  | Peso manifestado. |
| MaximumAllowablePackageHeight | DECIMAL(18,3) | Sí |  | Altura máxima paquete. |
| MaximumAllowablePackageLength | DECIMAL(18,3) | Sí |  | Largo máximo paquete. |
| MaximumAllowablePackageWidth | DECIMAL(18,3) | Sí |  | Ancho máximo paquete. |
| NoCopyBills | INT | Sí |  | Número de copias B/L. |
| NoOriginalBills | INT | Sí |  | Número de originales B/L. |
| OuterPacks | INT | Sí |  | Bultos externos. |
| TotalNoOfPacks | INT | Sí |  | Total bultos. |
| TotalPreallocatedChargeable | DECIMAL(18,4) | Sí |  | Carga preasignada total. |
| TotalPreallocatedVolume | DECIMAL(18,6) | Sí |  | Volumen preasignado total. |
| TotalPreallocatedWeight | DECIMAL(18,4) | Sí |  | Peso preasignado total. |
| TotalVolume | DECIMAL(18,6) | Sí |  | Volumen total. |
| TotalWeight | DECIMAL(18,4) | Sí |  | Peso total. |
| IsCFSRegistered | BIT | Sí |  | Indicador CFS. |
| IsDirectBooking | BIT | Sí |  | Booking directo. |
| IsForwardRegistered | BIT | Sí |  | Forward registrado. |
| IsHazardous | BIT | Sí |  | Peligroso. |
| IsNeutralMaster | BIT | Sí |  | Master neutral. |
| RequiresTemperatureControl | BIT | Sí |  | Requiere control de temperatura. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.BridgeFactARRecipientRole

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactAccountsReceivableTransactionKey | INT | No | PK(part), FK → FactAR | Hecho AR. |
| RecipientRoleKey | INT | No | PK(part), FK → DimRecipientRole | Rol de destinatario. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactMessageNumber

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactMessageNumberKey | INT IDENTITY | No | PK | Clave del mensaje. |
| FactShipmentKey | INT | Sí | FK → FactShipment | Shipment padre (Source='CSL'). |
| FactAccountsReceivableTransactionKey | INT | Sí | FK → FactAR | AR padre (Source='AR'). |
| Source | NVARCHAR(20) | No |  | Origen ('CSL'|'AR'). |
| Value | NVARCHAR(200) | No |  | Valor/número del mensaje. |
| Type | NVARCHAR(50) | Sí |  | Tipo del mensaje. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía del evento. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento del evento. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario del evento. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.BridgeFactAROrganization

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactAccountsReceivableTransactionKey | INT | No | PK(part), FK → FactAR | Hecho AR. |
| OrganizationKey | INT | No | PK(part), FK → DimOrganization | Organización relacionada. |
| AddressType | NVARCHAR(50) | No | PK(part) | Rol/dirección. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.BridgeFactShipmentOrganization

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactShipmentKey | INT | No | PK(part), FK → FactShipment | Hecho Shipment. |
| OrganizationKey | INT | No | PK(part), FK → DimOrganization | Organización relacionada. |
| AddressType | NVARCHAR(50) | No | PK(part) | Rol/dirección. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactException

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactExceptionKey | INT IDENTITY | No | PK | Clave de excepción. |
| FactShipmentKey | INT | Sí | FK → FactShipment | Shipment padre (exclusivo). |
| FactAccountsReceivableTransactionKey | INT | Sí | FK → FactAR | AR padre (exclusivo). |
| Source | NVARCHAR(20) | No |  | Origen ('CSL'|'AR'). |
| ExceptionId | NVARCHAR(100) | Sí |  | Identificador de excepción. |
| Code | NVARCHAR(50) | Sí |  | Código de excepción. |
| Type | NVARCHAR(100) | Sí |  | Tipo/categoría. |
| Severity | NVARCHAR(50) | Sí |  | Severidad. |
| Status | NVARCHAR(50) | Sí |  | Estatus. |
| Description | NVARCHAR(1000) | Sí |  | Descripción. |
| IsResolved | BIT | Sí |  | Indicador de resuelto. |
| Actioned | BIT | Sí |  | Indicador de acción tomada. |
| ActionedDateKey | INT | Sí | FK → DimDate | Fecha de acción. |
| ActionedTime | NVARCHAR(12) | Sí |  | Hora de acción. |
| Category | NVARCHAR(100) | Sí |  | Categoría. |
| EventDateKey | INT | Sí | FK → DimDate | Fecha del evento. |
| EventTime | NVARCHAR(12) | Sí |  | Hora del evento. |
| DurationHours | INT | Sí |  | Duración (horas). |
| LocationCode | NVARCHAR(100) | Sí |  | Código de ubicación. |
| LocationName | NVARCHAR(200) | Sí |  | Nombre de ubicación. |
| Notes | NVARCHAR(1000) | Sí |  | Notas. |
| StaffCode | NVARCHAR(100) | Sí |  | Código de staff. |
| StaffName | NVARCHAR(200) | Sí |  | Nombre de staff. |
| RaisedDateKey | INT | Sí | FK → DimDate | Fecha de levantamiento. |
| RaisedTime | NVARCHAR(12) | Sí |  | Hora de levantamiento. |
| ResolvedDateKey | INT | Sí | FK → DimDate | Fecha de resolución. |
| ResolvedTime | NVARCHAR(12) | Sí |  | Hora de resolución. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía del evento. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento del evento. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario del evento. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactSubShipment

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactSubShipmentKey | INT IDENTITY | No | PK | Clave sustituta de SubShipment. |
| FactShipmentKey | INT | Sí | FK → FactShipment | Shipment padre (exclusivo con AR). |
| FactAccountsReceivableTransactionKey | INT | Sí | FK → FactAR | AR padre (exclusivo con Shipment). |
| EventBranchHomePortKey | INT | Sí | FK → DimPort | Puerto base de sucursal. |
| PortOfLoadingKey | INT | Sí | FK → DimPort | Puerto de carga. |
| PortOfDischargeKey | INT | Sí | FK → DimPort | Puerto de descarga. |
| PortOfFirstArrivalKey | INT | Sí | FK → DimPort | Puerto de primera llegada. |
| PortOfDestinationKey | INT | Sí | FK → DimPort | Puerto de destino. |
| PortOfOriginKey | INT | Sí | FK → DimPort | Puerto de origen. |
| ServiceLevelKey | INT | Sí | FK → DimServiceLevel | Nivel de servicio. |
| ShipmentTypeKey | INT | Sí | FK → DimServiceLevel | Tipo de shipment. |
| ReleaseTypeKey | INT | Sí | FK → DimServiceLevel | Tipo de release. |
| ContainerModeKey | INT | Sí | FK → DimContainerMode | Modalidad. |
| FreightRateCurrencyKey | INT | Sí | FK → DimCurrency | Moneda tarifa. |
| GoodsValueCurrencyKey | INT | Sí | FK → DimCurrency | Moneda valor mercadería. |
| InsuranceValueCurrencyKey | INT | Sí | FK → DimCurrency | Moneda seguro. |
| TotalVolumeUnitKey | INT | Sí | FK → DimUnit | Unidad volumen. |
| TotalWeightUnitKey | INT | Sí | FK → DimUnit | Unidad peso. |
| PacksUnitKey | INT | Sí | FK → DimUnit | Unidad paquetes. |
| CO2eUnitKey | INT | Sí | FK → DimUnit | Unidad CO2e. |
| WayBillNumber | NVARCHAR(100) | Sí |  | Número de guía. |
| WayBillTypeCode | NVARCHAR(50) | Sí |  | Código tipo de guía. |
| WayBillTypeDescription | NVARCHAR(200) | Sí |  | Descripción tipo de guía. |
| VesselName | NVARCHAR(200) | Sí |  | Nombre del buque. |
| VoyageFlightNo | NVARCHAR(100) | Sí |  | Viaje/vuelo. |
| LloydsIMO | NVARCHAR(50) | Sí |  | IMO Lloyds. |
| TransportMode | NVARCHAR(50) | Sí |  | Modo de transporte. |
| ContainerCount | INT | Sí |  | # contenedores. |
| ActualChargeable | DECIMAL(18,4) | Sí |  | Carga imputada real. |
| DocumentedChargeable | DECIMAL(18,4) | Sí |  | Carga imputada documentada. |
| DocumentedVolume | DECIMAL(18,6) | Sí |  | Volumen documentado. |
| DocumentedWeight | DECIMAL(18,4) | Sí |  | Peso documentado. |
| GoodsValue | DECIMAL(18,4) | Sí |  | Valor de la mercadería. |
| InsuranceValue | DECIMAL(18,4) | Sí |  | Valor del seguro. |
| FreightRate | DECIMAL(18,4) | Sí |  | Tarifa de flete. |
| TotalVolume | DECIMAL(18,6) | Sí |  | Volumen total. |
| TotalWeight | DECIMAL(18,4) | Sí |  | Peso total. |
| TotalNoOfPacks | INT | Sí |  | Total de bultos. |
| OuterPacks | INT | Sí |  | Bultos externos. |
| GreenhouseGasEmissionCO2e | DECIMAL(18,4) | Sí |  | Emisiones CO2e. |
| IsBooking | BIT | Sí |  | Es booking. |
| IsCancelled | BIT | Sí |  | Cancelado. |
| IsCFSRegistered | BIT | Sí |  | CFS registrado. |
| IsDirectBooking | BIT | Sí |  | Booking directo. |
| IsForwardRegistered | BIT | Sí |  | Forward registrado. |
| IsHighRisk | BIT | Sí |  | Alto riesgo. |
| IsNeutralMaster | BIT | Sí |  | Master neutral. |
| IsShipping | BIT | Sí |  | En envío. |
| IsSplitShipment | BIT | Sí |  | Shipment dividido. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactEventDate

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactEventDateKey | INT IDENTITY | No | PK | Clave de evento de fecha. |
| FactShipmentKey | INT | Sí | FK → FactShipment | Shipment padre (exclusivo). |
| FactSubShipmentKey | INT | Sí | FK → FactSubShipment | SubShipment padre (exclusivo). |
| FactAccountsReceivableTransactionKey | INT | Sí | FK → FactAR | AR padre (exclusivo). |
| Source | NVARCHAR(20) | No |  | Origen ('CSL'|'AR'). |
| DateTypeCode | NVARCHAR(50) | Sí |  | Código de tipo de fecha/evento. |
| DateTypeDescription | NVARCHAR(200) | Sí |  | Descripción del tipo de fecha/evento. |
| DateKey | INT | Sí | FK → DimDate | Fecha del evento. |
| Time | NVARCHAR(12) | Sí |  | Hora del evento. |
| DateTimeText | NVARCHAR(50) | Sí |  | Texto de fecha/hora del origen. |
| IsEstimate | BIT | Sí |  | Indicador de estimado. |
| Value | NVARCHAR(200) | Sí |  | Valor raw alterno (cuando no parseable). |
| TimeZone | NVARCHAR(50) | Sí |  | Zona horaria declarada. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía del evento. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento del evento. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario del evento. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactMilestone

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactMilestoneKey | INT IDENTITY | No | PK | Clave de hito/milestone. |
| FactShipmentKey | INT | Sí | FK → FactShipment | Shipment padre (exclusivo). |
| FactSubShipmentKey | INT | Sí | FK → FactSubShipment | SubShipment padre (exclusivo). |
| FactAccountsReceivableTransactionKey | INT | Sí | FK → FactAR | AR padre (exclusivo). |
| Source | NVARCHAR(20) | No |  | Origen ('CSL'|'AR'). |
| EventCode | NVARCHAR(50) | Sí |  | Código de evento. |
| Description | NVARCHAR(500) | Sí |  | Descripción. |
| Sequence | INT | Sí |  | Secuencia. |
| ActualDateKey | INT | Sí | FK → DimDate | Fecha real. |
| ActualTime | NVARCHAR(12) | Sí |  | Hora real. |
| EstimatedDateKey | INT | Sí | FK → DimDate | Fecha estimada. |
| EstimatedTime | NVARCHAR(12) | Sí |  | Hora estimada. |
| ConditionReference | NVARCHAR(200) | Sí |  | Referencia de condición. |
| ConditionType | NVARCHAR(100) | Sí |  | Tipo de condición. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactTransportLeg

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactTransportLegKey | INT IDENTITY | No | PK | Clave de tramo de transporte. |
| FactShipmentKey | INT | Sí | FK → FactShipment | Shipment padre (exclusivo). |
| FactSubShipmentKey | INT | Sí | FK → FactSubShipment | SubShipment padre (exclusivo). |
| FactAccountsReceivableTransactionKey | INT | Sí | FK → FactAR | AR padre (exclusivo). |
| PortOfLoadingKey | INT | Sí | FK → DimPort | Puerto de carga. |
| PortOfDischargeKey | INT | Sí | FK → DimPort | Puerto de descarga. |
| Order | INT | Sí |  | Orden/secuencia. |
| TransportMode | NVARCHAR(50) | Sí |  | Modo de transporte. |
| VesselName | NVARCHAR(200) | Sí |  | Nombre del buque. |
| VesselLloydsIMO | NVARCHAR(50) | Sí |  | IMO del buque. |
| VoyageFlightNo | NVARCHAR(100) | Sí |  | Viaje/vuelo. |
| CarrierBookingReference | NVARCHAR(100) | Sí |  | Referencia de booking del carrier. |
| BookingStatusCode | NVARCHAR(50) | Sí |  | Código de estatus de booking. |
| BookingStatusDescription | NVARCHAR(200) | Sí |  | Descripción de estatus. |
| CarrierOrganizationKey | INT | Sí | FK → DimOrganization | Transportista. |
| CreditorOrganizationKey | INT | Sí | FK → DimOrganization | Acreedor. |
| ActualArrivalDateKey | INT | Sí | FK → DimDate | Fecha arribo real. |
| ActualArrivalTime | NVARCHAR(12) | Sí |  | Hora arribo real. |
| ActualDepartureDateKey | INT | Sí | FK → DimDate | Fecha salida real. |
| ActualDepartureTime | NVARCHAR(12) | Sí |  | Hora salida real. |
| EstimatedArrivalDateKey | INT | Sí | FK → DimDate | Fecha arribo estimada. |
| EstimatedArrivalTime | NVARCHAR(12) | Sí |  | Hora arribo estimada. |
| EstimatedDepartureDateKey | INT | Sí | FK → DimDate | Fecha salida estimada. |
| EstimatedDepartureTime | NVARCHAR(12) | Sí |  | Hora salida estimada. |
| ScheduledArrivalDateKey | INT | Sí | FK → DimDate | Fecha arribo programada. |
| ScheduledArrivalTime | NVARCHAR(12) | Sí |  | Hora arribo programada. |
| ScheduledDepartureDateKey | INT | Sí | FK → DimDate | Fecha salida programada. |
| ScheduledDepartureTime | NVARCHAR(12) | Sí |  | Hora salida programada. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactChargeLine

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactChargeLineKey | INT IDENTITY | No | PK | Clave de línea de cargo. |
| FactShipmentKey | INT | Sí | FK → FactShipment | Shipment padre (exclusivo). |
| FactSubShipmentKey | INT | Sí | FK → FactSubShipment | SubShipment padre (exclusivo). |
| FactAccountsReceivableTransactionKey | INT | Sí | FK → FactAR | AR padre (exclusivo). |
| Source | NVARCHAR(20) | No |  | Origen ('CSL'|'AR'). |
| BranchKey | INT | Sí | FK → DimBranch | Sucursal. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento. |
| ChargeCode | NVARCHAR(50) | Sí |  | Código de cargo. |
| ChargeCodeDescription | NVARCHAR(200) | Sí |  | Descripción de código de cargo. |
| ChargeCodeGroup | NVARCHAR(50) | Sí |  | Grupo de cargo. |
| ChargeCodeGroupDescription | NVARCHAR(200) | Sí |  | Descripción del grupo. |
| Description | NVARCHAR(500) | Sí |  | Descripción. |
| DisplaySequence | INT | No |  | Secuencia de despliegue. |
| CreditorOrganizationKey | INT | Sí | FK → DimOrganization | Acreedor. |
| DebtorOrganizationKey | INT | Sí | FK → DimOrganization | Deudor. |
| CostAPInvoiceNumber | NVARCHAR(100) | Sí |  | Número de factura AP. |
| CostDueDateKey | INT | Sí | FK → DimDate | Fecha de vencimiento costo. |
| CostDueTime | NVARCHAR(12) | Sí |  | Hora de vencimiento costo. |
| CostExchangeRate | DECIMAL(18,6) | Sí |  | Tipo de cambio costo. |
| CostInvoiceDateKey | INT | Sí | FK → DimDate | Fecha factura costo. |
| CostInvoiceTime | NVARCHAR(12) | Sí |  | Hora factura costo. |
| CostIsPosted | BIT | Sí |  | Costo contabilizado. |
| CostLocalAmount | DECIMAL(18,4) | Sí |  | Monto local costo. |
| CostOSAmount | DECIMAL(18,4) | Sí |  | Monto OS costo. |
| CostOSCurrencyKey | INT | Sí | FK → DimCurrency | Moneda OS costo. |
| CostOSGSTVATAmount | DECIMAL(18,4) | Sí |  | Impuesto OS costo. |
| SellExchangeRate | DECIMAL(18,6) | Sí |  | Tipo de cambio venta. |
| SellGSTVATTaxCode | NVARCHAR(50) | Sí |  | Código impuesto venta. |
| SellGSTVATDescription | NVARCHAR(200) | Sí |  | Descripción impuesto venta. |
| SellInvoiceType | NVARCHAR(10) | Sí |  | Tipo de factura venta. |
| SellIsPosted | BIT | Sí |  | Venta contabilizada. |
| SellLocalAmount | DECIMAL(18,4) | Sí |  | Monto local venta. |
| SellOSAmount | DECIMAL(18,4) | Sí |  | Monto OS venta. |
| SellOSCurrencyKey | INT | Sí | FK → DimCurrency | Moneda OS venta. |
| SellOSGSTVATAmount | DECIMAL(18,4) | Sí |  | Impuesto OS venta. |
| SellPostedTransactionNumber | NVARCHAR(50) | Sí |  | Número transacción posteada. |
| SellPostedTransactionType | NVARCHAR(10) | Sí |  | Tipo transacción posteada. |
| SellTransactionDateKey | INT | Sí | FK → DimDate | Fecha transacción venta. |
| SellTransactionTime | NVARCHAR(12) | Sí |  | Hora transacción venta. |
| SellDueDateKey | INT | Sí | FK → DimDate | Fecha vencimiento venta. |
| SellDueTime | NVARCHAR(12) | Sí |  | Hora vencimiento venta. |
| SellFullyPaidDateKey | INT | Sí | FK → DimDate | Fecha pago total. |
| SellFullyPaidTime | NVARCHAR(12) | Sí |  | Hora pago total. |
| SellOutstandingAmount | DECIMAL(18,4) | Sí |  | Monto pendiente de venta. |
| SupplierReference | NVARCHAR(100) | Sí |  | Referencia del proveedor. |
| SellReference | NVARCHAR(100) | Sí |  | Referencia de venta. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactJobCosting

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactJobCostingKey | INT IDENTITY | No | PK | Clave de métricas de JobCosting. |
| FactShipmentKey | INT | Sí | FK → FactShipment | Shipment padre (exclusivo). |
| FactSubShipmentKey | INT | Sí | FK → FactSubShipment | SubShipment padre (exclusivo). |
| FactAccountsReceivableTransactionKey | INT | Sí | FK → FactAR | AR padre (exclusivo). |
| Source | NVARCHAR(20) | No |  | Origen ('CSL'|'AR'). |
| BranchKey | INT | Sí | FK → DimBranch | Sucursal. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento. |
| HomeBranchKey | INT | Sí | FK → DimBranch | Sucursal home. |
| OperationsStaffKey | INT | Sí | FK → DimUser | Usuario de operaciones. |
| CurrencyKey | INT | Sí | FK → DimCurrency | Moneda. |
| ClientContractNumber | NVARCHAR(100) | Sí |  | Contrato del cliente. |
| AccrualNotRecognized | DECIMAL(18,4) | Sí |  | Devengos no reconocidos. |
| AccrualRecognized | DECIMAL(18,4) | Sí |  | Devengos reconocidos. |
| AgentRevenue | DECIMAL(18,4) | Sí |  | Ingreso de agente. |
| LocalClientRevenue | DECIMAL(18,4) | Sí |  | Ingreso cliente local. |
| OtherDebtorRevenue | DECIMAL(18,4) | Sí |  | Ingreso otros deudores. |
| TotalAccrual | DECIMAL(18,4) | Sí |  | Total devengos. |
| TotalCost | DECIMAL(18,4) | Sí |  | Costo total. |
| TotalJobProfit | DECIMAL(18,4) | Sí |  | Utilidad total del job. |
| TotalRevenue | DECIMAL(18,4) | Sí |  | Ingreso total. |
| TotalWIP | DECIMAL(18,4) | Sí |  | Trabajo en proceso total. |
| WIPNotRecognized | DECIMAL(18,4) | Sí |  | WIP no reconocido. |
| WIPRecognized | DECIMAL(18,4) | Sí |  | WIP reconocido. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactAdditionalReference

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactAdditionalReferenceKey | INT IDENTITY | No | PK | Clave de referencia adicional. |
| FactShipmentKey | INT | Sí | FK → FactShipment | Shipment padre (exclusivo). |
| FactSubShipmentKey | INT | Sí | FK → FactSubShipment | SubShipment padre (exclusivo). |
| FactAccountsReceivableTransactionKey | INT | Sí | FK → FactAR | AR padre (exclusivo). |
| Source | NVARCHAR(20) | No |  | Origen ('CSL'|'AR'). |
| TypeCode | NVARCHAR(50) | Sí |  | Código de tipo. |
| TypeDescription | NVARCHAR(200) | Sí |  | Descripción de tipo. |
| ReferenceNumber | NVARCHAR(200) | Sí |  | Número de referencia. |
| ContextInformation | NVARCHAR(500) | Sí |  | Información de contexto. |
| CountryOfIssueKey | INT | Sí | FK → DimCountry | País de emisión. |
| IssueDateKey | INT | Sí | FK → DimDate | Fecha de emisión. |
| IssueTime | NVARCHAR(12) | Sí |  | Hora de emisión. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactPackingLine

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactPackingLineKey | INT IDENTITY | No | PK | Clave de línea de empaque. |
| FactShipmentKey | INT | Sí | FK → FactShipment | Shipment padre (exclusivo). |
| FactSubShipmentKey | INT | Sí | FK → FactSubShipment | SubShipment padre (exclusivo). |
| FactAccountsReceivableTransactionKey | INT | Sí | FK → FactAR | AR padre (exclusivo). |
| Source | NVARCHAR(20) | No |  | Origen ('CSL'|'AR'). |
| CommodityCode | NVARCHAR(50) | Sí |  | Código de commodity. |
| CommodityDescription | NVARCHAR(200) | Sí |  | Descripción de commodity. |
| ContainerLink | INT | Sí |  | Enlace a contenedor. |
| ContainerNumber | NVARCHAR(50) | Sí |  | Número de contenedor. |
| ContainerPackingOrder | INT | Sí |  | Orden de empaque en contenedor. |
| CountryOfOriginCode | NVARCHAR(10) | Sí |  | País de origen (código). |
| DetailedDescription | NVARCHAR(1000) | Sí |  | Descripción detallada. |
| EndItemNo | INT | Sí |  | Ítem final #. |
| ExportReferenceNumber | NVARCHAR(100) | Sí |  | Referencia de exportación. |
| GoodsDescription | NVARCHAR(1000) | Sí |  | Descripción de bienes. |
| HarmonisedCode | NVARCHAR(50) | Sí |  | Código armonizado. |
| Height | DECIMAL(18,3) | Sí |  | Altura. |
| Length | DECIMAL(18,3) | Sí |  | Largo. |
| Width | DECIMAL(18,3) | Sí |  | Ancho. |
| LengthUnitKey | INT | Sí | FK → DimUnit | Unidad de largo. |
| ImportReferenceNumber | NVARCHAR(100) | Sí |  | Referencia de importación. |
| ItemNo | INT | Sí |  | Ítem #. |
| LastKnownCFSStatusCode | NVARCHAR(50) | Sí |  | Último estatus CFS (código). |
| LastKnownCFSStatusDateKey | INT | Sí | FK → DimDate | Fecha último estatus CFS. |
| LastKnownCFSStatusTime | NVARCHAR(12) | Sí |  | Hora último estatus CFS. |
| LinePrice | DECIMAL(18,4) | Sí |  | Precio de línea. |
| Link | INT | Sí |  | Enlace #. |
| LoadingMeters | DECIMAL(18,3) | Sí |  | Metros de carga. |
| MarksAndNos | NVARCHAR(500) | Sí |  | Marcas y números. |
| OutturnComment | NVARCHAR(500) | Sí |  | Comentario de outturn. |
| OutturnDamagedQty | INT | Sí |  | Cantidad dañada. |
| OutturnedHeight | DECIMAL(18,3) | Sí |  | Altura outturned. |
| OutturnedLength | DECIMAL(18,3) | Sí |  | Largo outturned. |
| OutturnedVolume | DECIMAL(18,3) | Sí |  | Volumen outturned. |
| OutturnedWeight | DECIMAL(18,3) | Sí |  | Peso outturned. |
| OutturnedWidth | DECIMAL(18,3) | Sí |  | Ancho outturned. |
| OutturnPillagedQty | INT | Sí |  | Cantidad saqueada. |
| OutturnQty | INT | Sí |  | Cantidad outturn. |
| PackingLineID | NVARCHAR(100) | Sí |  | ID de línea de empaque. |
| PackQty | INT | Sí |  | Cantidad de bultos. |
| PackTypeUnitKey | INT | Sí | FK → DimUnit | Unidad tipo de bulto. |
| ReferenceNumber | NVARCHAR(100) | Sí |  | Número de referencia. |
| RequiresTemperatureControl | BIT | Sí |  | Requiere control de temperatura. |
| Volume | DECIMAL(18,3) | Sí |  | Volumen. |
| VolumeUnitKey | INT | Sí | FK → DimUnit | Unidad de volumen. |
| Weight | DECIMAL(18,3) | Sí |  | Peso. |
| WeightUnitKey | INT | Sí | FK → DimUnit | Unidad de peso. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactContainer

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactContainerKey | INT IDENTITY | No | PK | Clave de contenedor. |
| FactShipmentKey | INT | Sí | FK → FactShipment | Shipment padre (exclusivo). |
| FactSubShipmentKey | INT | Sí | FK → FactSubShipment | SubShipment padre (exclusivo). |
| FactAccountsReceivableTransactionKey | INT | Sí | FK → FactAR | AR padre (exclusivo). |
| Source | NVARCHAR(20) | No |  | Origen ('CSL'|'AR'). |
| ContainerJobID | NVARCHAR(50) | Sí |  | ID de job de contenedor. |
| ContainerNumber | NVARCHAR(50) | Sí |  | Número de contenedor. |
| Link | INT | Sí |  | Enlace #. |
| ContainerTypeCode | NVARCHAR(20) | Sí |  | Código tipo de contenedor. |
| ContainerTypeDescription | NVARCHAR(200) | Sí |  | Descripción tipo contenedor. |
| ContainerTypeISOCode | NVARCHAR(10) | Sí |  | Código ISO de contenedor. |
| ContainerCategoryCode | NVARCHAR(20) | Sí |  | Código de categoría. |
| ContainerCategoryDescription | NVARCHAR(200) | Sí |  | Descripción de categoría. |
| DeliveryMode | NVARCHAR(50) | Sí |  | Modo de entrega. |
| FCL_LCL_AIR_Code | NVARCHAR(10) | Sí |  | Código FCL/LCL/AIR. |
| FCL_LCL_AIR_Description | NVARCHAR(100) | Sí |  | Descripción FCL/LCL/AIR. |
| ContainerCount | INT | Sí |  | Cantidad de contenedores. |
| Seal | NVARCHAR(50) | Sí |  | Sello. |
| SealPartyTypeCode | NVARCHAR(20) | Sí |  | Tipo de parte del sello. |
| SecondSeal | NVARCHAR(50) | Sí |  | Segundo sello. |
| SecondSealPartyTypeCode | NVARCHAR(20) | Sí |  | Tipo de parte del segundo sello. |
| ThirdSeal | NVARCHAR(50) | Sí |  | Tercer sello. |
| ThirdSealPartyTypeCode | NVARCHAR(20) | Sí |  | Tipo de parte del tercer sello. |
| StowagePosition | NVARCHAR(50) | Sí |  | Posición de estiba. |
| LengthUnitKey | INT | Sí | FK → DimUnit | Unidad de longitud. |
| VolumeUnitKey | INT | Sí | FK → DimUnit | Unidad de volumen. |
| WeightUnitKey | INT | Sí | FK → DimUnit | Unidad de peso. |
| TotalHeight | DECIMAL(18,3) | Sí |  | Altura total. |
| TotalLength | DECIMAL(18,3) | Sí |  | Largo total. |
| TotalWidth | DECIMAL(18,3) | Sí |  | Ancho total. |
| TareWeight | DECIMAL(18,3) | Sí |  | Peso tara. |
| GrossWeight | DECIMAL(18,3) | Sí |  | Peso bruto. |
| GoodsWeight | DECIMAL(18,3) | Sí |  | Peso de mercancía. |
| VolumeCapacity | DECIMAL(18,3) | Sí |  | Capacidad de volumen. |
| WeightCapacity | DECIMAL(18,3) | Sí |  | Capacidad de peso. |
| DunnageWeight | DECIMAL(18,3) | Sí |  | Peso de estiba. |
| OverhangBack | DECIMAL(18,3) | Sí |  | Saliente posterior. |
| OverhangFront | DECIMAL(18,3) | Sí |  | Saliente frontal. |
| OverhangHeight | DECIMAL(18,3) | Sí |  | Saliente altura. |
| OverhangLeft | DECIMAL(18,3) | Sí |  | Saliente izquierdo. |
| OverhangRight | DECIMAL(18,3) | Sí |  | Saliente derecho. |
| HumidityPercent | INT | Sí |  | Humedad %. |
| AirVentFlow | DECIMAL(18,3) | Sí |  | Flujo ventilación. |
| AirVentFlowRateUnitCode | NVARCHAR(20) | Sí |  | Unidad flujo ventilación. |
| NonOperatingReefer | BIT | Sí |  | Reefer no operativo. |
| IsCFSRegistered | BIT | Sí |  | CFS registrado. |
| IsControlledAtmosphere | BIT | Sí |  | Atmósfera controlada. |
| IsDamaged | BIT | Sí |  | Dañado. |
| IsEmptyContainer | BIT | Sí |  | Contenedor vacío. |
| IsSealOk | BIT | Sí |  | Sello OK. |
| IsShipperOwned | BIT | Sí |  | Propiedad del shipper. |
| ArrivalPickupByRail | BIT | Sí |  | Retiro por ferrocarril (arribo). |
| DepartureDeliveryByRail | BIT | Sí |  | Entrega por ferrocarril (salida). |
| ArrivalSlotDateKey | INT | Sí | FK → DimDate | Fecha slot arribo. |
| ArrivalSlotTime | NVARCHAR(12) | Sí |  | Hora slot arribo. |
| DepartureSlotDateKey | INT | Sí | FK → DimDate | Fecha slot salida. |
| DepartureSlotTime | NVARCHAR(12) | Sí |  | Hora slot salida. |
| EmptyReadyForReturnDateKey | INT | Sí | FK → DimDate | Fecha listo para devolución. |
| EmptyReadyForReturnTime | NVARCHAR(12) | Sí |  | Hora listo para devolución. |
| FCLWharfGateInDateKey | INT | Sí | FK → DimDate | Fecha gate-in muelle FCL. |
| FCLWharfGateInTime | NVARCHAR(12) | Sí |  | Hora gate-in muelle FCL. |
| FCLWharfGateOutDateKey | INT | Sí | FK → DimDate | Fecha gate-out muelle FCL. |
| FCLWharfGateOutTime | NVARCHAR(12) | Sí |  | Hora gate-out muelle FCL. |
| FCLStorageCommencesDateKey | INT | Sí | FK → DimDate | Fecha inicio almacenaje FCL. |
| FCLStorageCommencesTime | NVARCHAR(12) | Sí |  | Hora inicio almacenaje FCL. |
| LCLUnpackDateKey | INT | Sí | FK → DimDate | Fecha desempacado LCL. |
| LCLUnpackTime | NVARCHAR(12) | Sí |  | Hora desempacado LCL. |
| PackDateKey | INT | Sí | FK → DimDate | Fecha de empaque. |
| PackTime | NVARCHAR(12) | Sí |  | Hora de empaque. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactPostingJournal

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactPostingJournalKey | INT IDENTITY | No | PK | Clave de asiento (header) de AR. |
| FactAccountsReceivableTransactionKey | INT | No | FK → FactAR | Hecho AR padre. |
| BranchKey | INT | Sí | FK → DimBranch | Sucursal. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento. |
| LocalCurrencyKey | INT | Sí | FK → DimCurrency | Moneda local. |
| OSCurrencyKey | INT | Sí | FK → DimCurrency | Moneda OS. |
| ChargeCurrencyKey | INT | Sí | FK → DimCurrency | Moneda del cargo. |
| Sequence | INT | Sí |  | Secuencia de línea. |
| Description | NVARCHAR(500) | Sí |  | Descripción. |
| ChargeCode | NVARCHAR(50) | Sí |  | Código de cargo. |
| ChargeTypeCode | NVARCHAR(50) | Sí |  | Código tipo de cargo. |
| ChargeTypeDescription | NVARCHAR(200) | Sí |  | Descripción tipo de cargo. |
| ChargeClassCode | NVARCHAR(50) | Sí |  | Código de clase de cargo. |
| ChargeClassDescription | NVARCHAR(200) | Sí |  | Descripción clase de cargo. |
| ChargeCodeDescription | NVARCHAR(200) | Sí |  | Descripción código de cargo. |
| ChargeExchangeRate | DECIMAL(18,6) | Sí |  | Tipo de cambio del cargo. |
| ChargeTotalAmount | DECIMAL(18,4) | Sí |  | Total del cargo. |
| ChargeTotalExVATAmount | DECIMAL(18,4) | Sí |  | Total del cargo sin IVA. |
| GLAccountCode | NVARCHAR(100) | Sí |  | Código cuenta contable. |
| GLAccountDescription | NVARCHAR(200) | Sí |  | Descripción cuenta contable. |
| GLPostDateKey | INT | Sí | FK → DimDate | Fecha de contabilización GL. |
| GLPostTime | NVARCHAR(12) | Sí |  | Hora de contabilización GL. |
| JobDimKey | INT | Sí | FK → DimJob | Job relacionado. |
| JobRecognitionDateKey | INT | Sí | FK → DimDate | Fecha de reconocimiento. |
| JobRecognitionTime | NVARCHAR(12) | Sí |  | Hora de reconocimiento. |
| LocalAmount | DECIMAL(18,4) | Sí |  | Monto local. |
| LocalGSTVATAmount | DECIMAL(18,4) | Sí |  | IVA local. |
| LocalTotalAmount | DECIMAL(18,4) | Sí |  | Total local. |
| OrganizationKey | INT | Sí | FK → DimOrganization | Organización asociada. |
| OSAmount | DECIMAL(18,4) | Sí |  | Monto OS. |
| OSGSTVATAmount | DECIMAL(18,4) | Sí |  | IVA OS. |
| OSTotalAmount | DECIMAL(18,4) | Sí |  | Total OS. |
| RevenueRecognitionType | NVARCHAR(10) | Sí |  | Tipo reconocimiento ingreso. |
| TaxDateKey | INT | Sí | FK → DimDate | Fecha de impuesto. |
| TransactionCategory | NVARCHAR(10) | Sí |  | Categoría transacción. |
| TransactionType | NVARCHAR(10) | Sí |  | Tipo transacción. |
| VATTaxCode | NVARCHAR(50) | Sí |  | Código de IVA. |
| VATTaxDescription | NVARCHAR(200) | Sí |  | Descripción de IVA. |
| VATTaxRate | DECIMAL(9,4) | Sí |  | Tasa de IVA. |
| VATTaxTypeCode | NVARCHAR(50) | Sí |  | Código de tipo de IVA. |
| IsFinalCharge | BIT | Sí |  | Indicador de cargo final. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactPostingJournalDetail

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactPostingJournalDetailKey | INT IDENTITY | No | PK | Clave de detalle del asiento. |
| FactPostingJournalKey | INT | No | FK → FactPostingJournal | Header de asiento. |
| CreditGLAccountCode | NVARCHAR(100) | Sí |  | Cuenta GL crédito. |
| CreditGLAccountDescription | NVARCHAR(200) | Sí |  | Descripción GL crédito. |
| DebitGLAccountCode | NVARCHAR(100) | Sí |  | Cuenta GL débito. |
| DebitGLAccountDescription | NVARCHAR(200) | Sí |  | Descripción GL débito. |
| PostingAmount | DECIMAL(18,4) | Sí |  | Monto del asiento. |
| PostingCurrencyKey | INT | Sí | FK → DimCurrency | Moneda del asiento. |
| PostingDateKey | INT | Sí | FK → DimDate | Fecha del asiento. |
| PostingTime | NVARCHAR(12) | Sí |  | Hora del asiento. |
| PostingPeriod | NVARCHAR(10) | Sí |  | Periodo contable. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |

### Dwh2.FactNote

| Atributo | Tipo | Nulo | Rol | Descripción |
|---|---|---|---|---|
| FactNoteKey | INT IDENTITY | No | PK | Clave de nota. |
| FactShipmentKey | INT | Sí | FK → FactShipment | Shipment padre (exclusivo). |
| FactAccountsReceivableTransactionKey | INT | Sí | FK → FactAR | AR padre (exclusivo). |
| Source | NVARCHAR(20) | No |  | Origen ('CSL'|'AR'). |
| Description | NVARCHAR(200) | Sí |  | Descripción breve. |
| IsCustomDescription | BIT | Sí |  | Descripción personalizada. |
| NoteText | NVARCHAR(MAX) | Sí |  | Texto de la nota. |
| NoteContextCode | NVARCHAR(50) | Sí |  | Código de contexto. |
| NoteContextDescription | NVARCHAR(200) | Sí |  | Descripción de contexto. |
| VisibilityCode | NVARCHAR(50) | Sí |  | Código de visibilidad. |
| VisibilityDescription | NVARCHAR(200) | Sí |  | Descripción de visibilidad. |
| Content | NVARCHAR(50) | Sí |  | Contenido desde atributo @Content. |
| CompanyKey | INT | Sí | FK → DimCompany | Compañía. |
| DepartmentKey | INT | Sí | FK → DimDepartment | Departamento. |
| EventUserKey | INT | Sí | FK → DimUser | Usuario. |
| DataProviderKey | INT | Sí | FK → DimDataProvider | Proveedor/lote. |
| UpdatedAt | DATETIME2(3) | No |  | Última actualización. |
