# Guía rápida (ES)

## Resumen
Punto de partida: dos familias de XML de CargoWise
- AR (UniversalTransaction) y CSL (UniversalShipment) dentro de `XMLS_COL/`.

Lo realizado
- XSD estrictos generados para AR y CSL (con `XSD/CommonTypes.xsd`).
- Validación de XML contra sus XSD.
- Esquema analítico en Azure SQL (`Dwh2`) con Dimensiones (`Dim*`), Hechos (`Fact*`) y puentes.
- Procedimiento para poblar DimDate.
- ETL por fecha para AR y CSL con idempotencia y mejoras de rendimiento (cache de upserts y commits por lote).

## Artefactos clave
- `XSD/UniversalTransaction.strict.xsd`: XSD estricto para AR (UniversalTransaction).
- `XSD/UniversalShipment.strict.xsd`: XSD estricto para CSL (UniversalShipment).
- `XSD/CommonTypes.xsd`: Tipos compartidos.
- `tools/sql/dwh2_ddl.sql`: DDL que crea el esquema `Dwh2` (Dims/Fact, FKs, índices). Limpia el esquema previo antes de crear.
- `tools/sql/dwh2_load_dimdate.sql`: SP para poblar `Dwh2.DimDate` en un rango de fechas.
- `tools/validate_ar_xml.py`: Valida XML contra un XSD dado (acepta prefijo de archivo, p.ej. AR_ o CSL).
- `tools/etl/init_db.py`: Ejecuta `dwh2_ddl.sql` y crea el SP de DimDate.
- `tools/etl/load_by_date.py`: ETL por carpeta de fecha (`XMLS_COL/YYYYMMDD`) para AR y CSL.
- `tools/etl/exec_sql.py`: Utilidad para ejecutar SQL contra Azure SQL.
- `tools/etl/config.py`: Configuración por variables de entorno para la conexión a Azure SQL.
// Documentación
- `docs/dwh2_diccionario_datos.md`: Diccionario de datos completo (tablas, columnas, claves y descripciones) del esquema `Dwh2`.

## Pasos rápidos
1) Validar XML (opcional si ya validaste)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r tools/requirements.txt
python tools/gen_xsd_from_xmls.py           # AR (si necesitas regenerar)
python tools/gen_xsd_from_csl_xmls.py       # CSL (si necesitas regenerar)
python tools/validate_ar_xml.py XSD/UniversalTransaction.strict.xsd AR_
python tools/validate_ar_xml.py XSD/UniversalShipment.strict.xsd CSL
```

2) Crear estructura de BD (Azure SQL)
```bash
# Opción SQL Auth
export AZURE_SQL_SERVER=dbbemel.database.windows.net
export AZURE_SQL_DATABASE=dbbemel
export AZURE_SQL_USER=administrador
export AZURE_SQL_PASSWORD='Pa$$w0rd123456'

source .venv/bin/activate
python -m tools.etl.init_db --dates
```
Esto crea el esquema `Dwh2` y el SP para DimDate.

3) Poblar DimDate
En Azure SQL (Azure Data Studio/SSMS):
```sql
EXEC Dwh2.SpLoadDimDate @StartDate='2000-01-01', @EndDate='2040-12-31', @DeleteExisting=0;
```

4) Carga por fecha (AR y CSL)
```bash
# Ejecutar para una fecha
python -m tools.etl.load_by_date --date 20250709 --quiet

# Opciones
#   --only AR|CSL   Solo ese tipo
#   --limit N       Máximo de archivos
#   --quiet         Silencia logs de fallback
# Rendimiento: COMMIT_EVERY=10 (env) para confirmar cada N archivos
```
 El ETL inserta/upserta dimensiones, `FactAccountsReceivableTransaction` (AR) y `FactShipment` (CSL), y además los hechos de detalle unificados:
 - `FactEventDate` (DateCollection, AR + CSL)
 - `FactException` (ExceptionCollection, AR + CSL)
 - `FactMessageNumber` (MessageNumberCollection, AR + CSL)
 - `FactMilestone` (MilestoneCollection, AR + CSL)
 - `FactSubShipment` (SubShipmentCollection, CSL)
 - `FactTransportLeg` (TransportLegCollection, padre = Shipment/SubShipment/AR)
 - `FactChargeLine` (JobCosting/ChargeLineCollection, padre = SubShipment o AR)

Cobertura de mapeos (XML → Dwh2):
- OrganizationAddressCollection → `DimOrganization` con enriquecimiento básico (país/puerto) y vínculo por puente (`BridgeFactAROrganization` / `BridgeFactShipmentOrganization`); RegistrationNumberCollection → `DimOrganizationRegistrationNumber`.
- DateCollection → `FactEventDate` (regla de un solo padre: AR o Shipment).
- ExceptionCollection → `FactException` (regla de un solo padre).
- MessageNumberCollection → `FactMessageNumber` (unificado para AR + CSL; único por padre + Type).
- MilestoneCollection → `FactMilestone` (AR + CSL).
- SubShipmentCollection → `FactSubShipment` (CSL) con sus medidas/banderas.
- TransportLegCollection → `FactTransportLeg` bajo Shipment, SubShipment o AR, con fechas/horas y organizaciones Carrier/Creditor.
- JobCosting/ChargeLineCollection → `FactChargeLine` bajo SubShipment (CSL) o AR, con importes cost/sell, monedas, impuestos y referencias de transacciones posteadas.

### Ejemplo: ejecutar un rango de fechas (omitiendo días sin carpeta)
```bash
# Con entorno activado; realiza commits cada 10 archivos y sigue aun si falta la carpeta del día
export COMMIT_EVERY=10
python - << 'PY'
import subprocess, datetime, os
start=datetime.date(2025,7,7)
end=datetime.date(2025,8,12)
cur=start
env=os.environ.copy()
env['COMMIT_EVERY']=os.environ.get('COMMIT_EVERY','10')
while cur<=end:
	d=cur.strftime('%Y%m%d')
	print('=== Running', d, flush=True)
	# Usa el python actual del entorno para ejecutar el ETL del día
	r=subprocess.run(['python','-m','tools.etl.load_by_date','--date',d,'--quiet'], env=env)
	print('=== Done', d, 'exit', r.returncode, flush=True)
	# Continuar incluso si hubo error por carpeta faltante
	cur+=datetime.timedelta(days=1)
print('=== All done ===', flush=True)
PY
```

Alternativa: usar el helper de rango
```bash
python -m tools.etl.load_by_range --start 20250707 --end 20250812 --quiet
```

## Notas
- Autenticación AAD: usa `AZURE_SQL_CONNECTION_STRING` con `Authentication=ActiveDirectoryDefault` y ODBC 18.
- Asegúrate de poblar `Dwh2.DimDate` para las fechas referenciadas.
- Idempotencia: `FactShipment` único por `ShipmentJobKey` (índice único filtrado); AR único por `Number`.
- Organización: registros adicionales en `Dwh2.DimOrganizationRegistrationNumber` (renombrado desde `OrganizationRegistrationNumber`).
- Se removieron tablas no utilizadas: `FactARPostingJournal`, `FactARPostingJournalDetail`, `FactARRatingBasis`.

Consulta el diccionario de datos para detalles de cada campo: `docs/dwh2_diccionario_datos.md`.

## Próximos pasos
- Métricas por tipo (AR/CSL) en el resumen.
- Paralelización por archivos o días con control de concurrencia a BD.
- Integración con SFTP para origen en producción.
