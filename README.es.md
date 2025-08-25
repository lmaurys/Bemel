# Guía rápida (ES)

## Resumen
Punto de partida: dos familias de XML de CargoWise
- AR (UniversalTransaction) y CSL (UniversalShipment) dentro de `XMLS_COL/`.

Lo realizado
- Se generaron XSD estrictos a partir de los XML de ejemplo, uno para AR y otro para CSL.
- Se validaron todos los XML contra sus XSD correspondientes.
- Se factorizaron tipos comunes en `XSD/CommonTypes.xsd` y se incluyeron en ambos XSD estrictos.
- A partir de las estructuras (XSD) se diseñó un esquema analítico en Azure SQL (esquema `Dwh2`) con tablas Dimensión (`Dim*`) y Hechos (`Fact*`).
- Se creó un script/procedimiento para poblar la Dimensión de Fechas (DimDate).

## Artefactos clave
- `XSD/UniversalTransaction.strict.xsd`: XSD estricto para AR (UniversalTransaction).
- `XSD/UniversalShipment.strict.xsd`: XSD estricto para CSL (UniversalShipment).
- `XSD/CommonTypes.xsd`: Tipos compartidos.
- `tools/sql/dwh2_ddl.sql`: DDL que crea el esquema `Dwh2` (Dim y Fact con claves foráneas e índices).
- `tools/sql/dwh2_load_dimdate.sql`: SP para poblar `Dwh2.DimDate` en un rango de fechas.
- `tools/validate_ar_xml.py`: Valida XML contra un XSD dado (acepta prefijo de archivo, p.ej. AR_ o CSL).
- `tools/etl/init_db.py`: Ejecuta `dwh2_ddl.sql` y crea el SP de DimDate.
- `tools/etl/exec_sql.py`: Utilidad para ejecutar SQL contra Azure SQL.
- `tools/etl/config.py`: Configuración por variables de entorno para la conexión a Azure SQL.

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

4) Carga inicial de AR (subconjunto limitado)
```bash
# Generar y ejecutar directamente un lote pequeño
python tools/etl/load_ar_to_sql.py --limit 50 --execute
```
El script hace upsert de dimensiones necesarias y luego inserta en `Dwh2.FactAccountsReceivableTransaction`.

## Notas
- Si prefieres autenticación Entra ID (AAD), establece `AZURE_SQL_CONNECTION_STRING` con `Authentication=ActiveDirectoryDefault` y el driver ODBC 18 instalado.
- Asegúrate de que `Dwh2.DimDate` tenga las claves de fecha requeridas antes de cargar hechos.
- Para CSL (UniversalShipment) el enfoque es análogo; el generador/validador ya está listo y el ETL se ampliará en siguientes pasos.

## Próximos pasos
- Extender el ETL a CSL (`FactShipment`) y dimensiones relacionadas (puertos, unidades, niveles de servicio, etc.).
- Añadir filtros por fecha/carpeta, re‑proceso seguro (log de archivos procesados) y carga incremental.
- Integración con SFTP para origen en producción.
