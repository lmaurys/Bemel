# XML validation tools

This repository includes a permissive XSD for CargoWise UniversalTransaction XML files and a script to validate AR_*.xml files found under `XMLS_COL/**`.

Contents:
- `XSD/UniversalTransaction.xsd`: Schema ensuring the correct root element and namespace while allowing all nested content (lax).
- `XSD/UniversalTransaction.strict.xsd`: Strict schema inferred from all AR_*.xml samples.
- `XSD/UniversalShipment.strict.xsd`: Strict schema inferred from all CSL*.xml samples.
- `XSD/CommonTypes.xsd`: Common reusable XSD types included by the strict schemas.
- `tools/validate_ar_xml.py`: Python validator scanning `XMLS_COL/**/AR_*.xml`.
- `tools/gen_xsd_from_xmls.py`: Script to regenerate a strict XSD by analyzing all AR_*.xml files.
- `tools/gen_xsd_from_csl_xmls.py`: Script to regenerate a strict XSD by analyzing all CSL*.xml files.
- `tools/requirements.txt`: Python dependency list.
- `tools/sql/dwh2_ddl.sql`: Azure SQL DDL to create the Dwh2 star schema (Dims/Fact).
- `tools/sql/dwh2_load_dimdate.sql`: Stored procedure to populate `Dwh2.DimDate`.
- `tools/etl/load_ar_to_sql.py`: Genera un script T-SQL para upsert de dimensiones e inserción de hechos AR desde XML locales (carga limitada).

Usage
1. Create a virtual environment (optional but recommended) and install dependencies.
2. Run the validator.

Commands (macOS, zsh):
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r tools/requirements.txt
python tools/validate_ar_xml.py
```

You can pass a custom XSD path:
```
python tools/validate_ar_xml.py XSD/UniversalTransaction.xsd
```

Strict XSD (optional)
- Generate the strict schema from the current XML samples and validate against it:
```
python tools/gen_xsd_from_xmls.py
python tools/validate_ar_xml.py XSD/UniversalTransaction.strict.xsd
```
Notes:
- The strict XSD is inferred and aims to be comprehensive while avoiding over-constrained types. If new structures appear, regenerate the XSD.

## Manual de ejecución

### 1) Validación de XML (AR y CSL)
- Activar entorno e instalar dependencias (una vez):
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r tools/requirements.txt
```
- Generar XSDs estrictos (opcional si ya existen):
```bash
source .venv/bin/activate
python tools/gen_xsd_from_xmls.py           # UniversalTransaction.strict.xsd (AR)
python tools/gen_xsd_from_csl_xmls.py       # UniversalShipment.strict.xsd (CSL)
```
- Validar AR (UniversalTransaction):
```bash
source .venv/bin/activate
python tools/validate_ar_xml.py XSD/UniversalTransaction.strict.xsd AR_
```
- Validar CSL (UniversalShipment):
```bash
source .venv/bin/activate
python tools/validate_ar_xml.py XSD/UniversalShipment.strict.xsd CSL
```

### 2) Creación de estructura de base de datos (Azure SQL, esquema Dwh2)
- Abrir `tools/sql/dwh2_ddl.sql` en Azure Data Studio/SSMS y ejecutarlo sobre tu Azure SQL Database.
- El script crea el esquema `Dwh2`, todas las tablas `Dim*` y `Fact*`, claves foráneas e índices básicos.

### 3) Carga de DimDate
- Ejecutar `tools/sql/dwh2_load_dimdate.sql` para crear el procedimiento de carga.
- Poblar el calendario con el rango deseado:
```sql
EXEC Dwh2.SpLoadDimDate @StartDate = '2000-01-01', @EndDate = '2040-12-31', @DeleteExisting = 0;
```

### 4) ETL inicial (AR) – Generar script SQL desde XML locales
- Genera un script SQL para cargar un subconjunto (limit N) de archivos `AR_*.xml`:
```bash
python tools/etl/load_ar_to_sql.py --limit 50
```
- El script se guarda en `tools/sql/out/load_ar_YYYYMMDDHHMMSS.sql`. Revísalo y ejecútalo en Azure SQL. Asegúrate de tener `Dwh2.DimDate` poblada para los DateKeys referenciados.

### Notas
- Los XSD estrictos incluyen `XSD/CommonTypes.xsd`; mantenlos juntos en la misma carpeta.
- Si cambian las estructuras de los XML, vuelve a generar los XSD y repite la validación.

Notes
- The XSD is permissive on purpose to cover all provided files. If you need strict validation, replace the `xs:any` with the official CargoWise definitions and adjust accordingly.
