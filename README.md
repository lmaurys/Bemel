# XML validation, Data Warehouse (Dwh2) and ETL

This repository contains:
- XSDs for CargoWise UniversalTransaction (AR) and UniversalShipment (CSL), plus tools to validate and regenerate strict schemas.
- An Azure SQL star schema (Dwh2) covering AR and CSL, with dimensions, facts, and bridges.
- A Python ETL that parses XMLs under `XMLS_COL/YYYYMMDD` and upserts data into Dwh2.

Contents (key files):
- `XSD/UniversalTransaction.strict.xsd` and `XSD/UniversalShipment.strict.xsd` (+ `XSD/CommonTypes.xsd`).
- `tools/sql/dwh2_ddl.sql`: Creates the full Dwh2 model.
- `tools/sql/dwh2_load_dimdate.sql`: Stored procedure to populate `Dwh2.DimDate`.
- `tools/etl/init_db.py`: Applies the DDL and creates DimDate SP.
- `tools/etl/load_by_date.py`: Main ETL driver for a specific date folder (AR + CSL).
- Utilities: `tools/validate_ar_xml.py`, `tools/gen_xsd_from_xmls.py`, `tools/gen_xsd_from_csl_xmls.py`.

Highlights:
- Idempotency: `FactAccountsReceivableTransaction` is unique by Number; `FactShipment` has a unique filtered index on `ShipmentJobKey` to prevent duplicates on re-runs.
- Organization Registration Numbers stored as supporting dim `Dwh2.DimOrganizationRegistrationNumber` (per Organization + AddressType + Value) with uniqueness constraints.
- Clean rebuild: the DDL drops all existing Dwh2 objects first, then recreates the model only (no stray tables).
- Performance: ETL uses an in-memory upsert cache and batched commits. Tuning via `COMMIT_EVERY` env var.

Schema notes:
- Key dims: Country, Currency, Port, Company, User, Department, EventType, ActionPurpose, RecipientRole, AccountGroup, ScreeningStatus, ServiceLevel, ContainerMode, PaymentMethod, Unit, Branch, Organization, Job, Enterprise, Server, DataProvider, Co2eStatus.
- Facts: `FactAccountsReceivableTransaction` (AR), `FactShipment` (CSL).
- Bridges: `BridgeFactAROrganization`, `BridgeFactShipmentOrganization`, `BridgeFactARRecipientRole`, `FactARMessageNumber`.
- Removed (not used): `FactARPostingJournal`, `FactARPostingJournalDetail`, `FactARRatingBasis`.
- Renamed: `OrganizationRegistrationNumber` -> `DimOrganizationRegistrationNumber`.

Setup (optional commands):
```bash
# Create venv and install deps
python3 -m venv .venv
source .venv/bin/activate
pip install -r tools/requirements.txt

# Configure Azure SQL via environment (or use AZURE_SQL_CONNECTION_STRING)
export AZURE_SQL_SERVER=...
export AZURE_SQL_DATABASE=...
export AZURE_SQL_USER=...
export AZURE_SQL_PASSWORD=...

# Create schema and DimDate SP
python -m tools.etl.init_db --dates
```

Load by date (optional commands):
```bash
# Run AR + CSL for one date folder under XMLS_COL
python -m tools.etl.load_by_date --date 20250709 --quiet

# Options:
#   --only AR|CSL   Process only one type
#   --limit N       Max files
#   --quiet         Suppress expected fallback logs
# Tuning via env: COMMIT_EVERY=10 (commit every 10 files)
```

Example: run a date range (skip missing folders)
```bash
# With venv active; commit every 10 files and continue even if a day's folder is missing
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
	r=subprocess.run(['python','-m','tools.etl.load_by_date','--date',d,'--quiet'], env=env)
	print('=== Done', d, 'exit', r.returncode, flush=True)
	# continue even on failure (e.g., missing folder)
	cur+=datetime.timedelta(days=1)
print('=== All done ===', flush=True)
PY
```

Validate XMLs (optional commands):
```bash
python tools/validate_ar_xml.py XSD/UniversalTransaction.strict.xsd AR_
python tools/validate_ar_xml.py XSD/UniversalShipment.strict.xsd CSL
```

Notes
- Strict XSDs are inferred from current samples; regenerate if structures change.
- ETL prints a summary (start, end, duration, processed). Ensure `DimDate` is populated for referenced dates.
