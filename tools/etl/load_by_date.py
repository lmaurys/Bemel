#!/usr/bin/env python3
"""
ETL por fecha o rango: recorre XMLS_COL/YYYYMMDD, procesa AR_*.xml y CSL*.xml, y hace upsert directo en Azure SQL.

Uso:
    # Un solo día
    python -m tools.etl.load_by_date --date 20250707 [--only AR|CSL] [--limit N]

    # Rango (inclusive). Si falta una carpeta, continúa sin fallar.
    python -m tools.etl.load_by_date --from 20250707 --to 20250812 [--only AR|CSL] [--limit N]

Conexión DB y ruta XML:
    - AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USER, AZURE_SQL_PASSWORD, AZURE_SQL_AUTHENTICATION=SqlPassword
    - Opcional: AZURE_SQL_CONNECTION_STRING
    - Opcional: XML_ROOT (sobrescribe la ruta por defecto a XMLS_COL)
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from glob import glob
from datetime import datetime, timedelta
import time
from typing import Dict, Optional, Tuple, List

import pyodbc
from lxml import etree

# Ensure repository root is on sys.path so absolute imports work when run as a script
import os as _os
import sys as _sys
_ROOT = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", ".."))
if _ROOT not in _sys.path:
    _sys.path.insert(0, _ROOT)
from tools.etl.config import build_connection_string, Settings

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
XML_ROOT = Settings.xml_root or os.path.join(ROOT, "XMLS_COL")
NS = {"u": "http://www.cargowise.com/Schemas/Universal/2011/11"}
# Quiet mode suppresses expected fallback logs (default on; set QUIET=0 to see verbose)
QUIET = (os.getenv("QUIET", "1") == "1")
# Commit batching: commit every N files (default 1 = current behavior)
COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "5"))
# Batch controls for executemany usage
ENABLE_BATCH = (os.getenv("ENABLE_BATCH", "1") == "1")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))


# Tables to include in per-file summaries (facts and key bridges)
FACT_TABLES: List[str] = [
    'Dwh2.FactAccountsReceivableTransaction',
    'Dwh2.FactShipment',
    'Dwh2.FactSubShipment',
    'Dwh2.FactContainer',
    'Dwh2.FactPackingLine',
    'Dwh2.FactTransportLeg',
    'Dwh2.FactChargeLine',
    'Dwh2.FactJobCosting',
    'Dwh2.FactPostingJournal',
    'Dwh2.FactPostingJournalDetail',
    'Dwh2.FactAdditionalReference',
    'Dwh2.FactMessageNumber',
    'Dwh2.FactEventDate',
    'Dwh2.FactMilestone',
    'Dwh2.FactException',
    'Dwh2.FactNote',
    'Dwh2.FactFileIngestion',
    'Dwh2.BridgeFactShipmentOrganization',
]


class TableCounter:
    """Track adds/updates by table for a single file."""
    def __init__(self) -> None:
        self.counts: Dict[str, Dict[str, int]] = {}
        # Preload all fact/bridge tables so they appear even with zero activity
        for t in FACT_TABLES:
            self.counts[t] = {"added": 0, "updated": 0}

    def add(self, table: str, added: int = 0, updated: int = 0) -> None:
        rec = self.counts.get(table)
        if not rec:
            rec = {"added": 0, "updated": 0}
            self.counts[table] = rec
        if added:
            rec["added"] += int(added)
        if updated:
            rec["updated"] += int(updated)

    def summary_lines(self) -> List[str]:
        lines: List[str] = []
        for t in FACT_TABLES:
            c = self.counts.get(t, {"added": 0, "updated": 0})
            lines.append(f"{t}: added {c['added']}, updated {c['updated']}")
        return lines
class UpsertError(Exception):
    pass

# Per-run caches to reduce DB round-trips for dimensions
# Keyed by (fully-qualified table name like [Dwh2].[DimCountry], code)
_DIM_KEY_CACHE: Dict[Tuple[str, str], int] = {}
_DIM_ATTR_CACHE: Dict[Tuple[str, str], Tuple[Optional[str], Tuple[Tuple[str, object], ...]]] = {}


def _parse_file_ingestion(path: str, folder_date: Optional[str] = None) -> Tuple[Optional[str], str, Optional[int], Optional[str]]:
    """Parse Source, FileName, FileDateKey, FileTime from filename.

    Returns FileTime formatted as HH:MM:SS.fff (SQL TIME(3) compatible).

    Supports patterns like:
    - AR_FCOF00000138_COD_20250707100815.0365.xml -> Source AR, DateKey 20250707, Time 10:08:15.036
    - CSL040000903_SHP040001510_RDB_20250709_0145.xml -> Source CSL, DateKey 20250709, Time 01:45:00.000
    If parsing fails, falls back to folder_date (YYYYMMDD) for DateKey; Time stays None.
    Returns (source, file_name, date_key, time_str)
    """
    fname = os.path.basename(path)
    source: Optional[str] = None
    dkey: Optional[int] = None
    tstr: Optional[str] = None
    # AR pattern with compact datetime and optional fractional seconds
    m = re.match(r"^AR_.*_(\d{8})(\d{6})(?:\.(\d+))?\.xml$", fname, flags=re.IGNORECASE)
    if m:
        source = "AR"
        dkey = int(m.group(1))
        sec = m.group(2)  # hhmmss
        frac = (m.group(3) or "")
        hh, mm, ss = sec[0:2], sec[2:4], sec[4:6]
        ms = (frac[:3] if frac else "000").ljust(3, "0")
        tstr = f"{hh}:{mm}:{ss}.{ms}"
    else:
        # CSL pattern with underscore between date and time
        m2 = re.match(r"^CSL[^_]*_.*_(\d{8})_(\d{4})\.xml$", fname, flags=re.IGNORECASE)
        if m2:
            source = "CSL"
            dkey = int(m2.group(1))
            hm = m2.group(2)
            hh, mm = hm[0:2], hm[2:4]
            tstr = f"{hh}:{mm}:00.000"
        else:
            # Try simpler prefixes
            if fname.upper().startswith("AR_"):
                source = "AR"
            elif fname.upper().startswith("CSL"):
                source = "CSL"
    if dkey is None and folder_date and re.match(r"^\d{8}$", folder_date):
        try:
            dkey = int(folder_date)
        except Exception:
            dkey = None
    return source, fname, dkey, tstr


def _insert_file_ingestion(cur: pyodbc.Cursor, source: Optional[str], file_name: str, file_date_key: Optional[int], file_time: Optional[str]) -> None:
    """Idempotent insert into Dwh2.FactFileIngestion using FileName as unique key."""
    try:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM Dwh2.FactFileIngestion WHERE FileName=?) "
            "INSERT INTO Dwh2.FactFileIngestion (Source, FileName, FileDateKey, FileTime) VALUES (?,?,?,?)",
            file_name, (source or None), file_name, file_date_key, file_time
        )
    except Exception:
        # Silently ignore if unique constraint hit concurrently
        pass


def _datekey_from_iso(dt_str: Optional[str]) -> Optional[int]:
    if not dt_str:
        return None
    s = dt_str.strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if not m:
        return None
    return int(m.group(1) + m.group(2) + m.group(3))


def _time_from_iso(dt_str: Optional[str]) -> Optional[str]:
    if not dt_str:
        return None
    if "T" not in dt_str:
        return None
    return dt_str.split("T", 1)[1][:12]


def text(node: Optional[etree._Element]) -> str:
    return (node.text or "").strip() if node is not None else ""


def parse_datekey(s: str) -> Optional[int]:
    s = (s or "").strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if not m:
        return None
    return int(m.group(1) + m.group(2) + m.group(3))


def connect() -> pyodbc.Connection:
    conn_str = build_connection_string()
    if not conn_str:
        raise RuntimeError("Faltan variables de conexión a Azure SQL")
    cn = pyodbc.connect(conn_str)
    try:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON;")
        cur.close()
    except Exception:
        pass
    return cn


# ---------- Upsert helpers (devuelven surrogate keys) ----------
def _clean_str(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    if not isinstance(s, str):
        return s  # type: ignore[return-value]
    # Compact whitespace and trim
    return re.sub(r"\s+", " ", s).strip()

def _upsert_scalar_dim(cur: pyodbc.Cursor, table: str, code_col: str, code: str,
                       name_col: Optional[str] = None, name: Optional[str] = None,
                       extra_cols: Optional[Dict[str, object]] = None,
                       key_col: Optional[str] = None) -> Optional[int]:
    code = _clean_str(code) or ""
    name = _clean_str(name) if name is not None else None
    if not code:
        return None
    if not key_col:
        raise ValueError("key_col es obligatorio para _upsert_scalar_dim")
    # Fallback para columnas NOT NULL tipo Name/Description
    if name_col is not None and (name is None or str(name).strip() == ""):
        name = code
    # Update then insert if not exists
    # Quote table as [schema].[name]
    if "[" in table:
        table_sql = table
    else:
        if "." in table:
            sch, tbl = table.split(".", 1)
            table_sql = f"[{sch}].[{tbl}]"
        else:
            table_sql = f"[{table}]"
    # Build current attribute signature for caching comparisons
    curr_attrs_name = name if name_col else None
    curr_attrs_extra_items: Tuple[Tuple[str, object], ...] = tuple(sorted([
        (k, (_clean_str(v) if isinstance(v, str) else v)) for k, v in (extra_cols or {}).items()
    ]))
    cache_key = (table_sql, code)
    prev = _DIM_ATTR_CACHE.get(cache_key)
    cached_key = _DIM_KEY_CACHE.get(cache_key)
    # If we have a cached key, only update once per change of attributes
    if cached_key is not None:
        if (name_col or extra_cols) and prev != (curr_attrs_name, curr_attrs_extra_items):
            set_parts = []
            params: list[object] = []
            if name_col and curr_attrs_name is not None:
                set_parts.append(f"[{name_col}] = ?")
                params.append(curr_attrs_name)
            for k, v in curr_attrs_extra_items:
                set_parts.append(f"[{k}] = ?")
                params.append(v)
            if set_parts:
                set_sql = ", ".join(set_parts) + ", UpdatedAt = SYSUTCDATETIME()"
                cur.execute(f"UPDATE {table_sql} SET {set_sql} WHERE [{code_col}] = ?", *params, code)
                _DIM_ATTR_CACHE[cache_key] = (curr_attrs_name, curr_attrs_extra_items)
        return cached_key
    # No cache: perform update-then-select path
    if name_col and name is not None:
        cur.execute(f"UPDATE {table_sql} SET [{name_col}] = ?, UpdatedAt = SYSUTCDATETIME() WHERE [{code_col}] = ?", name, code)
    if extra_cols:
        clean_pairs = [(k, (_clean_str(v) if isinstance(v, str) else v)) for k, v in extra_cols.items()]
        if clean_pairs:
            sets = ", ".join([f"[{c}] = ?" for c, _ in clean_pairs])
            vals = [v for _, v in clean_pairs]
            cur.execute(f"UPDATE {table_sql} SET {sets}, UpdatedAt = SYSUTCDATETIME() WHERE [{code_col}] = ?",
                        *vals, code)
    cur.execute(f"SELECT {key_col} FROM {table_sql} WHERE [{code_col}] = ?", code)
    row = cur.fetchone()
    if row:
        k = int(row[0])
        _DIM_KEY_CACHE[cache_key] = k
        _DIM_ATTR_CACHE[cache_key] = (curr_attrs_name, curr_attrs_extra_items)
        return k
    # Insert
    cols = [code_col]
    vals = [code]
    if name_col:
        cols.append(name_col)
        vals.append(name)
    if extra_cols:
        for k, v in extra_cols.items():
            # Always include the column, even if None, to set NULL explicitly via param
            cols.append(k)
            vals.append(_clean_str(v) if isinstance(v, str) else v)
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT INTO {table_sql} ([" + "],[".join(cols) + "]) VALUES ({placeholders})"
    try:
        cur.execute(sql, *vals)
    except Exception as e:
        # Suppress noisy logs in quiet mode; caller will fallback
        if not QUIET:
            print(f"DIM INSERT FAIL table={table} code={code} cols={cols} vals={vals} err={e}")
        raise
    cur.execute(f"SELECT {key_col} FROM {table_sql} WHERE [{code_col}] = ?", code)
    row = cur.fetchone()
    if row:
        k = int(row[0])
        _DIM_KEY_CACHE[cache_key] = k
        _DIM_ATTR_CACHE[cache_key] = (curr_attrs_name, curr_attrs_extra_items)
        return k
    return None


def ensure_country(cur: pyodbc.Cursor, code: str, name: str) -> Optional[int]:
    code = _clean_str(code) or ""
    name = _clean_str(name) or code
    try:
        return _upsert_scalar_dim(cur, "Dwh2.DimCountry", "Code", code, "Name", name, key_col="CountryKey")
    except Exception:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimCountry] WHERE [Code]=?) "
            "INSERT INTO [Dwh2].[DimCountry] ([Code],[Name]) VALUES (?,?); "
            "ELSE UPDATE [Dwh2].[DimCountry] SET [Name]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
            code, code, name, name, code,
        )
        cur.execute("SELECT [CountryKey] FROM [Dwh2].[DimCountry] WHERE [Code]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None


def ensure_company(cur: pyodbc.Cursor, code: str, name: str, country_code: str) -> Optional[int]:
    code = _clean_str(code) or ""
    name = _clean_str(name) or code
    country_code = _clean_str(country_code) or ""
    ckey = None
    if country_code:
        # Try get existing country key; don't create blindly if empty
        cur.execute("SELECT CountryKey FROM Dwh2.DimCountry WHERE Code = ?", country_code)
        r = cur.fetchone()
        if r:
            ckey = int(r[0])
    try:
        # Upsert company (set CountryKey when available)
        return _upsert_scalar_dim(
            cur,
            "Dwh2.DimCompany",
            "Code",
            code,
            "Name",
            name,
            extra_cols={"CountryKey": ckey} if ckey else None,
            key_col="CompanyKey",
        )
    except Exception:
        if ckey is None:
            cur.execute(
                "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimCompany] WHERE [Code]=?) "
                "INSERT INTO [Dwh2].[DimCompany] ([Code],[Name]) VALUES (?,?); "
                "ELSE UPDATE [Dwh2].[DimCompany] SET [Name]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
                code, code, name, name, code,
            )
        else:
            cur.execute(
                "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimCompany] WHERE [Code]=?) "
                "INSERT INTO [Dwh2].[DimCompany] ([Code],[Name],[CountryKey]) VALUES (?,?,?); "
                "ELSE UPDATE [Dwh2].[DimCompany] SET [Name]=?, [CountryKey]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
                code, code, name, ckey, name, ckey, code,
            )
        cur.execute("SELECT [CompanyKey] FROM [Dwh2].[DimCompany] WHERE [Code]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None


def ensure_simple_dims(cur: pyodbc.Cursor, dims: Dict[str, Tuple]) -> Dict[str, Optional[int]]:
    keys: Dict[str, Optional[int]] = {}
    # Department
    code, name = dims.get("Department", ("", ""))
    code = (code or "").strip()
    name = (name or code).strip()
    try:
        keys["DepartmentKey"] = _upsert_scalar_dim(cur, "Dwh2.DimDepartment", "Code", code, "Name", name, key_col="DepartmentKey")
    except Exception:
        cur.execute("IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimDepartment] WHERE [Code]=?) INSERT INTO [Dwh2].[DimDepartment] ([Code],[Name]) VALUES (?,?); ELSE UPDATE [Dwh2].[DimDepartment] SET [Name]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
                    code, code, name, name, code)
        cur.execute("SELECT [DepartmentKey] FROM [Dwh2].[DimDepartment] WHERE [Code]=?", code)
        row = cur.fetchone()
        keys["DepartmentKey"] = int(row[0]) if row else None
    # EventType
    code, desc = dims.get("EventType", ("", ""))
    code = (code or "").strip()
    desc = (desc or code).strip()
    try:
        keys["EventTypeKey"] = _upsert_scalar_dim(cur, "Dwh2.DimEventType", "Code", code, "Description", desc, key_col="EventTypeKey")
    except Exception:
        cur.execute("IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimEventType] WHERE [Code]=?) INSERT INTO [Dwh2].[DimEventType] ([Code],[Description]) VALUES (?,?); ELSE UPDATE [Dwh2].[DimEventType] SET [Description]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
                    code, code, desc, desc, code)
        cur.execute("SELECT [EventTypeKey] FROM [Dwh2].[DimEventType] WHERE [Code]=?", code)
        row = cur.fetchone()
        keys["EventTypeKey"] = int(row[0]) if row else None
    # ActionPurpose
    code, desc = dims.get("ActionPurpose", ("", ""))
    code = (code or "").strip()
    desc = (desc or code).strip()
    try:
        keys["ActionPurposeKey"] = _upsert_scalar_dim(cur, "Dwh2.DimActionPurpose", "Code", code, "Description", desc, key_col="ActionPurposeKey")
    except Exception:
        cur.execute("IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimActionPurpose] WHERE [Code]=?) INSERT INTO [Dwh2].[DimActionPurpose] ([Code],[Description]) VALUES (?,?); ELSE UPDATE [Dwh2].[DimActionPurpose] SET [Description]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
                    code, code, desc, desc, code)
        cur.execute("SELECT [ActionPurposeKey] FROM [Dwh2].[DimActionPurpose] WHERE [Code]=?", code)
        row = cur.fetchone()
        keys["ActionPurposeKey"] = int(row[0]) if row else None
    # User
    code, name = dims.get("User", ("", ""))
    code = (code or "").strip()
    name = (name or code).strip()
    try:
        keys["UserKey"] = _upsert_scalar_dim(cur, "Dwh2.DimUser", "Code", code, "Name", name, key_col="UserKey")
    except Exception:
        cur.execute("IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimUser] WHERE [Code]=?) INSERT INTO [Dwh2].[DimUser] ([Code],[Name]) VALUES (?,?); ELSE UPDATE [Dwh2].[DimUser] SET [Name]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
                    code, code, name, name, code)
        cur.execute("SELECT [UserKey] FROM [Dwh2].[DimUser] WHERE [Code]=?", code)
        row = cur.fetchone()
        keys["UserKey"] = int(row[0]) if row else None
    # Enterprise
    (ent_id,) = dims.get("Enterprise", ("",))
    try:
        keys["EnterpriseKey"] = _upsert_scalar_dim(cur, "Dwh2.DimEnterprise", "EnterpriseId", ent_id, key_col="EnterpriseKey")
    except Exception:
        cur.execute("IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimEnterprise] WHERE [EnterpriseId]=?) INSERT INTO [Dwh2].[DimEnterprise] ([EnterpriseId]) VALUES (?);",
                    ent_id, ent_id)
        cur.execute("SELECT [EnterpriseKey] FROM [Dwh2].[DimEnterprise] WHERE [EnterpriseId]=?", ent_id)
        row = cur.fetchone()
        keys["EnterpriseKey"] = int(row[0]) if row else None
    # Server
    (srv_id,) = dims.get("Server", ("",))
    try:
        keys["ServerKey"] = _upsert_scalar_dim(cur, "Dwh2.DimServer", "ServerId", srv_id, key_col="ServerKey")
    except Exception:
        cur.execute("IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimServer] WHERE [ServerId]=?) INSERT INTO [Dwh2].[DimServer] ([ServerId]) VALUES (?);",
                    srv_id, srv_id)
        cur.execute("SELECT [ServerKey] FROM [Dwh2].[DimServer] WHERE [ServerId]=?", srv_id)
        row = cur.fetchone()
        keys["ServerKey"] = int(row[0]) if row else None
    # DataProvider
    (prov,) = dims.get("DataProvider", ("",))
    try:
        keys["DataProviderKey"] = _upsert_scalar_dim(cur, "Dwh2.DimDataProvider", "ProviderCode", prov, key_col="DataProviderKey")
    except Exception:
        cur.execute("IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimDataProvider] WHERE [ProviderCode]=?) INSERT INTO [Dwh2].[DimDataProvider] ([ProviderCode]) VALUES (?);",
                    prov, prov)
        cur.execute("SELECT [DataProviderKey] FROM [Dwh2].[DimDataProvider] WHERE [ProviderCode]=?", prov)
        row = cur.fetchone()
        keys["DataProviderKey"] = int(row[0]) if row else None
    return keys
# ---------- Shared helpers for rich dims ----------

def ensure_branch(cur: pyodbc.Cursor, code: str, name: str, extra: Optional[Dict[str, object]] = None) -> Optional[int]:
    code = _clean_str(code) or ""
    name = _clean_str(name) or code
    if not code:
        return None
    try:
        return _upsert_scalar_dim(
            cur,
            "Dwh2.DimBranch",
            "Code",
            code,
            "Name",
            name,
            extra_cols=extra,
            key_col="BranchKey",
        )
    except Exception:
        # Build dynamic SQL depending on provided extras
        cols = ["[Code]", "[Name]"]
        vals = [code, name]
        set_cols = ["[Name]=?"]
        set_vals = [name]
        if extra:
            for k, v in extra.items():
                cols.append(f"[{k}]")
                vals.append(v)
                set_cols.append(f"[{k}]=?")
                set_vals.append(v)
        insert_cols = ",".join(cols)
        insert_placeholders = ",".join(["?"] * len(vals))
        set_clause = ",".join(set_cols) + ", UpdatedAt=SYSUTCDATETIME()"
        cur.execute(
            f"IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimBranch] WHERE [Code]=?) "
            f"INSERT INTO [Dwh2].[DimBranch] ({insert_cols}) VALUES ({insert_placeholders}); "
            f"ELSE UPDATE [Dwh2].[DimBranch] SET {set_clause} WHERE [Code]=?;",
            code, *vals, *set_vals, code
        )
        cur.execute("SELECT [BranchKey] FROM [Dwh2].[DimBranch] WHERE [Code]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None


def ensure_organization_min(cur: pyodbc.Cursor, code: str, company_name: Optional[str] = None,
                            country_code: Optional[str] = None, port_code: Optional[str] = None) -> Optional[int]:
    code = _clean_str(code) or ""
    if not code:
        return None
    name = _clean_str(company_name) or code
    extra: Dict[str, object] = {}
    if country_code:
        cur.execute("SELECT CountryKey FROM Dwh2.DimCountry WHERE Code = ?", country_code)
        r = cur.fetchone()
        if r:
            extra["CountryKey"] = int(r[0])
    if port_code:
        cur.execute("SELECT PortKey FROM Dwh2.DimPort WHERE Code = ?", port_code)
        r = cur.fetchone()
        if r:
            extra["PortKey"] = int(r[0])
    try:
        return _upsert_scalar_dim(
            cur,
            "Dwh2.DimOrganization",
            "OrganizationCode",
            code,
            "CompanyName",
            name,
            extra_cols=extra if extra else None,
            key_col="OrganizationKey",
        )
    except Exception:
        # Fallback
        if extra:
            cur.execute(
                "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimOrganization] WHERE [OrganizationCode]=?) "
                "INSERT INTO [Dwh2].[DimOrganization] ([OrganizationCode],[CompanyName],[CountryKey],[PortKey]) VALUES (?,?,?,?); "
                "ELSE UPDATE [Dwh2].[DimOrganization] SET [CompanyName]=?,[CountryKey]=?,[PortKey]=?, UpdatedAt=SYSUTCDATETIME() WHERE [OrganizationCode]=?;",
                code, code, name, extra.get("CountryKey"), extra.get("PortKey"), name, extra.get("CountryKey"), extra.get("PortKey"), code
            )
        else:
            cur.execute(
                "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimOrganization] WHERE [OrganizationCode]=?) "
                "INSERT INTO [Dwh2].[DimOrganization] ([OrganizationCode],[CompanyName]) VALUES (?,?); "
                "ELSE UPDATE [Dwh2].[DimOrganization] SET [CompanyName]=?, UpdatedAt=SYSUTCDATETIME() WHERE [OrganizationCode]=?;",
                code, code, name, name, code
            )
        cur.execute("SELECT [OrganizationKey] FROM [Dwh2].[DimOrganization] WHERE [OrganizationCode]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None



# ---------- AR (Accounts Receivable) ----------

def parse_ar(path: str) -> Tuple[Dict, Dict]:
    parser = etree.XMLParser(remove_blank_text=False, ns_clean=True)
    doc = etree.parse(path, parser)
    root = doc.getroot()

    dc = root.find("u:TransactionInfo/u:DataContext", NS)
    company = dc.find("u:Company", NS) if dc is not None else None
    company_code = text(company.find("u:Code", NS)) if company is not None else ""
    company_name = text(company.find("u:Name", NS)) if company is not None else ""
    comp_country = company.find("u:Country", NS) if company is not None else None
    comp_country_code = text(comp_country.find("u:Code", NS)) if comp_country is not None else ""
    comp_country_name = text(comp_country.find("u:Name", NS)) if comp_country is not None else ""

    dept = dc.find("u:EventDepartment", NS) if dc is not None else None
    dept_code = text(dept.find("u:Code", NS)) if dept is not None else ""
    dept_name = text(dept.find("u:Name", NS)) if dept is not None else ""

    et = dc.find("u:EventType", NS) if dc is not None else None
    et_code = text(et.find("u:Code", NS)) if et is not None else ""
    et_desc = text(et.find("u:Description", NS)) if et is not None else ""

    ap = dc.find("u:ActionPurpose", NS) if dc is not None else None
    ap_code = text(ap.find("u:Code", NS)) if ap is not None else ""
    ap_desc = text(ap.find("u:Description", NS)) if ap is not None else ""

    usr = dc.find("u:EventUser", NS) if dc is not None else None
    usr_code = text(usr.find("u:Code", NS)) if usr is not None else ""
    usr_name = text(usr.find("u:Name", NS)) if usr is not None else ""

    # Event Branch (for DimBranch, used in AR context as issuer branch)
    evb = dc.find("u:EventBranch", NS) if dc is not None else None
    evb_code = text(evb.find("u:Code", NS)) if evb is not None else ""
    evb_name = text(evb.find("u:Name", NS)) if evb is not None else ""

    ent_id = text(dc.find("u:EnterpriseID", NS)) if dc is not None else ""
    srv_id = text(dc.find("u:ServerID", NS)) if dc is not None else ""
    provider = text(dc.find("u:DataProvider", NS)) if dc is not None else ""

    # DataSource (AccountingInvoice)
    ds_type = None
    ds_key = None
    dsc = dc.find("u:DataSourceCollection", NS) if dc is not None else None
    if dsc is not None:
        for ds in dsc.findall("u:DataSource", NS):
            t = text(ds.find("u:Type", NS))
            k = text(ds.find("u:Key", NS))
            if t:
                ds_type = t
                ds_key = k
                break

    # Trigger / event context
    event_reference = text(dc.find("u:EventReference", NS)) if dc is not None else ""
    timestamp = text(dc.find("u:Timestamp", NS)) if dc is not None else ""
    trigger_count = text(dc.find("u:TriggerCount", NS)) if dc is not None else ""
    trigger_desc = text(dc.find("u:TriggerDescription", NS)) if dc is not None else ""
    trigger_type = text(dc.find("u:TriggerType", NS)) if dc is not None else ""
    # Recipient roles
    recipient_roles = []
    rrc = dc.find("u:RecipientRoleCollection", NS) if dc is not None else None
    if rrc is not None:
        for rr in rrc.findall("u:RecipientRole", NS):
            recipient_roles.append((text(rr.find("u:Code", NS)), text(rr.find("u:Description", NS))))

    trigger_date = text(dc.find("u:TriggerDate", NS)) if dc is not None else ""
    trigger_datekey = parse_datekey(trigger_date)

    number = text(root.find(".//u:Number", NS))
    ledger = text(root.find(".//u:Ledger", NS)) or None

    local_currency = root.find(".//u:LocalCurrency", NS)
    lc_code = text(local_currency.find("u:Code", NS)) if local_currency is not None else ""
    lc_desc = text(local_currency.find("u:Description", NS)) if local_currency is not None else ""

    # Other header attributes
    category = text(root.find(".//u:Category", NS))
    agreed_payment_method = text(root.find(".//u:AgreedPaymentMethod", NS))
    compliance_subtype = text(root.find(".//u:ComplianceSubType", NS))
    create_time = text(root.find(".//u:CreateTime", NS))
    create_user = text(root.find(".//u:CreateUser", NS))
    exchange_rate = text(root.find(".//u:ExchangeRate", NS)) or None
    invoice_term = text(root.find(".//u:InvoiceTerm", NS))
    invoice_term_days = text(root.find(".//u:InvoiceTermDays", NS))
    job_invoice_number = text(root.find(".//u:JobInvoiceNumber", NS))
    check_drawer = text(root.find(".//u:CheckDrawer", NS))
    check_ref = text(root.find(".//u:CheckNumberOrPaymentRef", NS))
    drawer_bank = text(root.find(".//u:DrawerBank", NS))
    drawer_branch = text(root.find(".//u:DrawerBranch", NS))
    receipt_or_dd = text(root.find(".//u:ReceiptOrDirectDebitNumber", NS))
    requisition_status = text(root.find(".//u:RequisitionStatus", NS))
    transaction_reference = text(root.find(".//u:TransactionReference", NS))
    transaction_type = text(root.find(".//u:TransactionType", NS))
    number_of_supporting_docs = text(root.find(".//u:NumberOfSupportingDocuments", NS))
    outstanding_amount = text(root.find(".//u:OutstandingAmount", NS)) or None

    account_group = root.find(".//u:ARAccountGroup", NS)
    ag_code = text(account_group.find("u:Code", NS)) if account_group is not None else ""
    ag_desc = text(account_group.find("u:Description", NS)) if account_group is not None else ""

    transaction_date = text(root.find(".//u:TransactionDate", NS))
    post_date = text(root.find(".//u:PostDate", NS))
    due_date = text(root.find(".//u:DueDate", NS))
    tk = parse_datekey(transaction_date)
    pk = parse_datekey(post_date)
    dk = parse_datekey(due_date)

    def dec(node_name: str) -> Optional[str]:
        val = text(root.find(f".//u:{node_name}", NS))
        return val if val != "" else None

    amt_local_ex_vat = dec("LocalExVATAmount")
    amt_local_vat = dec("LocalVATAmount")
    amt_local_tax = dec("LocalTaxTransactionsAmount")
    amt_local_total = dec("LocalTotal")

    # OS currency and measures
    os_currency = root.find(".//u:OSCurrency", NS)
    os_code = text(os_currency.find("u:Code", NS)) if os_currency is not None else ""
    os_desc = text(os_currency.find("u:Description", NS)) if os_currency is not None else ""
    amt_os_ex_vat = dec("OSExGSTVATAmount")
    amt_os_vat = dec("OSGSTVATAmount")
    amt_os_tax = dec("OSTaxTransactionsAmount")
    amt_os_total = dec("OSTotal")

    # PlaceOfIssue free text
    place_of_issue_text = text(root.find(".//u:PlaceOfIssue", NS))

    # Message numbers
    message_numbers = []
    mnc = root.find(".//u:MessageNumberCollection", NS)
    if mnc is not None:
        for mn in mnc.findall("u:MessageNumber", NS):
            mtype = mn.get("Type") or ""
            mval = (mn.text or "").strip()
            if mval:
                message_numbers.append((mtype, mval))

    # Job (header)
    job = root.find(".//u:Job", NS)
    job_type = text(job.find("u:Type", NS)) if job is not None else ""
    job_key = text(job.find("u:Key", NS)) if job is not None else ""

    def bit(node_name: str) -> Optional[int]:
        v = text(root.find(f".//u:{node_name}", NS)).lower()
        if v in ("true", "1", "y", "yes"):
            return 1
        if v in ("false", "0", "n", "no"):
            return 0
        return None

    is_cancelled = bit("IsCancelled")
    is_created_by_matching = bit("IsCreatedByMatchingProcess")
    is_printed = bit("IsPrinted")

    # Exceptions
    exceptions = []
    # DateCollection at AR (header) level
    date_items = []
    dtc = root.find('.//u:DateCollection', NS)
    if dtc is not None:
        for di in dtc.findall('u:Date', NS):
            dtype = di.find('u:Type', NS)
            # Support both nested Code/Description and plain-text Type
            d_code = None
            d_desc = None
            if dtype is not None:
                d_code_candidate = text(dtype.find('u:Code', NS)) if dtype is not None else ""
                d_desc_candidate = text(dtype.find('u:Description', NS)) if dtype is not None else ""
                if d_code_candidate or d_desc_candidate:
                    d_code = d_code_candidate or None
                    d_desc = d_desc_candidate or None
                else:
                    # Plain text inside <Type>
                    d_code = text(dtype) or None
                    d_desc = None
            dtxt = text(di.find('u:DateTime', NS)) or None
            is_est_txt = text(di.find('u:IsEstimate', NS)) or ""
            is_est = 1 if is_est_txt.lower() in ("true", "1", "y", "yes") else (0 if is_est_txt.lower() in ("false", "0", "n", "no") else None)
            dval = text(di.find('u:Value', NS)) or None
            tz = text(di.find('u:TimeZone', NS)) or None
            # Fallback: if DateTime is missing, infer from Value when it looks like ISO
            eff_dt = dtxt or (dval if dval and re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", dval) else None)
            date_items.append({
                'Source': 'AR',
                'DateTypeCode': d_code,
                'DateTypeDescription': d_desc,
                'DateKey': _datekey_from_iso(eff_dt),
                'Time': _time_from_iso(eff_dt),
                'DateTimeText': dtxt,
                'IsEstimate': is_est,
                'Value': dval,
                'TimeZone': tz,
            })
    # Exceptions: collect all Exception nodes anywhere in the document
    for ex in root.findall('.//u:Exception', NS):
            code = text(ex.find('u:Code', NS)) or None
            typ = text(ex.find('u:Type', NS)) or None
            sev = text(ex.find('u:Severity', NS)) or None
            status = text(ex.find('u:Status', NS)) or None
            desc = text(ex.find('u:Description', NS)) or None
            is_resolved_txt = text(ex.find('u:IsResolved', NS))
            is_resolved = None if is_resolved_txt == '' else (1 if is_resolved_txt.lower() == 'true' else 0)
            raised = text(ex.find('u:RaisedDate', NS)) or None
            resolved = text(ex.find('u:ResolvedDate', NS)) or None
            # Rich fields
            exc_id = text(ex.find('u:ExceptionID', NS)) or None
            actioned_txt = text(ex.find('u:Actioned', NS))
            actioned = None if actioned_txt == '' else (1 if actioned_txt.lower() in ('true','1','y','yes') else 0)
            actioned_date = text(ex.find('u:ActionedDate', NS)) or None
            category = text(ex.find('u:Category', NS)) or None
            ev_date = text(ex.find('u:Date', NS)) or None
            duration_hours_txt = text(ex.find('u:DurationHours', NS)) or None
            try:
                duration_hours = int(duration_hours_txt) if (duration_hours_txt and duration_hours_txt.isdigit()) else None
            except Exception:
                duration_hours = None
            loc_code = text(ex.find('u:Location/u:Code', NS)) or None
            loc_name = text(ex.find('u:Location/u:Name', NS)) or None
            notes = text(ex.find('u:Notes', NS)) or None
            staff_code = text(ex.find('u:Staff/u:Code', NS)) or None
            staff_name = text(ex.find('u:Staff/u:Name', NS)) or None
            exceptions.append({
                'Source': 'AR',
                'ExceptionId': exc_id,
                'Code': code,
                'Type': typ,
                'Severity': sev,
                'Status': status,
                'Description': desc,
                'IsResolved': is_resolved,
                'Actioned': actioned,
                'ActionedDateKey': _datekey_from_iso(actioned_date),
                'ActionedTime': _time_from_iso(actioned_date),
                'Category': category,
                'EventDateKey': _datekey_from_iso(ev_date),
                'EventTime': _time_from_iso(ev_date),
                'DurationHours': duration_hours,
                'LocationCode': loc_code,
                'LocationName': loc_name,
                'Notes': notes,
                'StaffCode': staff_code,
                'StaffName': staff_name,
                'RaisedDateKey': _datekey_from_iso(raised),
                'RaisedTime': _time_from_iso(raised),
                'ResolvedDateKey': _datekey_from_iso(resolved),
                'ResolvedTime': _time_from_iso(resolved),
            })

    # Milestones: collect all Milestone nodes anywhere in the document
    milestones = []
    for ms in root.findall('.//u:Milestone', NS):
            desc = text(ms.find('u:Description', NS)) or None
            code = text(ms.find('u:EventCode', NS)) or None
            seq = text(ms.find('u:Sequence', NS))
            ad = text(ms.find('u:ActualDate', NS)) or None
            ed = text(ms.find('u:EstimatedDate', NS)) or None
            cr = text(ms.find('u:ConditionReference', NS)) or None
            ct = text(ms.find('u:ConditionType', NS)) or None
            milestones.append({
                'Source': 'AR',
                'Description': desc,
                'EventCode': code,
                'Sequence': int(seq) if seq.isdigit() else None,
                'ActualDateKey': _datekey_from_iso(ad),
                'ActualTime': _time_from_iso(ad),
                'EstimatedDateKey': _datekey_from_iso(ed),
                'EstimatedTime': _time_from_iso(ed),
                'ConditionReference': cr,
                'ConditionType': ct,
            })

    # Notes: collect all NoteCollection/Note with context and visibility
    notes = []
    for nc in root.findall('.//u:NoteCollection', NS):
        content_attr = nc.get('Content') if hasattr(nc, 'get') else None
        for n in nc.findall('u:Note', NS):
            notes.append({
                'Source': 'AR',
                'Description': text(n.find('u:Description', NS)) or None,
                'IsCustomDescription': (text(n.find('u:IsCustomDescription', NS)).lower() == 'true') if n.find('u:IsCustomDescription', NS) is not None else None,
                'NoteText': text(n.find('u:NoteText', NS)) or None,
                'NoteContextCode': text(n.find('u:NoteContext/u:Code', NS)) or None,
                'NoteContextDescription': text(n.find('u:NoteContext/u:Description', NS)) or None,
                'VisibilityCode': text(n.find('u:Visibility/u:Code', NS)) or None,
                'VisibilityDescription': text(n.find('u:Visibility/u:Description', NS)) or None,
                'Content': content_attr or None,
            })

    # Branch (issuing) and address
    br = root.find(".//u:Branch", NS)
    branch_code = text(br.find("u:Code", NS)) if br is not None else ""
    branch_name = text(br.find("u:Name", NS)) if br is not None else ""
    br_addr = root.find(".//u:BranchAddress", NS)
    branch_address = None
    if br_addr is not None:
        ba_type = text(br_addr.find("u:AddressType", NS))
        ba_a1 = text(br_addr.find("u:Address1", NS))
        ba_a2 = text(br_addr.find("u:Address2", NS))
        ba_over = text(br_addr.find("u:AddressOverride", NS))
        ba_short = text(br_addr.find("u:AddressShortCode", NS))
        ba_city = text(br_addr.find("u:City", NS))
        ba_state = text(br_addr.find("u:State", NS))
        ba_pc = text(br_addr.find("u:Postcode", NS))
        ba_email = text(br_addr.find("u:Email", NS))
        ba_fax = text(br_addr.find("u:Fax", NS))
        ba_phone = text(br_addr.find("u:Phone", NS))
        sc = br_addr.find("u:ScreeningStatus", NS)
        ba_scr_code = text(sc.find("u:Code", NS)) if sc is not None else ""
        ba_scr_desc = text(sc.find("u:Description", NS)) if sc is not None else ""
        bc = br_addr.find("u:Country", NS)
        ba_country_code = text(bc.find("u:Code", NS)) if bc is not None else ""
        ba_country_name = text(bc.find("u:Name", NS)) if bc is not None else ""
        bp = br_addr.find("u:Port", NS)
        ba_port_code = text(bp.find("u:Code", NS)) if bp is not None else ""
        ba_port_name = text(bp.find("u:Name", NS)) if bp is not None else ""
        branch_address = {
            "AddressType": ba_type,
            "Address1": ba_a1,
            "Address2": ba_a2,
            "AddressOverride": ba_over,
            "AddressShortCode": ba_short,
            "City": ba_city,
            "State": ba_state,
            "Postcode": ba_pc,
            "Email": ba_email,
            "Fax": ba_fax,
            "Phone": ba_phone,
            "ScreeningStatus": (ba_scr_code, ba_scr_desc),
            "Country": (ba_country_code, ba_country_name),
            "Port": (ba_port_code, ba_port_name),
        }

    # OrganizationAddress: collect all
    org_list = []
    for org in root.findall(".//u:OrganizationAddress", NS):
        org_code = text(org.find("u:OrganizationCode", NS))
        org_company = text(org.find("u:CompanyName", NS))
        address_type = text(org.find("u:AddressType", NS))
        address1 = text(org.find("u:Address1", NS))
        address2 = text(org.find("u:Address2", NS))
        address_override = text(org.find("u:AddressOverride", NS))
        address_short = text(org.find("u:AddressShortCode", NS))
        city = text(org.find("u:City", NS))
        state = text(org.find("u:State", NS))
        postcode = text(org.find("u:Postcode", NS))
        email = text(org.find("u:Email", NS))
        fax = text(org.find("u:Fax", NS))
        phone = text(org.find("u:Phone", NS))
        oc = org.find("u:Country", NS)
        org_country_code = text(oc.find("u:Code", NS)) if oc is not None else ""
        org_country_name = text(oc.find("u:Name", NS)) if oc is not None else ""
        op = org.find("u:Port", NS)
        org_port_code = text(op.find("u:Code", NS)) if op is not None else ""
        org_port_name = text(op.find("u:Name", NS)) if op is not None else ""
        # Gov reg num
        grn = text(org.find("u:GovRegNum", NS))
        grt = org.find("u:GovRegNumType", NS)
        grt_code = text(grt.find("u:Code", NS)) if grt is not None else ""
        grt_desc = text(grt.find("u:Description", NS)) if grt is not None else ""
        # RegistrationNumberCollection
        regnums = []
        rnc = org.find("u:RegistrationNumberCollection", NS)
        if rnc is not None:
            for rn in rnc.findall("u:RegistrationNumber", NS):
                t = rn.find("u:Type", NS)
                t_code = text(t.find("u:Code", NS)) if t is not None else ""
                t_desc = text(t.find("u:Description", NS)) if t is not None else ""
                coi = rn.find("u:CountryOfIssue", NS)
                coi_code = text(coi.find("u:Code", NS)) if coi is not None else ""
                coi_name = text(coi.find("u:Name", NS)) if coi is not None else ""
                val = text(rn.find("u:Value", NS))
                if val:
                    regnums.append({
                        "TypeCode": t_code,
                        "TypeDescription": t_desc,
                        "CountryOfIssueCode": coi_code,
                        "CountryOfIssueName": coi_name,
                        "Value": val,
                    })
        org_list.append({
            "OrganizationCode": org_code,
            "CompanyName": org_company,
            "AddressType": address_type,
            "Address1": address1,
            "Address2": address2,
            "AddressOverride": address_override,
            "AddressShortCode": address_short,
            "City": city,
            "State": state,
            "Postcode": postcode,
            "Email": email,
            "Fax": fax,
            "Phone": phone,
            "Country": (org_country_code, org_country_name),
            "Port": (org_port_code, org_port_name),
            "GovRegNum": grn,
            "GovRegNumType": (grt_code, grt_desc),
            "RegistrationNumbers": regnums,
        })

    # ChargeLineCollection from AR JobCosting (optional)
    # Important: AR UniversalTransaction files may embed Shipment/SubShipment sections with their own JobCosting.
    # We must ignore JobCosting nested under SubShipment to avoid misclassifying sub-shipment charges as AR charges.
    ar_charge_lines = []
    ar_jobcost_header: Optional[Dict[str, object]] = None
    jc_candidates = root.findall('.//u:JobCosting', NS)
    jc = None
    for jc_try in jc_candidates:
        # Walk up ancestors; skip if any ancestor local-name == 'SubShipment'
        parent = jc_try.getparent()
        is_under_sub = False
        while parent is not None:
            tag = parent.tag
            # Handle namespaces by testing local-name
            if isinstance(tag, str) and tag.split('}')[-1] == 'SubShipment':
                is_under_sub = True
                break
            parent = parent.getparent()
        if not is_under_sub:
            jc = jc_try
            break
    if jc is not None:
        # Header metrics present at AR-level JobCosting (not under SubShipment)
        def _jobcost_header_from(elem: etree._Element) -> Dict[str, object]:
            return {
                'Branch': (text(elem.find('u:Branch/u:Code', NS)) or None, text(elem.find('u:Branch/u:Name', NS)) or None),
                'Department': (text(elem.find('u:Department/u:Code', NS)) or None, text(elem.find('u:Department/u:Name', NS)) or None),
                'HomeBranch': (text(elem.find('u:HomeBranch/u:Code', NS)) or None, text(elem.find('u:HomeBranch/u:Name', NS)) or None),
                'Currency': (text(elem.find('u:Currency/u:Code', NS)) or None, text(elem.find('u:Currency/u:Description', NS)) or None),
                'OperationsStaff': (text(elem.find('u:OperationsStaff/u:Code', NS)) or None, text(elem.find('u:OperationsStaff/u:Name', NS)) or None),
                'ClientContractNumber': text(elem.find('u:ClientContractNumber', NS)) or None,
                # Totals/measures (strings, parsed later)
                'AccrualNotRecognized': text(elem.find('u:AccrualNotRecognized', NS)) or None,
                'AccrualRecognized': text(elem.find('u:AccrualRecognized', NS)) or None,
                'AgentRevenue': text(elem.find('u:AgentRevenue', NS)) or None,
                'LocalClientRevenue': text(elem.find('u:LocalClientRevenue', NS)) or None,
                'OtherDebtorRevenue': text(elem.find('u:OtherDebtorRevenue', NS)) or None,
                'TotalAccrual': text(elem.find('u:TotalAccrual', NS)) or None,
                'TotalCost': text(elem.find('u:TotalCost', NS)) or None,
                'TotalJobProfit': text(elem.find('u:TotalJobProfit', NS)) or None,
                'TotalRevenue': text(elem.find('u:TotalRevenue', NS)) or None,
                'TotalWIP': text(elem.find('u:TotalWIP', NS)) or None,
                'WIPNotRecognized': text(elem.find('u:WIPNotRecognized', NS)) or None,
                'WIPRecognized': text(elem.find('u:WIPRecognized', NS)) or None,
            }
        ar_jobcost_header = _jobcost_header_from(jc)
        clc = jc.find('u:ChargeLineCollection', NS)
        if clc is not None:
            for cl in clc.findall('u:ChargeLine', NS):
                ar_charge_lines.append({
                    'Branch': (text(cl.find('u:Branch/u:Code', NS)), text(cl.find('u:Branch/u:Name', NS))),
                    'Department': (text(cl.find('u:Department/u:Code', NS)), text(cl.find('u:Department/u:Name', NS))),
                    'ChargeCode': (text(cl.find('u:ChargeCode/u:Code', NS)), text(cl.find('u:ChargeCode/u:Description', NS))),
                    'ChargeGroup': (text(cl.find('u:ChargeCodeGroup/u:Code', NS)), text(cl.find('u:ChargeCodeGroup/u:Description', NS))),
                    'Description': text(cl.find('u:Description', NS)) or None,
                    'DisplaySequence': text(cl.find('u:DisplaySequence', NS)) or '',
                    'CreditorKey': text(cl.find('u:Creditor/u:Key', NS)) or '',
                    'DebtorKey': text(cl.find('u:Debtor/u:Key', NS)) or '',
                    'Cost': {
                        'APInvoiceNumber': text(cl.find('u:CostAPInvoiceNumber', NS)) or None,
                        'DueDate': text(cl.find('u:CostDueDate', NS)) or None,
                        'ExchangeRate': text(cl.find('u:CostExchangeRate', NS)) or None,
                        'InvoiceDate': text(cl.find('u:CostInvoiceDate', NS)) or None,
                        'IsPosted': text(cl.find('u:CostIsPosted', NS)) or None,
                        'LocalAmount': text(cl.find('u:CostLocalAmount', NS)) or None,
                        'OSAmount': text(cl.find('u:CostOSAmount', NS)) or None,
                        'OSCurrency': (text(cl.find('u:CostOSCurrency/u:Code', NS)), text(cl.find('u:CostOSCurrency/u:Description', NS))),
                        'OSGSTVATAmount': text(cl.find('u:CostOSGSTVATAmount', NS)) or None,
                    },
                    'Sell': {
                        'ExchangeRate': text(cl.find('u:SellExchangeRate', NS)) or None,
                        'GSTVAT': (text(cl.find('u:SellGSTVATID/u:TaxCode', NS)) or None, text(cl.find('u:SellGSTVATID/u:Description', NS)) or None),
                        'InvoiceType': text(cl.find('u:SellInvoiceType', NS)) or None,
                        'IsPosted': text(cl.find('u:SellIsPosted', NS)) or None,
                        'LocalAmount': text(cl.find('u:SellLocalAmount', NS)) or None,
                        'OSAmount': text(cl.find('u:SellOSAmount', NS)) or None,
                        'OSCurrency': (text(cl.find('u:SellOSCurrency/u:Code', NS)), text(cl.find('u:SellOSCurrency/u:Description', NS))),
                        'OSGSTVATAmount': text(cl.find('u:SellOSGSTVATAmount', NS)) or None,
                        'PostedTransaction': (
                            text(cl.find('u:SellPostedTransaction/u:Number', NS)) or None,
                            text(cl.find('u:SellPostedTransaction/u:TransactionType', NS)) or None,
                            text(cl.find('u:SellPostedTransaction/u:TransactionDate', NS)) or None,
                            text(cl.find('u:SellPostedTransaction/u:DueDate', NS)) or None,
                            text(cl.find('u:SellPostedTransaction/u:FullyPaidDate', NS)) or None,
                            text(cl.find('u:SellPostedTransaction/u:OutstandingAmount', NS)) or None,
                        ),
                    },
                    'SupplierReference': text(cl.find('u:SupplierReference', NS)) or None,
                    'SellReference': text(cl.find('u:SellReference', NS)) or None,
                })

    # TransportLegCollection possibly present in AR at top-level (rare)
    transport_legs_ar = []
    # Collect all TransportLegCollection nodes that are NOT under Shipment or SubShipment
    for tlc_ar in root.findall('.//u:TransportLegCollection', NS):
        # Walk up ancestors to ensure not under Shipment/SubShipment
        parent = tlc_ar.getparent()
        under_ship = False
        while parent is not None:
            tag = parent.tag if isinstance(parent.tag, str) else ''
            local = tag.split('}')[-1]
            if local in ('SubShipment', 'Shipment'):
                under_ship = True
                break
            parent = parent.getparent()
        if under_ship:
            continue
        for leg in tlc_ar.findall('u:TransportLeg', NS):
            order_txt = text(leg.find('u:LegOrder', NS))
            bs = leg.find('u:BookingStatus', NS)
            transport_legs_ar.append({
                'PortOfLoading': (text(leg.find('u:PortOfLoading/u:Code', NS)), text(leg.find('u:PortOfLoading/u:Name', NS))),
                'PortOfDischarge': (text(leg.find('u:PortOfDischarge/u:Code', NS)), text(leg.find('u:PortOfDischarge/u:Name', NS))),
                'Order': int(order_txt) if order_txt.isdigit() else None,
                'TransportMode': text(leg.find('u:TransportMode', NS)) or None,
                'VesselName': text(leg.find('u:VesselName', NS)) or None,
                'VesselLloydsIMO': text(leg.find('u:VesselLloydsIMO', NS)) or None,
                'VoyageFlightNo': text(leg.find('u:VoyageFlightNo', NS)) or None,
                'CarrierBookingReference': text(leg.find('u:CarrierBookingReference', NS)) or None,
                'BookingStatus': (text(bs.find('u:Code', NS)) if bs is not None else None, text(bs.find('u:Description', NS)) if bs is not None else None),
                'Carrier': (text(leg.find('u:Carrier/u:OrganizationCode', NS)), text(leg.find('u:Carrier/u:CompanyName', NS)), text(leg.find('u:Carrier/u:Country/u:Code', NS)), text(leg.find('u:Carrier/u:Port/u:Code', NS))),
                'Creditor': (text(leg.find('u:Creditor/u:OrganizationCode', NS)), text(leg.find('u:Creditor/u:CompanyName', NS)), text(leg.find('u:Creditor/u:Country/u:Code', NS)), text(leg.find('u:Creditor/u:Port/u:Code', NS))),
                'ActualArrival': text(leg.find('u:ActualArrival', NS)) or None,
                'ActualDeparture': text(leg.find('u:ActualDeparture', NS)) or None,
                'EstimatedArrival': text(leg.find('u:EstimatedArrival', NS)) or None,
                'EstimatedDeparture': text(leg.find('u:EstimatedDeparture', NS)) or None,
                'ScheduledArrival': text(leg.find('u:ScheduledArrival', NS)) or None,
                'ScheduledDeparture': text(leg.find('u:ScheduledDeparture', NS)) or None,
            })

    # ShipmentCollection embedded in AR (we'll use it to create a minimal FactShipment if CSL isn't present)
    shipment_from_ar: Optional[Dict[str, object]] = None
    sh = root.find('.//u:Shipment', NS)
    if sh is not None:
        def n(path_: str) -> Optional[etree._Element]:
            return sh.find(path_, NS)
        # Jobs from Shipment DataContext
        dc_sh = n('u:DataContext')
        consol_job_key = None
        shipment_job_key = None
        if dc_sh is not None:
            dsc = dc_sh.find('u:DataSourceCollection', NS)
            if dsc is not None:
                for ds in dsc.findall('u:DataSource', NS):
                    ds_type = text(ds.find('u:Type', NS))
                    ds_key_ = text(ds.find('u:Key', NS))
                    if ds_type == 'ForwardingConsol':
                        consol_job_key = ds_key_
                    elif ds_type == 'ForwardingShipment':
                        shipment_job_key = ds_key_
        # Ports
        def code_name(elem: Optional[etree._Element]) -> Tuple[str, str]:
            return (text(elem.find('u:Code', NS)) if elem is not None else '', text(elem.find('u:Name', NS)) if elem is not None else '')
        ports = {
            'PlaceOfDelivery': code_name(n('u:PlaceOfDelivery')),
            'PlaceOfIssue': code_name(n('u:PlaceOfIssue')),
            'PlaceOfReceipt': code_name(n('u:PlaceOfReceipt')),
            'PortFirstForeign': code_name(n('u:PortFirstForeign')),
            'PortLastForeign': code_name(n('u:PortLastForeign')),
            'PortOfDischarge': code_name(n('u:PortOfDischarge')),
            'PortOfFirstArrival': code_name(n('u:PortOfFirstArrival')),
            'PortOfLoading': code_name(n('u:PortOfLoading')),
            'EventBranchHomePort': code_name(n('u:EventBranchHomePort')),
        }
        def code_desc(elem: Optional[etree._Element]) -> Tuple[str, str]:
            return (text(elem.find('u:Code', NS)) if elem is not None else '', text(elem.find('u:Description', NS)) if elem is not None else '')
        shipment_from_ar = {
            'ConsolJob': ('ForwardingConsol', consol_job_key),
            'ShipmentJob': ('ForwardingShipment', shipment_job_key),
            'Ports': ports,
            'AWB': code_desc(n('u:AWBServiceLevel')),
            'Gateway': code_desc(n('u:GatewayServiceLevel')),
            'ShipmentType': code_desc(n('u:ShipmentType')),
            'ReleaseType': code_desc(n('u:ReleaseType')),
            'ScreeningStatus': code_desc(n('u:ScreeningStatus')),
            'PaymentMethod': code_desc(n('u:PaymentMethod')),
            'Currency': code_desc(n('u:FreightRateCurrency')),
            'ContainerMode': code_desc(n('u:ContainerMode')),
            'CO2eStatus': ('',''),
            'CO2eUnit': ('',''),
            'TotalVolumeUnit': code_desc(n('u:TotalVolumeUnit')),
            'TotalWeightUnit': code_desc(n('u:TotalWeightUnit')),
            'PacksUnit': code_desc(n('u:TotalNoOfPacksPackageType')),
            'Measures': {
                'ContainerCount': text(n('u:ContainerCount')) or None,
                'ChargeableRate': (text(n('u:ChargeableRate')) or None),
                'DocumentedChargeable': (text(n('u:DocumentedChargeable')) or None),
                'DocumentedVolume': (text(n('u:DocumentedVolume')) or None),
                'DocumentedWeight': (text(n('u:DocumentedWeight')) or None),
                'FreightRate': (text(n('u:FreightRate')) or None),
                'GreenhouseGasEmissionCO2e': (text(n('u:GreenhouseGasEmission/u:CO2e')) if n('u:GreenhouseGasEmission') is not None else None),
                'ManifestedChargeable': (text(n('u:ManifestedChargeable')) or None),
                'ManifestedVolume': (text(n('u:ManifestedVolume')) or None),
                'ManifestedWeight': (text(n('u:ManifestedWeight')) or None),
                'MaximumAllowablePackageHeight': (text(n('u:MaximumAllowablePackageHeight')) or None),
                'MaximumAllowablePackageLength': (text(n('u:MaximumAllowablePackageLength')) or None),
                'MaximumAllowablePackageWidth': (text(n('u:MaximumAllowablePackageWidth')) or None),
                'NoCopyBills': text(n('u:NoCopyBills')) or None,
                'NoOriginalBills': text(n('u:NoOriginalBills')) or None,
                'OuterPacks': text(n('u:OuterPacks')) or None,
                'TotalNoOfPacks': text(n('u:TotalNoOfPacks')) or None,
                'TotalPreallocatedChargeable': (text(n('u:TotalPreallocatedChargeable')) or None),
                'TotalPreallocatedVolume': (text(n('u:TotalPreallocatedVolume')) or None),
                'TotalPreallocatedWeight': (text(n('u:TotalPreallocatedWeight')) or None),
                'TotalVolume': (text(n('u:TotalVolume')) or None),
                'TotalWeight': (text(n('u:TotalWeight')) or None),
            },
            'Flags': {
                'IsCFSRegistered': (text(n('u:IsCFSRegistered')).lower() == 'true') if n('u:IsCFSRegistered') is not None else None,
                'IsDirectBooking': (text(n('u:IsDirectBooking')).lower() == 'true') if n('u:IsDirectBooking') is not None else None,
                'IsForwardRegistered': (text(n('u:IsForwardRegistered')).lower() == 'true') if n('u:IsForwardRegistered') is not None else None,
                'IsHazardous': (text(n('u:IsHazardous')).lower() == 'true') if n('u:IsHazardous') is not None else None,
                'IsNeutralMaster': (text(n('u:IsNeutralMaster')).lower() == 'true') if n('u:IsNeutralMaster') is not None else None,
                'RequiresTemperatureControl': (text(n('u:RequiresTemperatureControl')).lower() == 'true') if n('u:RequiresTemperatureControl') is not None else None,
            }
        }

        # Helper to parse a PackingLine element into a dict
        def _parse_packing_line(elem: etree._Element) -> Dict[str, object]:
            return {
                'CommodityCode': text(elem.find('u:Commodity/u:Code', NS)) or None,
                'CommodityDescription': text(elem.find('u:Commodity/u:Description', NS)) or None,
                'ContainerLink': (int(text(elem.find('u:ContainerLink', NS))) if text(elem.find('u:ContainerLink', NS)).isdigit() else None),
                'ContainerNumber': text(elem.find('u:ContainerNumber', NS)) or None,
                'ContainerPackingOrder': (int(text(elem.find('u:ContainerPackingOrder', NS))) if text(elem.find('u:ContainerPackingOrder', NS)).isdigit() else None),
                'CountryOfOriginCode': text(elem.find('u:CountryOfOrigin/u:Code', NS)) or None,
                'DetailedDescription': text(elem.find('u:DetailedDescription', NS)) or None,
                'EndItemNo': (int(text(elem.find('u:EndItemNo', NS))) if text(elem.find('u:EndItemNo', NS)).isdigit() else None),
                'ExportReferenceNumber': text(elem.find('u:ExportReferenceNumber', NS)) or None,
                'GoodsDescription': text(elem.find('u:GoodsDescription', NS)) or None,
                'HarmonisedCode': text(elem.find('u:HarmonisedCode', NS)) or None,
                'Height': text(elem.find('u:Height', NS)) or None,
                'ImportReferenceNumber': text(elem.find('u:ImportReferenceNumber', NS)) or None,
                'ItemNo': (int(text(elem.find('u:ItemNo', NS))) if text(elem.find('u:ItemNo', NS)).isdigit() else None),
                'LastKnownCFSStatusCode': text(elem.find('u:LastKnownCFSStatus/u:Code', NS)) or None,
                'LastKnownCFSStatusDate': text(elem.find('u:LastKnownCFSStatusDate', NS)) or None,
                'Length': text(elem.find('u:Length', NS)) or None,
                'LengthUnit': (text(elem.find('u:LengthUnit/u:Code', NS)) or None, text(elem.find('u:LengthUnit/u:Description', NS)) or None),
                'LinePrice': text(elem.find('u:LinePrice', NS)) or None,
                'Link': (int(text(elem.find('u:Link', NS))) if text(elem.find('u:Link', NS)).isdigit() else None),
                'LoadingMeters': text(elem.find('u:LoadingMeters', NS)) or None,
                'MarksAndNos': text(elem.find('u:MarksAndNos', NS)) or None,
                'OutturnComment': text(elem.find('u:OutturnComment', NS)) or None,
                'OutturnDamagedQty': (int(text(elem.find('u:OutturnDamagedQty', NS))) if text(elem.find('u:OutturnDamagedQty', NS)).isdigit() else None),
                'OutturnedHeight': text(elem.find('u:OutturnedHeight', NS)) or None,
                'OutturnedLength': text(elem.find('u:OutturnedLength', NS)) or None,
                'OutturnedVolume': text(elem.find('u:OutturnedVolume', NS)) or None,
                'OutturnedWeight': text(elem.find('u:OutturnedWeight', NS)) or None,
                'OutturnedWidth': text(elem.find('u:OutturnedWidth', NS)) or None,
                'OutturnPillagedQty': (int(text(elem.find('u:OutturnPillagedQty', NS))) if text(elem.find('u:OutturnPillagedQty', NS)).isdigit() else None),
                'OutturnQty': (int(text(elem.find('u:OutturnQty', NS))) if text(elem.find('u:OutturnQty', NS)).isdigit() else None),
                'PackingLineID': text(elem.find('u:PackingLineID', NS)) or None,
                'PackQty': (int(text(elem.find('u:PackQty', NS))) if text(elem.find('u:PackQty', NS)).isdigit() else None),
                'PackType': (text(elem.find('u:PackType/u:Code', NS)) or None, text(elem.find('u:PackType/u:Description', NS)) or None),
                'ReferenceNumber': text(elem.find('u:ReferenceNumber', NS)) or None,
                'RequiresTemperatureControl': ((text(elem.find('u:RequiresTemperatureControl', NS)).lower() == 'true') if elem.find('u:RequiresTemperatureControl', NS) is not None else None),
                'Volume': text(elem.find('u:Volume', NS)) or None,
                'VolumeUnit': (text(elem.find('u:VolumeUnit/u:Code', NS)) or None, text(elem.find('u:VolumeUnit/u:Description', NS)) or None),
                'Weight': text(elem.find('u:Weight', NS)) or None,
                'WeightUnit': (text(elem.find('u:WeightUnit/u:Code', NS)) or None, text(elem.find('u:WeightUnit/u:Description', NS)) or None),
                'Width': text(elem.find('u:Width', NS)) or None,
            }

        # SubShipmentCollection under Shipment (AR) -> capture for later insert
        sub_shipments_ar: list[Dict[str, object]] = []
        sub_coll_ar = sh.find('u:SubShipmentCollection', NS)
        if sub_coll_ar is not None:
            for sub in sub_coll_ar.findall('u:SubShipment', NS):
                def c(path_: str) -> Optional[etree._Element]:
                    return sub.find(path_, NS)
                sub_obj: Dict[str, object] = {}
                def simple_cd(parent: Optional[etree._Element]) -> Tuple[str, str]:
                    return (text(parent.find('u:Code', NS)) if parent is not None else '', text(parent.find('u:Description', NS)) if parent is not None else '')
                sub_obj['Ports'] = {
                    'PortOfLoading': (text(c('u:PortOfLoading/u:Code')), text(c('u:PortOfLoading/u:Name'))),
                    'PortOfDischarge': (text(c('u:PortOfDischarge/u:Code')), text(c('u:PortOfDischarge/u:Name'))),
                    'PortOfFirstArrival': (text(c('u:PortOfFirstArrival/u:Code')), text(c('u:PortOfFirstArrival/u:Name'))),
                    'PortOfDestination': (text(c('u:PortOfDestination/u:Code')), text(c('u:PortOfDestination/u:Name'))),
                    'PortOfOrigin': (text(c('u:PortOfOrigin/u:Code')), text(c('u:PortOfOrigin/u:Name'))),
                    'EventBranchHomePort': (text(c('u:EventBranchHomePort/u:Code')), text(c('u:EventBranchHomePort/u:Name'))),
                }
                sub_obj['Dims'] = {
                    'ServiceLevel': simple_cd(c('u:ServiceLevel')),
                    'ShipmentType': simple_cd(c('u:ShipmentType')),
                    'ReleaseType': simple_cd(c('u:ReleaseType')),
                    'ContainerMode': simple_cd(c('u:ContainerMode')),
                    'FreightRateCurrency': simple_cd(c('u:FreightRateCurrency')),
                    'GoodsValueCurrency': simple_cd(c('u:GoodsValueCurrency')),
                    'InsuranceValueCurrency': simple_cd(c('u:InsuranceValueCurrency')),
                    'TotalVolumeUnit': simple_cd(c('u:TotalVolumeUnit')),
                    'TotalWeightUnit': simple_cd(c('u:TotalWeightUnit')),
                    'PacksUnit': simple_cd(c('u:TotalNoOfPacksPackageType')),
                    'CO2eUnit': simple_cd(c('u:GreenhouseGasEmission/u:CO2eUnit')),
                }
                def dec_child(tag: str) -> Optional[str]:
                    val = text(c(tag))
                    return val if val != '' else None
                sub_obj['Attrs'] = {
                    'WayBillNumber': dec_child('u:WayBillNumber'),
                    'WayBillType': (text(c('u:WayBillType/u:Code')) or None, text(c('u:WayBillType/u:Description')) or None),
                    'VesselName': dec_child('u:VesselName'),
                    'VoyageFlightNo': dec_child('u:VoyageFlightNo'),
                    'LloydsIMO': dec_child('u:LloydsIMO'),
                    'TransportMode': dec_child('u:TransportMode/u:Description') or dec_child('u:TransportMode'),
                    'ContainerCount': text(c('u:ContainerCount')),
                    'ActualChargeable': dec_child('u:ActualChargeable'),
                    'DocumentedChargeable': dec_child('u:DocumentedChargeable'),
                    'DocumentedVolume': dec_child('u:DocumentedVolume'),
                    'DocumentedWeight': dec_child('u:DocumentedWeight'),
                    'GoodsValue': dec_child('u:GoodsValue'),
                    'InsuranceValue': dec_child('u:InsuranceValue'),
                    'FreightRate': dec_child('u:FreightRate'),
                    'TotalVolume': dec_child('u:TotalVolume'),
                    'TotalWeight': dec_child('u:TotalWeight'),
                    'TotalNoOfPacks': text(c('u:TotalNoOfPacks')),
                    'OuterPacks': text(c('u:OuterPacks')),
                    'GreenhouseGasEmissionCO2e': text(c('u:GreenhouseGasEmission/u:CO2e')) or None,
                    'IsBooking': text(c('u:IsBooking')),
                    'IsCancelled': text(c('u:IsCancelled')),
                    'IsCFSRegistered': text(c('u:IsCFSRegistered')),
                    'IsDirectBooking': text(c('u:IsDirectBooking')),
                    'IsForwardRegistered': text(c('u:IsForwardRegistered')),
                    'IsHighRisk': text(c('u:IsHighRisk')),
                    'IsNeutralMaster': text(c('u:IsNeutralMaster')),
                    'IsShipping': text(c('u:IsShipping')),
                    'IsSplitShipment': text(c('u:IsSplitShipment')),
                }
                # Legs under sub-shipment
                tlegs = []
                tlc2 = sub.find('u:TransportLegCollection', NS)
                if tlc2 is not None:
                    for leg in tlc2.findall('u:TransportLeg', NS):
                        order_txt = text(leg.find('u:LegOrder', NS))
                        bs = leg.find('u:BookingStatus', NS)
                        tlegs.append({
                            'PortOfLoading': (text(leg.find('u:PortOfLoading/u:Code', NS)), text(leg.find('u:PortOfLoading/u:Name', NS))),
                            'PortOfDischarge': (text(leg.find('u:PortOfDischarge/u:Code', NS)), text(leg.find('u:PortOfDischarge/u:Name', NS))),
                            'Order': int(order_txt) if order_txt.isdigit() else None,
                            'TransportMode': text(leg.find('u:TransportMode', NS)) or None,
                            'VesselName': text(leg.find('u:VesselName', NS)) or None,
                            'VesselLloydsIMO': text(leg.find('u:VesselLloydsIMO', NS)) or None,
                            'VoyageFlightNo': text(leg.find('u:VoyageFlightNo', NS)) or None,
                            'CarrierBookingReference': text(leg.find('u:CarrierBookingReference', NS)) or None,
                            'BookingStatus': (text(bs.find('u:Code', NS)) if bs is not None else None, text(bs.find('u:Description', NS)) if bs is not None else None),
                            'Carrier': (text(leg.find('u:Carrier/u:OrganizationCode', NS)), text(leg.find('u:Carrier/u:CompanyName', NS)), text(leg.find('u:Carrier/u:Country/u:Code', NS)), text(leg.find('u:Carrier/u:Port/u:Code', NS))),
                            'Creditor': (text(leg.find('u:Creditor/u:OrganizationCode', NS)), text(leg.find('u:Creditor/u:CompanyName', NS)), text(leg.find('u:Creditor/u:Country/u:Code', NS)), text(leg.find('u:Creditor/u:Port/u:Code', NS))),
                            'ActualArrival': text(leg.find('u:ActualArrival', NS)) or None,
                            'ActualDeparture': text(leg.find('u:ActualDeparture', NS)) or None,
                            'EstimatedArrival': text(leg.find('u:EstimatedArrival', NS)) or None,
                            'EstimatedDeparture': text(leg.find('u:EstimatedDeparture', NS)) or None,
                            'ScheduledArrival': text(leg.find('u:ScheduledArrival', NS)) or None,
                            'ScheduledDeparture': text(leg.find('u:ScheduledDeparture', NS)) or None,
                        })
                sub_obj['TransportLegs'] = tlegs
                # Charge lines under sub-shipment JobCosting
                charge_lines = []
                jc2 = sub.find('u:JobCosting', NS)
                if jc2 is not None:
                    # Capture JobCosting header for this sub-shipment
                    sub_obj['JobCostingHeader'] = {
                        'Branch': (text(jc2.find('u:Branch/u:Code', NS)) or None, text(jc2.find('u:Branch/u:Name', NS)) or None),
                        'Department': (text(jc2.find('u:Department/u:Code', NS)) or None, text(jc2.find('u:Department/u:Name', NS)) or None),
                        'HomeBranch': (text(jc2.find('u:HomeBranch/u:Code', NS)) or None, text(jc2.find('u:HomeBranch/u:Name', NS)) or None),
                        'Currency': (text(jc2.find('u:Currency/u:Code', NS)) or None, text(jc2.find('u:Currency/u:Description', NS)) or None),
                        'OperationsStaff': (text(jc2.find('u:OperationsStaff/u:Code', NS)) or None, text(jc2.find('u:OperationsStaff/u:Name', NS)) or None),
                        'ClientContractNumber': text(jc2.find('u:ClientContractNumber', NS)) or None,
                        'AccrualNotRecognized': text(jc2.find('u:AccrualNotRecognized', NS)) or None,
                        'AccrualRecognized': text(jc2.find('u:AccrualRecognized', NS)) or None,
                        'AgentRevenue': text(jc2.find('u:AgentRevenue', NS)) or None,
                        'LocalClientRevenue': text(jc2.find('u:LocalClientRevenue', NS)) or None,
                        'OtherDebtorRevenue': text(jc2.find('u:OtherDebtorRevenue', NS)) or None,
                        'TotalAccrual': text(jc2.find('u:TotalAccrual', NS)) or None,
                        'TotalCost': text(jc2.find('u:TotalCost', NS)) or None,
                        'TotalJobProfit': text(jc2.find('u:TotalJobProfit', NS)) or None,
                        'TotalRevenue': text(jc2.find('u:TotalRevenue', NS)) or None,
                        'TotalWIP': text(jc2.find('u:TotalWIP', NS)) or None,
                        'WIPNotRecognized': text(jc2.find('u:WIPNotRecognized', NS)) or None,
                        'WIPRecognized': text(jc2.find('u:WIPRecognized', NS)) or None,
                    }
                    clc = jc2.find('u:ChargeLineCollection', NS)
                    if clc is not None:
                        for cl in clc.findall('u:ChargeLine', NS):
                            charge_lines.append({
                                'Branch': (text(cl.find('u:Branch/u:Code', NS)), text(cl.find('u:Branch/u:Name', NS))),
                                'Department': (text(cl.find('u:Department/u:Code', NS)), text(cl.find('u:Department/u:Name', NS))),
                                'ChargeCode': (text(cl.find('u:ChargeCode/u:Code', NS)), text(cl.find('u:ChargeCode/u:Description', NS))),
                                'ChargeGroup': (text(cl.find('u:ChargeCodeGroup/u:Code', NS)), text(cl.find('u:ChargeCodeGroup/u:Description', NS))),
                                'Description': text(cl.find('u:Description', NS)) or None,
                                'DisplaySequence': text(cl.find('u:DisplaySequence', NS)) or '',
                                'CreditorKey': text(cl.find('u:Creditor/u:Key', NS)) or '',
                                'DebtorKey': text(cl.find('u:Debtor/u:Key', NS)) or '',
                                'Cost': {
                                    'APInvoiceNumber': text(cl.find('u:CostAPInvoiceNumber', NS)) or None,
                                    'DueDate': text(cl.find('u:CostDueDate', NS)) or None,
                                    'ExchangeRate': text(cl.find('u:CostExchangeRate', NS)) or None,
                                    'InvoiceDate': text(cl.find('u:CostInvoiceDate', NS)) or None,
                                    'IsPosted': text(cl.find('u:CostIsPosted', NS)) or None,
                                    'LocalAmount': text(cl.find('u:CostLocalAmount', NS)) or None,
                                    'OSAmount': text(cl.find('u:CostOSAmount', NS)) or None,
                                    'OSCurrency': (text(cl.find('u:CostOSCurrency/u:Code', NS)), text(cl.find('u:CostOSCurrency/u:Description', NS))),
                                    'OSGSTVATAmount': text(cl.find('u:CostOSGSTVATAmount', NS)) or None,
                                },
                                'Sell': {
                                    'ExchangeRate': text(cl.find('u:SellExchangeRate', NS)) or None,
                                    'GSTVAT': (text(cl.find('u:SellGSTVATID/u:TaxCode', NS)) or None, text(cl.find('u:SellGSTVATID/u:Description', NS)) or None),
                                    'InvoiceType': text(cl.find('u:SellInvoiceType', NS)) or None,
                                    'IsPosted': text(cl.find('u:SellIsPosted', NS)) or None,
                                    'LocalAmount': text(cl.find('u:SellLocalAmount', NS)) or None,
                                    'OSAmount': text(cl.find('u:SellOSAmount', NS)) or None,
                                    'OSCurrency': (text(cl.find('u:SellOSCurrency/u:Code', NS)), text(cl.find('u:SellOSCurrency/u:Description', NS))),
                                    'OSGSTVATAmount': text(cl.find('u:SellOSGSTVATAmount', NS)) or None,
                                    'PostedTransaction': (
                                        text(cl.find('u:SellPostedTransaction/u:Number', NS)) or None,
                                        text(cl.find('u:SellPostedTransaction/u:TransactionType', NS)) or None,
                                        text(cl.find('u:SellPostedTransaction/u:TransactionDate', NS)) or None,
                                        text(cl.find('u:SellPostedTransaction/u:DueDate', NS)) or None,
                                        text(cl.find('u:SellPostedTransaction/u:FullyPaidDate', NS)) or None,
                                        text(cl.find('u:SellPostedTransaction/u:OutstandingAmount', NS)) or None,
                                    ),
                                },
                                'SupplierReference': text(cl.find('u:SupplierReference', NS)) or None,
                                'SellReference': text(cl.find('u:SellReference', NS)) or None,
                            })
                sub_obj['ChargeLines'] = charge_lines
                # Packing lines under sub-shipment
                packs = []
                plc = sub.find('u:PackingLineCollection', NS)
                if plc is not None:
                    for pl in plc.findall('u:PackingLine', NS):
                        packs.append(_parse_packing_line(pl))
                sub_obj['PackingLines'] = packs
                # Containers under sub-shipment (AR)
                conts = []
                cc = sub.find('u:ContainerCollection', NS)
                if cc is not None:
                    for ctn in cc.findall('u:Container', NS):
                        def dec_ct(tag: str) -> Optional[str]:
                            return text(ctn.find(tag, NS)) or None
                        def bool_opt_ct(tag: str) -> Optional[bool]:
                            el = ctn.find(tag, NS)
                            return (text(el).lower() == 'true') if el is not None and text(el) != '' else None
                        conts.append({
                            'ContainerJobID': dec_ct('u:ContainerJobID'),
                            'ContainerNumber': dec_ct('u:ContainerNumber'),
                            'Link': (int(text(ctn.find('u:Link', NS))) if text(ctn.find('u:Link', NS)).isdigit() else None),
                            'ContainerType': (
                                text(ctn.find('u:ContainerType/u:Code', NS)) or None,
                                text(ctn.find('u:ContainerType/u:Description', NS)) or None,
                                text(ctn.find('u:ContainerType/u:ISOCode', NS)) or None,
                                text(ctn.find('u:ContainerType/u:Category/u:Code', NS)) or None,
                                text(ctn.find('u:ContainerType/u:Category/u:Description', NS)) or None,
                            ),
                            'DeliveryMode': dec_ct('u:DeliveryMode'),
                            'FCL_LCL_AIR': (
                                text(ctn.find('u:FCL_LCL_AIR/u:Code', NS)) or None,
                                text(ctn.find('u:FCL_LCL_AIR/u:Description', NS)) or None,
                            ),
                            'ContainerCount': (int(text(ctn.find('u:ContainerCount', NS))) if text(ctn.find('u:ContainerCount', NS)).isdigit() else None),
                            'Seal': dec_ct('u:Seal'),
                            'SealPartyTypeCode': text(ctn.find('u:SealPartyType/u:Code', NS)) or None,
                            'SecondSeal': dec_ct('u:SecondSeal'),
                            'SecondSealPartyTypeCode': text(ctn.find('u:SecondSealPartyType/u:Code', NS)) or None,
                            'ThirdSeal': dec_ct('u:ThirdSeal'),
                            'ThirdSealPartyTypeCode': text(ctn.find('u:ThirdSealPartyType/u:Code', NS)) or None,
                            'StowagePosition': dec_ct('u:StowagePosition'),
                            'LengthUnit': (text(ctn.find('u:LengthUnit/u:Code', NS)) or None, text(ctn.find('u:LengthUnit/u:Description', NS)) or None),
                            'VolumeUnit': (text(ctn.find('u:VolumeUnit/u:Code', NS)) or None, text(ctn.find('u:VolumeUnit/u:Description', NS)) or None),
                            'WeightUnit': (text(ctn.find('u:WeightUnit/u:Code', NS)) or None, text(ctn.find('u:WeightUnit/u:Description', NS)) or None),
                            'TotalHeight': dec_ct('u:TotalHeight'),
                            'TotalLength': dec_ct('u:TotalLength'),
                            'TotalWidth': dec_ct('u:TotalWidth'),
                            'TareWeight': dec_ct('u:TareWeight'),
                            'GrossWeight': dec_ct('u:GrossWeight'),
                            'GoodsWeight': dec_ct('u:GoodsWeight'),
                            'VolumeCapacity': dec_ct('u:VolumeCapacity'),
                            'WeightCapacity': dec_ct('u:WeightCapacity'),
                            'DunnageWeight': dec_ct('u:DunnageWeight'),
                            'OverhangBack': dec_ct('u:OverhangBack'),
                            'OverhangFront': dec_ct('u:OverhangFront'),
                            'OverhangHeight': dec_ct('u:OverhangHeight'),
                            'OverhangLeft': dec_ct('u:OverhangLeft'),
                            'OverhangRight': dec_ct('u:OverhangRight'),
                            'HumidityPercent': (int(text(ctn.find('u:HumidityPercent', NS))) if text(ctn.find('u:HumidityPercent', NS)).isdigit() else None),
                            'AirVentFlow': dec_ct('u:AirVentFlow'),
                            'AirVentFlowRateUnitCode': text(ctn.find('u:AirVentFlowRateUnit/u:Code', NS)) or None,
                            'NonOperatingReefer': bool_opt_ct('u:NonOperatingReefer'),
                            'IsCFSRegistered': bool_opt_ct('u:IsCFSRegistered'),
                            'IsControlledAtmosphere': bool_opt_ct('u:IsControlledAtmosphere'),
                            'IsDamaged': bool_opt_ct('u:IsDamaged'),
                            'IsEmptyContainer': bool_opt_ct('u:IsEmptyContainer'),
                            'IsSealOk': bool_opt_ct('u:IsSealOk'),
                            'IsShipperOwned': bool_opt_ct('u:IsShipperOwned'),
                            'ArrivalPickupByRail': bool_opt_ct('u:ArrivalPickupByRail'),
                            'DepartureDeliveryByRail': bool_opt_ct('u:DepartureDeliveryByRail'),
                            'ArrivalSlotDateTime': dec_ct('u:ArrivalSlotDateTime'),
                            'DepartureSlotDateTime': dec_ct('u:DepartureSlotDateTime'),
                            'EmptyReadyForReturn': dec_ct('u:EmptyReadyForReturn'),
                            'FCLWharfGateIn': dec_ct('u:FCLWharfGateIn'),
                            'FCLWharfGateOut': dec_ct('u:FCLWharfGateOut'),
                            'FCLStorageCommences': dec_ct('u:FCLStorageCommences'),
                            'LCLUnpack': dec_ct('u:LCLUnpack'),
                            'PackDate': dec_ct('u:PackDate'),
                        })
                sub_obj['Containers'] = conts
                # DateCollection under sub-shipment (AR)
                sub_dates = []
                sdc2 = sub.find('u:DateCollection', NS)
                if sdc2 is not None:
                    for di in sdc2.findall('u:Date', NS):
                        dtype = di.find('u:Type', NS)
                        d_code = None
                        d_desc = None
                        if dtype is not None:
                            d_code_candidate = text(dtype.find('u:Code', NS)) if dtype is not None else ""
                            d_desc_candidate = text(dtype.find('u:Description', NS)) if dtype is not None else ""
                            if d_code_candidate or d_desc_candidate:
                                d_code = d_code_candidate or None
                                d_desc = d_desc_candidate or None
                            else:
                                d_code = text(dtype) or None
                                d_desc = None
                        dtxt = text(di.find('u:DateTime', NS)) or None
                        is_est_txt = text(di.find('u:IsEstimate', NS)) or ""
                        is_est = 1 if is_est_txt.lower() in ("true", "1", "y", "yes") else (0 if is_est_txt.lower() in ("false", "0", "n", "no") else None)
                        dval = text(di.find('u:Value', NS)) or None
                        tz = text(di.find('u:TimeZone', NS)) or None
                        eff_dt = dtxt or (dval if dval and re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", dval) else None)
                        sub_dates.append({
                            'Source': 'AR',
                            'DateTypeCode': d_code,
                            'DateTypeDescription': d_desc,
                            'DateKey': _datekey_from_iso(eff_dt),
                            'Time': _time_from_iso(eff_dt),
                            'DateTimeText': dtxt,
                            'IsEstimate': is_est,
                            'Value': dval,
                            'TimeZone': tz,
                        })
                if sub_dates:
                    sub_obj['DateCollection'] = sub_dates
                # Additional references under sub-shipment
                add_refs2 = []
                arc2 = sub.find('u:AdditionalReferenceCollection', NS)
                if arc2 is not None:
                    for ar_ in arc2.findall('u:AdditionalReference', NS):
                        t = ar_.find('u:Type', NS)
                        coi = ar_.find('u:CountryOfIssue', NS)
                        issue = text(ar_.find('u:IssueDate', NS)) or None
                        add_refs2.append({
                            'Source': 'AR',
                            'TypeCode': text(t.find('u:Code', NS)) if t is not None else None,
                            'TypeDescription': text(t.find('u:Description', NS)) if t is not None else None,
                            'ReferenceNumber': text(ar_.find('u:ReferenceNumber', NS)) or None,
                            'ContextInformation': text(ar_.find('u:ContextInformation', NS)) or None,
                            'CountryOfIssue': (text(coi.find('u:Code', NS)) if coi is not None else None, text(coi.find('u:Name', NS)) if coi is not None else None),
                            'IssueDate': issue,
                        })
                sub_obj['AdditionalReferences'] = add_refs2
                sub_shipments_ar.append(sub_obj)

        # Shipment-level PackingLineCollection
        packs_sh = []
        plc_sh = sh.find('u:PackingLineCollection', NS)
        if plc_sh is not None:
            for pl in plc_sh.findall('u:PackingLine', NS):
                packs_sh.append(_parse_packing_line(pl))

        # Shipment-level DateCollection (AR)
        date_items_sh = []
        dtc_sh = sh.find('u:DateCollection', NS)
        if dtc_sh is not None:
            for di in dtc_sh.findall('u:Date', NS):
                dtype = di.find('u:Type', NS)
                d_code = None
                d_desc = None
                if dtype is not None:
                    d_code_candidate = text(dtype.find('u:Code', NS)) if dtype is not None else ""
                    d_desc_candidate = text(dtype.find('u:Description', NS)) if dtype is not None else ""
                    if d_code_candidate or d_desc_candidate:
                        d_code = d_code_candidate or None
                        d_desc = d_desc_candidate or None
                    else:
                        d_code = text(dtype) or None
                        d_desc = None
                dtxt = text(di.find('u:DateTime', NS)) or None
                is_est_txt = text(di.find('u:IsEstimate', NS)) or ""
                is_est = 1 if is_est_txt.lower() in ("true", "1", "y", "yes") else (0 if is_est_txt.lower() in ("false", "0", "n", "no") else None)
                dval = text(di.find('u:Value', NS)) or None
                tz = text(di.find('u:TimeZone', NS)) or None
                eff_dt = dtxt or (dval if dval and re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", dval) else None)
                date_items_sh.append({
                    'Source': 'AR',
                    'DateTypeCode': d_code,
                    'DateTypeDescription': d_desc,
                    'DateKey': _datekey_from_iso(eff_dt),
                    'Time': _time_from_iso(eff_dt),
                    'DateTimeText': dtxt,
                    'IsEstimate': is_est,
                    'Value': dval,
                    'TimeZone': tz,
                })

        # Shipment-level ContainerCollection
        conts_sh = []
        cc_sh = sh.find('u:ContainerCollection', NS)
        if cc_sh is not None:
            for ctn in cc_sh.findall('u:Container', NS):
                def dec_ct(tag: str) -> Optional[str]:
                    return text(ctn.find(tag, NS)) or None
                def bool_opt_ct(tag: str) -> Optional[bool]:
                    el = ctn.find(tag, NS)
                    return (text(el).lower() == 'true') if el is not None and text(el) != '' else None
                conts_sh.append({
                    'ContainerJobID': dec_ct('u:ContainerJobID'),
                    'ContainerNumber': dec_ct('u:ContainerNumber'),
                    'Link': (int(text(ctn.find('u:Link', NS))) if text(ctn.find('u:Link', NS)).isdigit() else None),
                    'ContainerType': (
                        text(ctn.find('u:ContainerType/u:Code', NS)) or None,
                        text(ctn.find('u:ContainerType/u:Description', NS)) or None,
                        text(ctn.find('u:ContainerType/u:ISOCode', NS)) or None,
                        text(ctn.find('u:ContainerType/u:Category/u:Code', NS)) or None,
                        text(ctn.find('u:ContainerType/u:Category/u:Description', NS)) or None,
                    ),
                    'DeliveryMode': dec_ct('u:DeliveryMode'),
                    'FCL_LCL_AIR': (
                        text(ctn.find('u:FCL_LCL_AIR/u:Code', NS)) or None,
                        text(ctn.find('u:FCL_LCL_AIR/u:Description', NS)) or None,
                    ),
                    'ContainerCount': (int(text(ctn.find('u:ContainerCount', NS))) if text(ctn.find('u:ContainerCount', NS)).isdigit() else None),
                    'Seal': dec_ct('u:Seal'),
                    'SealPartyTypeCode': text(ctn.find('u:SealPartyType/u:Code', NS)) or None,
                    'SecondSeal': dec_ct('u:SecondSeal'),
                    'SecondSealPartyTypeCode': text(ctn.find('u:SecondSealPartyType/u:Code', NS)) or None,
                    'ThirdSeal': dec_ct('u:ThirdSeal'),
                    'ThirdSealPartyTypeCode': text(ctn.find('u:ThirdSealPartyType/u:Code', NS)) or None,
                    'StowagePosition': dec_ct('u:StowagePosition'),
                    'LengthUnit': (text(ctn.find('u:LengthUnit/u:Code', NS)) or None, text(ctn.find('u:LengthUnit/u:Description', NS)) or None),
                    'VolumeUnit': (text(ctn.find('u:VolumeUnit/u:Code', NS)) or None, text(ctn.find('u:VolumeUnit/u:Description', NS)) or None),
                    'WeightUnit': (text(ctn.find('u:WeightUnit/u:Code', NS)) or None, text(ctn.find('u:WeightUnit/u:Description', NS)) or None),
                    'TotalHeight': dec_ct('u:TotalHeight'),
                    'TotalLength': dec_ct('u:TotalLength'),
                    'TotalWidth': dec_ct('u:TotalWidth'),
                    'TareWeight': dec_ct('u:TareWeight'),
                    'GrossWeight': dec_ct('u:GrossWeight'),
                    'GoodsWeight': dec_ct('u:GoodsWeight'),
                    'VolumeCapacity': dec_ct('u:VolumeCapacity'),
                    'WeightCapacity': dec_ct('u:WeightCapacity'),
                    'DunnageWeight': dec_ct('u:DunnageWeight'),
                    'OverhangBack': dec_ct('u:OverhangBack'),
                    'OverhangFront': dec_ct('u:OverhangFront'),
                    'OverhangHeight': dec_ct('u:OverhangHeight'),
                    'OverhangLeft': dec_ct('u:OverhangLeft'),
                    'OverhangRight': dec_ct('u:OverhangRight'),
                    'HumidityPercent': (int(text(ctn.find('u:HumidityPercent', NS))) if text(ctn.find('u:HumidityPercent', NS)).isdigit() else None),
                    'AirVentFlow': dec_ct('u:AirVentFlow'),
                    'AirVentFlowRateUnitCode': text(ctn.find('u:AirVentFlowRateUnit/u:Code', NS)) or None,
                    'NonOperatingReefer': bool_opt_ct('u:NonOperatingReefer'),
                    'IsCFSRegistered': bool_opt_ct('u:IsCFSRegistered'),
                    'IsControlledAtmosphere': bool_opt_ct('u:IsControlledAtmosphere'),
                    'IsDamaged': bool_opt_ct('u:IsDamaged'),
                    'IsEmptyContainer': bool_opt_ct('u:IsEmptyContainer'),
                    'IsSealOk': bool_opt_ct('u:IsSealOk'),
                    'IsShipperOwned': bool_opt_ct('u:IsShipperOwned'),
                    'ArrivalPickupByRail': bool_opt_ct('u:ArrivalPickupByRail'),
                    'DepartureDeliveryByRail': bool_opt_ct('u:DepartureDeliveryByRail'),
                    'ArrivalSlotDateTime': dec_ct('u:ArrivalSlotDateTime'),
                    'DepartureSlotDateTime': dec_ct('u:DepartureSlotDateTime'),
                    'EmptyReadyForReturn': dec_ct('u:EmptyReadyForReturn'),
                    'FCLWharfGateIn': dec_ct('u:FCLWharfGateIn'),
                    'FCLWharfGateOut': dec_ct('u:FCLWharfGateOut'),
                    'FCLStorageCommences': dec_ct('u:FCLStorageCommences'),
                    'LCLUnpack': dec_ct('u:LCLUnpack'),
                    'PackDate': dec_ct('u:PackDate'),
                })
        if conts_sh:
            shipment_from_ar['ContainersShipment'] = conts_sh

        if date_items_sh:
            shipment_from_ar['DateCollection'] = date_items_sh

        # Shipment-level TransportLegCollection (AR)
        legs_sh = []
        tlc_sh = sh.find('u:TransportLegCollection', NS)
        if tlc_sh is not None:
            for leg in tlc_sh.findall('u:TransportLeg', NS):
                order_txt = text(leg.find('u:LegOrder', NS))
                bs = leg.find('u:BookingStatus', NS)
                legs_sh.append({
                    'PortOfLoading': (text(leg.find('u:PortOfLoading/u:Code', NS)), text(leg.find('u:PortOfLoading/u:Name', NS))),
                    'PortOfDischarge': (text(leg.find('u:PortOfDischarge/u:Code', NS)), text(leg.find('u:PortOfDischarge/u:Name', NS))),
                    'Order': int(order_txt) if order_txt.isdigit() else None,
                    'TransportMode': text(leg.find('u:TransportMode', NS)) or None,
                    'VesselName': text(leg.find('u:VesselName', NS)) or None,
                    'VesselLloydsIMO': text(leg.find('u:VesselLloydsIMO', NS)) or None,
                    'VoyageFlightNo': text(leg.find('u:VoyageFlightNo', NS)) or None,
                    'CarrierBookingReference': text(leg.find('u:CarrierBookingReference', NS)) or None,
                    'BookingStatus': (text(bs.find('u:Code', NS)) if bs is not None else None, text(bs.find('u:Description', NS)) if bs is not None else None),
                    'Carrier': (text(leg.find('u:Carrier/u:OrganizationCode', NS)), text(leg.find('u:Carrier/u:CompanyName', NS)), text(leg.find('u:Carrier/u:Country/u:Code', NS)), text(leg.find('u:Carrier/u:Port/u:Code', NS))),
                    'Creditor': (text(leg.find('u:Creditor/u:OrganizationCode', NS)), text(leg.find('u:Creditor/u:CompanyName', NS)), text(leg.find('u:Creditor/u:Country/u:Code', NS)), text(leg.find('u:Creditor/u:Port/u:Code', NS))),
                    'ActualArrival': text(leg.find('u:ActualArrival', NS)) or None,
                    'ActualDeparture': text(leg.find('u:ActualDeparture', NS)) or None,
                    'EstimatedArrival': text(leg.find('u:EstimatedArrival', NS)) or None,
                    'EstimatedDeparture': text(leg.find('u:EstimatedDeparture', NS)) or None,
                    'ScheduledArrival': text(leg.find('u:ScheduledArrival', NS)) or None,
                    'ScheduledDeparture': text(leg.find('u:ScheduledDeparture', NS)) or None,
                })
        if legs_sh:
            shipment_from_ar['TransportLegsShipment'] = legs_sh

        if sub_shipments_ar or packs_sh or date_items_sh:
            shipment_from_ar['SubShipments'] = sub_shipments_ar
            if packs_sh:
                shipment_from_ar['PackingLinesShipment'] = packs_sh

    dims = {
        "Country": (comp_country_code, comp_country_name),
        "Company": (company_code, company_name, comp_country_code),
        "Branch": (branch_code, branch_name),
        "BranchAddress": branch_address,
        "Department": (dept_code, dept_name),
        "EventType": (et_code, et_desc),
        "ActionPurpose": (ap_code, ap_desc),
        "User": (usr_code, usr_name),
        "Enterprise": (ent_id,),
        "Server": (srv_id,),
        "DataProvider": (provider,),
        "Currency": (lc_code, lc_desc),
        "AccountGroup": (ag_code, ag_desc, "AR"),
        "Organizations": org_list,
    }
    fact = {
        "Number": number,
        "Ledger": ledger,
        "TransactionDateKey": tk,
        "PostDateKey": pk,
        "DueDateKey": dk,
        "TriggerDateKey": trigger_datekey,
        # DataSource & event context
        "DataSource": (ds_type, ds_key),
        "EventReference": event_reference,
        "Timestamp": timestamp,
        "TriggerCount": int(trigger_count) if trigger_count.isdigit() else None,
        "TriggerDescription": trigger_desc,
        "TriggerType": trigger_type,
        # Header attributes
        "AgreedPaymentMethod": agreed_payment_method,
        "Category": category,
        "ComplianceSubType": compliance_subtype,
        "CreateTime": create_time,
        "CreateUser": create_user,
        "ExchangeRate": exchange_rate,
        "InvoiceTerm": invoice_term,
        "InvoiceTermDays": int(invoice_term_days) if invoice_term_days.isdigit() else None,
        "JobInvoiceNumber": job_invoice_number,
        "CheckDrawer": check_drawer,
        "CheckNumberOrPaymentRef": check_ref,
        "DrawerBank": drawer_bank,
        "DrawerBranch": drawer_branch,
        "ReceiptOrDirectDebitNumber": receipt_or_dd,
        "RequisitionStatus": requisition_status,
        "TransactionReference": transaction_reference,
        "TransactionType": transaction_type,
        "NumberOfSupportingDocuments": int(number_of_supporting_docs) if number_of_supporting_docs.isdigit() else None,
        # Local amounts
        "LocalExVATAmount": amt_local_ex_vat,
        "LocalVATAmount": amt_local_vat,
        "LocalTaxTransactionsAmount": amt_local_tax,
        "LocalTotal": amt_local_total,
        # OS amounts & currency
        "OSCurrency": (os_code, os_desc),
        "OSExGSTVATAmount": amt_os_ex_vat,
        "OSGSTVATAmount": amt_os_vat,
        "OSTaxTransactionsAmount": amt_os_tax,
        "OSTotal": amt_os_total,
        "OutstandingAmount": outstanding_amount,
        # Place of issue
        "PlaceOfIssueText": place_of_issue_text,
        # Flags
        "IsCancelled": is_cancelled,
        "IsCreatedByMatchingProcess": is_created_by_matching,
        "IsPrinted": is_printed,
        # Job & collections
        "Job": (job_type, job_key),
    "MessageNumbers": message_numbers,
        "RecipientRoles": recipient_roles,
    "Exceptions": exceptions,
    "DateCollection": date_items,
    "Milestones": milestones,
    "ChargeLines": ar_charge_lines,
    "JobCostingHeader": ar_jobcost_header,
    "TransportLegs": transport_legs_ar,
    "Notes": notes,
    }

    if shipment_from_ar is not None:
        fact["ShipmentFromAR"] = shipment_from_ar

    # AdditionalReferenceCollection (attach to AR parent)
    add_refs = []
    for arc in root.findall('.//u:AdditionalReferenceCollection', NS):
        for ar_ in arc.findall('u:AdditionalReference', NS):
            t = ar_.find('u:Type', NS)
            coi = ar_.find('u:CountryOfIssue', NS)
            issue = text(ar_.find('u:IssueDate', NS)) or None
            add_refs.append({
                'Source': 'AR',
                'TypeCode': text(t.find('u:Code', NS)) if t is not None else None,
                'TypeDescription': text(t.find('u:Description', NS)) if t is not None else None,
                'ReferenceNumber': text(ar_.find('u:ReferenceNumber', NS)) or None,
                'ContextInformation': text(ar_.find('u:ContextInformation', NS)) or None,
                'CountryOfIssue': (text(coi.find('u:Code', NS)) if coi is not None else None, text(coi.find('u:Name', NS)) if coi is not None else None),
                'IssueDate': issue,
            })
    fact["AdditionalReferences"] = add_refs

    # PostingJournalCollection (AR only)
    posting_journals = []
    pjc = root.find('.//u:PostingJournalCollection', NS)
    if pjc is not None:
        for pj in pjc.findall('u:PostingJournal', NS):
            header = {}
            # Branch / Department
            header['Branch'] = (text(pj.find('u:Branch/u:Code', NS)), text(pj.find('u:Branch/u:Name', NS)))
            header['Department'] = (text(pj.find('u:Department/u:Code', NS)), text(pj.find('u:Department/u:Name', NS)))
            # Charge code and related classifications
            header['ChargeCode'] = text(pj.find('u:ChargeCode/u:Code', NS)) or None
            header['ChargeCodeDescription'] = text(pj.find('u:ChargeCode/u:Description', NS)) or None
            header['ChargeType'] = (text(pj.find('u:ChargeCode/u:ChargeType/u:Code', NS)) or None, text(pj.find('u:ChargeCode/u:ChargeType/u:Description', NS)) or None)
            header['ChargeClass'] = (text(pj.find('u:ChargeCode/u:Class/u:Code', NS)) or None, text(pj.find('u:ChargeCode/u:Class/u:Description', NS)) or None)
            # Currencies
            header['LocalCurrency'] = (text(pj.find('u:LocalCurrency/u:Code', NS)), text(pj.find('u:LocalCurrency/u:Description', NS)))
            header['OSCurrency'] = (text(pj.find('u:OSCurrency/u:Code', NS)), text(pj.find('u:OSCurrency/u:Description', NS)))
            header['ChargeCurrency'] = (text(pj.find('u:ChargeCurrency/u:Code', NS)), text(pj.find('u:ChargeCurrency/u:Description', NS)))
            # Numbers
            header['ChargeExchangeRate'] = text(pj.find('u:ChargeExchangeRate', NS)) or None
            header['ChargeTotalAmount'] = text(pj.find('u:ChargeTotalAmount', NS)) or None
            header['ChargeTotalExVATAmount'] = text(pj.find('u:ChargeTotalExVATAmount', NS)) or None
            header['LocalAmount'] = text(pj.find('u:LocalAmount', NS)) or None
            header['LocalGSTVATAmount'] = text(pj.find('u:LocalGSTVATAmount', NS)) or None
            header['LocalTotalAmount'] = text(pj.find('u:LocalTotalAmount', NS)) or None
            header['OSAmount'] = text(pj.find('u:OSAmount', NS)) or None
            header['OSGSTVATAmount'] = text(pj.find('u:OSGSTVATAmount', NS)) or None
            header['OSTotalAmount'] = text(pj.find('u:OSTotalAmount', NS)) or None
            # GL / dates
            header['GLAccount'] = (text(pj.find('u:GLAccount/u:AccountCode', NS)) or None, text(pj.find('u:GLAccount/u:Description', NS)) or None)
            header['GLPostDate'] = text(pj.find('u:GLPostDate', NS)) or None
            header['Job'] = (text(pj.find('u:Job/u:Type', NS)) or None, text(pj.find('u:Job/u:Key', NS)) or None)
            header['JobRecognitionDate'] = text(pj.find('u:JobRecognitionDate', NS)) or None
            header['IsFinalCharge'] = text(pj.find('u:IsFinalCharge', NS)) or None
            header['OrganizationKey'] = text(pj.find('u:Organization/u:Key', NS)) or None
            header['RevenueRecognitionType'] = text(pj.find('u:RevenueRecognitionType', NS)) or None
            header['Sequence'] = text(pj.find('u:Sequence', NS)) or None
            header['TaxDate'] = text(pj.find('u:TaxDate', NS)) or None
            header['TransactionCategory'] = text(pj.find('u:TransactionCategory', NS)) or None
            header['TransactionType'] = text(pj.find('u:TransactionType', NS)) or None
            header['Description'] = text(pj.find('u:Description', NS)) or None
            header['VATTaxID'] = (
                text(pj.find('u:VATTaxID/u:TaxCode', NS)) or None,
                text(pj.find('u:VATTaxID/u:Description', NS)) or None,
                text(pj.find('u:VATTaxID/u:TaxRate', NS)) or None,
                text(pj.find('u:VATTaxID/u:TaxType/u:Code', NS)) or None,
            )
            # Details
            details = []
            djc = pj.find('u:PostingJournalDetailCollection', NS)
            if djc is not None:
                for d in djc.findall('u:PostingJournalDetail', NS):
                    details.append({
                        'CreditGL': (text(d.find('u:CreditGLAccount/u:AccountCode', NS)) or None, text(d.find('u:CreditGLAccount/u:Description', NS)) or None),
                        'DebitGL': (text(d.find('u:DebitGLAccount/u:AccountCode', NS)) or None, text(d.find('u:DebitGLAccount/u:Description', NS)) or None),
                        'PostingAmount': text(d.find('u:PostingAmount', NS)) or None,
                        'PostingCurrency': (text(d.find('u:PostingCurrency/u:Code', NS)), text(d.find('u:PostingCurrency/u:Description', NS))),
                        'PostingDate': text(d.find('u:PostingDate', NS)) or None,
                        'PostingPeriod': text(d.find('u:PostingPeriod', NS)) or None,
                    })
            posting_journals.append({'Header': header, 'Details': details})
    fact["PostingJournals"] = posting_journals
    
    return dims, fact


def upsert_ar(cur: pyodbc.Cursor, dims: Dict, fact: Dict, counters: Optional[TableCounter] = None) -> None:
    # Country and Company
    (country_code, country_name) = dims["Country"]
    if country_code:
        ensure_country(cur, country_code, country_name)
    (company_code, company_name, comp_country_code) = dims["Company"]
    company_key = ensure_company(cur, company_code, company_name, comp_country_code)

    # Simple dims
    simple_keys = ensure_simple_dims(cur, dims)
    # Currency
    (cur_code, cur_desc) = dims["Currency"]
    currency_key = ensure_currency(cur, cur_code, cur_desc)
    # OS Currency (optional)
    os_currency = fact.get("OSCurrency") or ("", "")
    os_cur_code, os_cur_desc = os_currency
    os_currency_key = ensure_currency(cur, os_cur_code, os_cur_desc) if os_cur_code else None
    # Account Group
    (ag_code, ag_desc, ag_type) = dims["AccountGroup"]
    account_group_key = ensure_account_group(cur, ag_code, ag_desc, ag_type)

    # Branch (ensure from Branch + BranchAddress)
    branch_key = None
    b_code, b_name = dims.get("Branch", ("", ""))
    b_extra: Dict[str, object] = {}
    b_addr = dims.get("BranchAddress")
    if b_addr:
        # FKs first
        scr_code, scr_desc = b_addr.get("ScreeningStatus", ("", ""))
        scr_key = ensure_screening_status(cur, scr_code, scr_desc) if scr_code else None
        c_code, c_name = b_addr.get("Country", ("", ""))
        p_code, p_name = b_addr.get("Port", ("", ""))
        ckey = ensure_country(cur, c_code, c_name) if c_code else None
        pkey = ensure_port(cur, p_code, p_name) if p_code else None
        b_extra.update({
            "AddressType": b_addr.get("AddressType"),
            "Address1": b_addr.get("Address1"),
            "Address2": b_addr.get("Address2"),
            "AddressOverride": b_addr.get("AddressOverride"),
            "AddressShortCode": b_addr.get("AddressShortCode"),
            "City": b_addr.get("City"),
            "State": b_addr.get("State"),
            "Postcode": b_addr.get("Postcode"),
            "Email": b_addr.get("Email"),
            "Fax": b_addr.get("Fax"),
            "Phone": b_addr.get("Phone"),
        })
        if scr_key is not None:
            b_extra["ScreeningStatusKey"] = scr_key
        if ckey is not None:
            b_extra["CountryKey"] = ckey
        if pkey is not None:
            b_extra["PortKey"] = pkey
    branch_key = ensure_branch(cur, b_code, b_name, b_extra if b_extra else None)

    # Organizations (many); pick first as primary for fact, also enrich bridge
    organization_key = None
    all_orgs = dims.get("Organizations") or []
    upserted_orgs: list[Tuple[int, str]] = []  # (OrganizationKey, AddressType)
    for org in all_orgs:
        # Ensure FK dims first
        c_code, c_name = org.get("Country", ("", ""))
        p_code, p_name = org.get("Port", ("", ""))
        ckey = ensure_country(cur, c_code, c_name) if c_code else None
        pkey = ensure_port(cur, p_code, p_name) if p_code else None
        extra = {
            "AddressType": org.get("AddressType"),
            "Address1": org.get("Address1"),
            "Address2": org.get("Address2"),
            "AddressOverride": org.get("AddressOverride"),
            "AddressShortCode": org.get("AddressShortCode"),
            "City": org.get("City"),
            "State": org.get("State"),
            "Postcode": org.get("Postcode"),
            "Email": org.get("Email"),
            "Fax": org.get("Fax"),
            "Phone": org.get("Phone"),
        }
        if ckey is not None:
            extra["CountryKey"] = ckey
        if pkey is not None:
            extra["PortKey"] = pkey
        # Gov reg
        gov_reg = _clean_str(org.get("GovRegNum")) or None
        grt_code, grt_desc = org.get("GovRegNumType", ("", ""))
        if gov_reg or grt_code or grt_desc:
            extra["GovRegNum"] = gov_reg
            extra["GovRegNumTypeCode"] = grt_code or None
            extra["GovRegNumTypeDescription"] = grt_desc or None
        org_key = None
        code = _clean_str(org.get("OrganizationCode") or "")
        name = _clean_str(org.get("CompanyName") or code)
        try:
            org_key = _upsert_scalar_dim(
                cur,
                "Dwh2.DimOrganization",
                "OrganizationCode",
                code,
                "CompanyName",
                name,
                extra_cols=extra,
                key_col="OrganizationKey",
            )
        except Exception:
            # Dynamic fallback supporting optional columns in DimOrganization
            cols = ["[OrganizationCode]", "[CompanyName]"]
            vals: list[object] = [code, name]
            set_cols = ["[CompanyName]=?"]
            set_vals: list[object] = [name]
            for k, v in (extra or {}).items():
                cols.append(f"[{k}]")
                vals.append(v)
                set_cols.append(f"[{k}]=?")
                set_vals.append(v)
            insert_cols = ",".join(cols)
            insert_placeholders = ",".join(["?"] * len(vals))
            set_clause = ",".join(set_cols) + ", UpdatedAt=SYSUTCDATETIME()"
            cur.execute(
                f"IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimOrganization] WHERE [OrganizationCode]=?) "
                f"INSERT INTO [Dwh2].[DimOrganization] ({insert_cols}) VALUES ({insert_placeholders}); "
                f"ELSE UPDATE [Dwh2].[DimOrganization] SET {set_clause} WHERE [OrganizationCode]=?;",
                code, *vals, *set_vals, code
            )
            cur.execute("SELECT [OrganizationKey] FROM [Dwh2].[DimOrganization] WHERE [OrganizationCode]=?", code)
            r = cur.fetchone()
            org_key = int(r[0]) if r else None
        # Track primary org and collect for bridge
        if org_key is not None:
            if organization_key is None:
                organization_key = org_key
            upserted_orgs.append((org_key, org.get("AddressType") or ""))
            # Persist registration numbers for this org/address if any
            regs = org.get("RegistrationNumbers") or []
            for rn in regs:
                t_code = _clean_str(rn.get("TypeCode"))
                t_desc = _clean_str(rn.get("TypeDescription"))
                coi_code = _clean_str(rn.get("CountryOfIssueCode"))
                coi_name = _clean_str(rn.get("CountryOfIssueName"))
                val = _clean_str(rn.get("Value"))
                if not val:
                    continue
                addr_type = _clean_str(org.get("AddressType")) or None
                try:
                    # NULL-safe existence check for AddressType
                    cur.execute(
                        "IF NOT EXISTS (SELECT 1 FROM Dwh2.DimOrganizationRegistrationNumber WHERE OrganizationKey=? AND ((AddressType IS NULL AND ? IS NULL) OR AddressType=?) AND [Value]=?) "
                        "INSERT INTO Dwh2.DimOrganizationRegistrationNumber (OrganizationKey, AddressType, TypeCode, TypeDescription, CountryOfIssueCode, CountryOfIssueName, [Value]) VALUES (?,?,?,?,?,?,?);",
                        org_key, addr_type, addr_type, val, org_key, addr_type, t_code, t_desc, coi_code, coi_name, val
                    )
                except Exception:
                    # ignore unique conflicts or minor issues
                    pass

    # Job linkage
    job_type, job_key_text = fact.get("Job", (None, None))
    job_dim_key = ensure_job(cur, job_type, job_key_text) if job_type and job_key_text else None

    # Compose and upsert Fact AR by Number (business key)
    number = fact.get("Number")
    # Prepare column list and values
    cols = [
        ("CompanyKey", company_key),
        ("BranchKey", branch_key),
        ("DepartmentKey", simple_keys.get("DepartmentKey")),
        ("EventTypeKey", simple_keys.get("EventTypeKey")),
        ("ActionPurposeKey", simple_keys.get("ActionPurposeKey")),
        ("UserKey", simple_keys.get("UserKey")),
        ("EnterpriseKey", simple_keys.get("EnterpriseKey")),
        ("ServerKey", simple_keys.get("ServerKey")),
        ("DataProviderKey", simple_keys.get("DataProviderKey")),
        ("TransactionDateKey", fact.get("TransactionDateKey")),
        ("PostDateKey", fact.get("PostDateKey")),
        ("DueDateKey", fact.get("DueDateKey")),
        ("TriggerDateKey", fact.get("TriggerDateKey")),
        ("AccountGroupKey", account_group_key),
        ("LocalCurrencyKey", currency_key),
        ("OSCurrencyKey", os_currency_key),
        ("OrganizationKey", organization_key),
        ("JobDimKey", job_dim_key),
        # identifiers/context
        ("DataSourceType", (fact.get("DataSource") or (None, None))[0]),
        ("DataSourceKey", (fact.get("DataSource") or (None, None))[1]),
        ("Ledger", fact.get("Ledger")),
        ("Category", fact.get("Category")),
        ("InvoiceTerm", fact.get("InvoiceTerm")),
        ("InvoiceTermDays", fact.get("InvoiceTermDays")),
        ("JobInvoiceNumber", fact.get("JobInvoiceNumber")),
        ("CheckDrawer", fact.get("CheckDrawer")),
        ("CheckNumberOrPaymentRef", fact.get("CheckNumberOrPaymentRef")),
        ("DrawerBank", fact.get("DrawerBank")),
        ("DrawerBranch", fact.get("DrawerBranch")),
        ("ReceiptOrDirectDebitNumber", fact.get("ReceiptOrDirectDebitNumber")),
        ("RequisitionStatus", fact.get("RequisitionStatus")),
        ("TransactionReference", fact.get("TransactionReference")),
        ("TransactionType", fact.get("TransactionType")),
        ("AgreedPaymentMethod", fact.get("AgreedPaymentMethod")),
        ("ComplianceSubType", fact.get("ComplianceSubType")),
        ("CreateTime", fact.get("CreateTime")),
        ("CreateUser", fact.get("CreateUser")),
        ("EventReference", fact.get("EventReference")),
        ("Timestamp", fact.get("Timestamp")),
        ("TriggerCount", fact.get("TriggerCount")),
        ("TriggerDescription", fact.get("TriggerDescription")),
        ("TriggerType", fact.get("TriggerType")),
        ("NumberOfSupportingDocuments", fact.get("NumberOfSupportingDocuments")),
        ("LocalExVATAmount", fact.get("LocalExVATAmount")),
        ("LocalVATAmount", fact.get("LocalVATAmount")),
        ("LocalTaxTransactionsAmount", fact.get("LocalTaxTransactionsAmount")),
        ("LocalTotal", fact.get("LocalTotal")),
        ("OSExGSTVATAmount", fact.get("OSExGSTVATAmount")),
        ("OSGSTVATAmount", fact.get("OSGSTVATAmount")),
        ("OSTaxTransactionsAmount", fact.get("OSTaxTransactionsAmount")),
        ("OSTotal", fact.get("OSTotal")),
        ("OutstandingAmount", fact.get("OutstandingAmount")),
        ("ExchangeRate", fact.get("ExchangeRate")),
        ("IsCancelled", fact.get("IsCancelled")),
        ("IsCreatedByMatchingProcess", fact.get("IsCreatedByMatchingProcess")),
        ("IsPrinted", fact.get("IsPrinted")),
        ("PlaceOfIssueText", fact.get("PlaceOfIssueText")),
    ]
    # Update or insert
    cur.execute("SELECT FactAccountsReceivableTransactionKey FROM Dwh2.FactAccountsReceivableTransaction WHERE [Number] = ?", number)
    row = cur.fetchone()
    if row:
        fact_key = int(row[0]) if row else None
        set_clause = ", ".join([f"[{c}] = ?" for c, _ in cols] + ["UpdatedAt = SYSUTCDATETIME()"])
        params = [v for _, v in cols] + [number]
        cur.execute(f"UPDATE Dwh2.FactAccountsReceivableTransaction SET {set_clause} WHERE [Number] = ?", *params)
        if counters:
            counters.add('Dwh2.FactAccountsReceivableTransaction', updated=1)
    else:
        col_names = ["Number"] + [c for c, _ in cols]
        placeholders = ",".join(["?"] * len(col_names))
        params = [number] + [v for _, v in cols]
        cur.execute("INSERT INTO Dwh2.FactAccountsReceivableTransaction ([" + "],[".join(col_names) + "]) VALUES (" + placeholders + ")", *params)
        cur.execute("SELECT FactAccountsReceivableTransactionKey FROM Dwh2.FactAccountsReceivableTransaction WHERE [Number] = ?", number)
        r2 = cur.fetchone()
        fact_key = int(r2[0]) if r2 else None
        if counters:
            counters.add('Dwh2.FactAccountsReceivableTransaction', added=1)

    # Bridge rows for all organizations
    if upserted_orgs and fact_key:
        for org_key2, addr_type in upserted_orgs:
            try:
                cur.execute(
                    "IF NOT EXISTS (SELECT 1 FROM Dwh2.BridgeFactAROrganization WHERE FactAccountsReceivableTransactionKey=? AND OrganizationKey=? AND AddressType=?) "
                    "INSERT INTO Dwh2.BridgeFactAROrganization (FactAccountsReceivableTransactionKey, OrganizationKey, AddressType) VALUES (?,?,?);",
                    fact_key, org_key2, addr_type, fact_key, org_key2, addr_type
                )
            except Exception:
                # ignore unique conflicts on re-run
                pass

    # Message numbers (AR) with idempotent insert
    if fact_key and fact.get("MessageNumbers"):
        try:
            if ENABLE_BATCH:
                cur.fast_executemany = True
        except Exception:
            pass
        rows = []
        for mtype, mval in fact.get("MessageNumbers"):
            # Existence check params: (FactARKey, Value, Type)
            # Insert params follow
            rows.append((
                fact_key, mval, (mtype or None),
                None,            # FactShipmentKey
                fact_key,        # FactARKey
                'AR',            # Source
                mval, (mtype or None),
                company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
            ))
        if rows:
            cur.executemany(
                "IF NOT EXISTS (SELECT 1 FROM Dwh2.FactMessageNumber WHERE FactAccountsReceivableTransactionKey=? AND [Value]=? AND ISNULL([Type],'') = ISNULL(?,'')) "
                "INSERT INTO Dwh2.FactMessageNumber (FactShipmentKey, FactAccountsReceivableTransactionKey, Source, [Value], [Type], CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?)",
                rows
            )
            if counters:
                counters.add('Dwh2.FactMessageNumber', added=len(rows))

    # Recipient roles
    if fact_key and fact.get("RecipientRoles"):
        for code, desc in fact.get("RecipientRoles"):
            try:
                rr_key = _upsert_scalar_dim(cur, "Dwh2.DimRecipientRole", "Code", code or "", "Description", desc or (code or ""), key_col="RecipientRoleKey")
            except Exception:
                cur.execute(
                    "IF NOT EXISTS (SELECT 1 FROM Dwh2.DimRecipientRole WHERE [Code]=?) INSERT INTO Dwh2.DimRecipientRole ([Code],[Description]) VALUES (?,?); ELSE UPDATE Dwh2.DimRecipientRole SET [Description]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
                    code or "", code or "", desc or (code or ""), desc or (code or ""), code or ""
                )
                cur.execute("SELECT RecipientRoleKey FROM Dwh2.DimRecipientRole WHERE [Code]=?", code or "")
                r = cur.fetchone()
                rr_key = int(r[0]) if r else None
            if rr_key is not None:
                try:
                    cur.execute(
                        "IF NOT EXISTS (SELECT 1 FROM Dwh2.BridgeFactARRecipientRole WHERE FactAccountsReceivableTransactionKey=? AND RecipientRoleKey=?) "
                        "INSERT INTO Dwh2.BridgeFactARRecipientRole (FactAccountsReceivableTransactionKey, RecipientRoleKey) VALUES (?,?);",
                        fact_key, rr_key, fact_key, rr_key
                    )
                except Exception:
                    pass

    # Exceptions for AR
    if fact_key and fact.get("Exceptions"):
        try:
            if ENABLE_BATCH:
                cur.fast_executemany = True
        except Exception:
            pass
        rows = fact["Exceptions"]
        if rows:
            # Replace existing exceptions for this AR to ensure idempotency
            try:
                cur.execute(
                    "DELETE FROM Dwh2.FactException WHERE FactAccountsReceivableTransactionKey = ? AND Source = 'AR'",
                    fact_key,
                )
            except Exception:
                pass
            params = []
            for r in rows:
                params.append((
                    None,  # FactShipmentKey
                    fact_key,  # FactARKey
                    r.get('Source'),
                    r.get('ExceptionId'),
                    r.get('Code'), r.get('Type'), r.get('Severity'), r.get('Status'), r.get('Description'), r.get('IsResolved'),
                    r.get('Actioned'), r.get('ActionedDateKey'), r.get('ActionedTime'), r.get('Category'), r.get('EventDateKey'), r.get('EventTime'), r.get('DurationHours'), r.get('LocationCode'), r.get('LocationName'), r.get('Notes'), r.get('StaffCode'), r.get('StaffName'),
                    r.get('RaisedDateKey'), r.get('RaisedTime'), r.get('ResolvedDateKey'), r.get('ResolvedTime'),
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
            cur.executemany(
                "INSERT INTO Dwh2.FactException (FactShipmentKey, FactAccountsReceivableTransactionKey, Source, ExceptionId, Code, [Type], Severity, [Status], [Description], IsResolved, Actioned, ActionedDateKey, ActionedTime, Category, EventDateKey, EventTime, DurationHours, LocationCode, LocationName, Notes, StaffCode, StaffName, RaisedDateKey, RaisedTime, ResolvedDateKey, ResolvedTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params
            )
            if counters:
                counters.add('Dwh2.FactException', added=len(params))
    # DateCollection for AR
    if fact_key and fact.get("DateCollection"):
        try:
            if ENABLE_BATCH:
                cur.fast_executemany = True
        except Exception:
            pass
        rows = fact["DateCollection"]
        if rows:
            # Intra-level de-duplication only for AR header dates; do NOT exclude based on shipment/sub-shipment.
            seen_keys: set = set()
            rows = [
                r for r in rows
                if not (((r.get('DateTypeCode') or ''), r.get('DateKey'), r.get('Time')) in seen_keys)
                and not seen_keys.add(((r.get('DateTypeCode') or ''), r.get('DateKey'), r.get('Time')))
            ]
            # Idempotent behavior: replace existing DateCollection rows for this AR
            try:
                cur.execute(
                    "DELETE FROM Dwh2.FactEventDate WHERE FactAccountsReceivableTransactionKey = ? AND Source = 'AR'",
                    fact_key,
                )
            except Exception:
                pass
            params = []
            for r in rows:
                params.append((
                    None,  # FactShipmentKey
                    fact_key,
                    r.get('Source'), r.get('DateTypeCode'), r.get('DateTypeDescription'), r.get('DateKey'), r.get('Time'), r.get('DateTimeText'), r.get('IsEstimate'), r.get('Value'), r.get('TimeZone'),
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
            if params:
                cur.executemany(
                    "INSERT INTO Dwh2.FactEventDate (FactShipmentKey, FactAccountsReceivableTransactionKey, Source, DateTypeCode, DateTypeDescription, DateKey, [Time], DateTimeText, IsEstimate, [Value], TimeZone, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    params
                )
                if counters:
                    counters.add('Dwh2.FactEventDate', added=len(params))
    # Milestones for AR
    if fact_key and fact.get("Milestones"):
        try:
            if ENABLE_BATCH:
                cur.fast_executemany = True
        except Exception:
            pass
        rows = fact["Milestones"]
        if rows:
            # Idempotent behavior: replace existing Milestone rows for this AR
            try:
                cur.execute(
                    "DELETE FROM Dwh2.FactMilestone WHERE FactAccountsReceivableTransactionKey = ? AND Source = 'AR'",
                    fact_key,
                )
            except Exception:
                pass
            params = []
            for r in rows:
                params.append((
                    None,
                    fact_key,
                    r.get('Source'), r.get('EventCode'), r.get('Description'), r.get('Sequence'),
                    r.get('ActualDateKey'), r.get('ActualTime'), r.get('EstimatedDateKey'), r.get('EstimatedTime'), r.get('ConditionReference'), r.get('ConditionType'),
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
            cur.executemany(
                "INSERT INTO Dwh2.FactMilestone (FactShipmentKey, FactAccountsReceivableTransactionKey, Source, EventCode, [Description], [Sequence], ActualDateKey, ActualTime, EstimatedDateKey, EstimatedTime, ConditionReference, ConditionType, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params
            )
            if counters:
                counters.add('Dwh2.FactMilestone', added=len(params))

    # Notes for AR
    if fact_key and fact.get('Notes'):
        try:
            if ENABLE_BATCH:
                cur.fast_executemany = True
        except Exception:
            pass
        rows = fact['Notes']
        if rows:
            # Replace existing notes for this AR to ensure idempotency
            try:
                cur.execute(
                    "DELETE FROM Dwh2.FactNote WHERE FactAccountsReceivableTransactionKey = ? AND Source = 'AR'",
                    fact_key,
                )
            except Exception:
                pass
            params = []
            for r in rows:
                params.append((
                    None,  # FactShipmentKey
                    fact_key,
                    r.get('Source'),
                    r.get('Description'), r.get('IsCustomDescription'), r.get('NoteText'),
                    r.get('NoteContextCode'), r.get('NoteContextDescription'),
                    r.get('VisibilityCode'), r.get('VisibilityDescription'),
                    r.get('Content'),
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
            cur.executemany(
                "INSERT INTO Dwh2.FactNote (FactShipmentKey, FactAccountsReceivableTransactionKey, Source, [Description], IsCustomDescription, NoteText, NoteContextCode, NoteContextDescription, VisibilityCode, VisibilityDescription, Content, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params
            )
            if counters:
                counters.add('Dwh2.FactNote', added=len(params))

    # AR ChargeLineCollection -> FactChargeLine (parent = AR)
    if fact_key and fact.get('ChargeLines'):
        rows = []
        for cl in fact['ChargeLines']:
            br_code, br_name = cl['Branch']
            brk = ensure_branch(cur, br_code, br_name) if br_code else None
            dept_code, dept_name = cl['Department']
            dept_key = None
            if dept_code:
                try:
                    dept_key = _upsert_scalar_dim(cur, 'Dwh2.DimDepartment', 'Code', dept_code, 'Name', dept_name or dept_code, key_col='DepartmentKey')
                except Exception:
                    cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimDepartment WHERE Code=?) INSERT INTO Dwh2.DimDepartment (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimDepartment SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", dept_code, dept_code, dept_name or dept_code, dept_name or dept_code, dept_code)
                    cur.execute("SELECT DepartmentKey FROM Dwh2.DimDepartment WHERE Code=?", dept_code)
                    r = cur.fetchone(); dept_key = int(r[0]) if r else None
            chg_code, chg_desc = cl['ChargeCode']
            chg_group_code, chg_group_desc = cl['ChargeGroup']
            cred_key = ensure_organization_min(cur, cl.get('CreditorKey') or '') if cl.get('CreditorKey') else None
            debt_key = ensure_organization_min(cur, cl.get('DebtorKey') or '') if cl.get('DebtorKey') else None
            sell_pt = cl['Sell'].get('PostedTransaction') if cl.get('Sell') else None
            rows.append((
                None, None, fact_key,
                'AR',
                brk, dept_key,
                chg_code or None,
                chg_desc or None,
                chg_group_code or None,
                chg_group_desc or None,
                cl.get('Description'),
                int(cl.get('DisplaySequence') or '0') if str(cl.get('DisplaySequence') or '').isdigit() else 0,
                cred_key, debt_key,
                cl['Cost'].get('APInvoiceNumber') if cl.get('Cost') else None,
                _datekey_from_iso(cl['Cost'].get('DueDate')) if cl.get('Cost') else None, _time_from_iso(cl['Cost'].get('DueDate')) if cl.get('Cost') else None,
                (cl['Cost'].get('ExchangeRate') if cl.get('Cost') else None),
                _datekey_from_iso(cl['Cost'].get('InvoiceDate')) if cl.get('Cost') else None, _time_from_iso(cl['Cost'].get('InvoiceDate')) if cl.get('Cost') else None,
                1 if (str((cl['Cost'].get('IsPosted') if cl.get('Cost') else '') or '').lower() == 'true') else (0 if (str((cl['Cost'].get('IsPosted') if cl.get('Cost') else '') or '').lower() == 'false') else None),
                (cl['Cost'].get('LocalAmount') if cl.get('Cost') else None),
                (cl['Cost'].get('OSAmount') if cl.get('Cost') else None),
                ensure_currency(cur, *(cl['Cost'].get('OSCurrency') or ('',''))) if cl.get('Cost') and cl['Cost'].get('OSCurrency') and cl['Cost'].get('OSCurrency')[0] else None,
                (cl['Cost'].get('OSGSTVATAmount') if cl.get('Cost') else None),
                (cl['Sell'].get('ExchangeRate') if cl.get('Sell') else None),
                (cl['Sell'].get('GSTVAT') or (None,None))[0] if cl.get('Sell') else None,
                (cl['Sell'].get('GSTVAT') or (None,None))[1] if cl.get('Sell') else None,
                (cl['Sell'].get('InvoiceType') if cl.get('Sell') else None),
                1 if (str((cl['Sell'].get('IsPosted') if cl.get('Sell') else '') or '').lower() == 'true') else (0 if (str((cl['Sell'].get('IsPosted') if cl.get('Sell') else '') or '').lower() == 'false') else None),
                (cl['Sell'].get('LocalAmount') if cl.get('Sell') else None),
                (cl['Sell'].get('OSAmount') if cl.get('Sell') else None),
                ensure_currency(cur, *(cl['Sell'].get('OSCurrency') or ('',''))) if cl.get('Sell') and cl['Sell'].get('OSCurrency') and cl['Sell'].get('OSCurrency')[0] else None,
                (cl['Sell'].get('OSGSTVATAmount') if cl.get('Sell') else None),
                (sell_pt or (None,None,None,None,None,None))[0] if sell_pt else None,
                (sell_pt or (None,None,None,None,None,None))[1] if sell_pt else None,
                _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[2]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[2]) if sell_pt else None,
                _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[3]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[3]) if sell_pt else None,
                _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[4]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[4]) if sell_pt else None,
                (sell_pt or (None,None,None,None,None,None))[5] if sell_pt else None,
                cl.get('SupplierReference'),
                cl.get('SellReference'),
                company_key, simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
            ))
        if rows:
            try:
                if ENABLE_BATCH:
                    cur.fast_executemany = True
            except Exception:
                pass
            # For idempotency: check by (FactARKey, DisplaySequence, ChargeCode)
            insert_placeholders = ",".join(["?"] * len(rows[0]))
            check_placeholders = "?,?,?"  # fact_key, DisplaySequence, ChargeCode
            sql = (
                "IF NOT EXISTS (SELECT 1 FROM Dwh2.FactChargeLine WHERE FactAccountsReceivableTransactionKey=? "
                "AND [DisplaySequence]=? AND ISNULL([ChargeCode],'') = ISNULL(?,'')) "
                "INSERT INTO Dwh2.FactChargeLine ("
                "FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, BranchKey, DepartmentKey, "
                "ChargeCode, ChargeCodeDescription, ChargeCodeGroup, ChargeCodeGroupDescription, [Description], DisplaySequence, "
                "CreditorOrganizationKey, DebtorOrganizationKey, CostAPInvoiceNumber, CostDueDateKey, CostDueTime, CostExchangeRate, CostInvoiceDateKey, CostInvoiceTime, "
                "CostIsPosted, CostLocalAmount, CostOSAmount, CostOSCurrencyKey, CostOSGSTVATAmount, "
                "SellExchangeRate, SellGSTVATTaxCode, SellGSTVATDescription, SellInvoiceType, SellIsPosted, SellLocalAmount, SellOSAmount, SellOSCurrencyKey, SellOSGSTVATAmount, "
                "SellPostedTransactionNumber, SellPostedTransactionType, SellTransactionDateKey, SellTransactionTime, SellDueDateKey, SellDueTime, SellFullyPaidDateKey, SellFullyPaidTime, "
                "SellOutstandingAmount, SupplierReference, SellReference, CompanyKey, EventUserKey, DataProviderKey) VALUES (" + insert_placeholders + ")"
            )
            # Map rows to include the check arguments upfront
            rows2 = []
            for r in rows:
                # r structure: (..., [Description], DisplaySequence, ..., ChargeCode ...)
                # We know positions: DisplaySequence is at index 11, ChargeCode is at index 6
                display_seq = r[11]
                charge_code = r[6]
                rows2.append((fact_key, display_seq, charge_code) + r)
            cur.executemany(sql, rows2)
            if counters:
                counters.add('Dwh2.FactChargeLine', added=len(rows))

    # Fallback: ChargeLineCollection under AR Shipment->SubShipments -> FactChargeLine (parent = AR)
    # Some AR files carry ChargeLines only under SubShipment JobCosting. We map them to AR to avoid loss.
    if fact_key and fact.get('ShipmentFromAR'):
        shf = fact['ShipmentFromAR']  # type: ignore[assignment]
        sub_list = shf.get('SubShipments') if isinstance(shf, dict) else None
        rows = []
        if sub_list:
            for sub_obj in sub_list:  # type: ignore[assignment]
                for cl in (sub_obj.get('ChargeLines') or []):  # type: ignore[index]
                    br_code, br_name = cl['Branch']
                    brk = ensure_branch(cur, br_code, br_name) if br_code else None
                    dept_code, dept_name = cl['Department']
                    dept_key = None
                    if dept_code:
                        try:
                            dept_key = _upsert_scalar_dim(cur, 'Dwh2.DimDepartment', 'Code', dept_code, 'Name', dept_name or dept_code, key_col='DepartmentKey')
                        except Exception:
                            cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimDepartment WHERE Code=?) INSERT INTO Dwh2.DimDepartment (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimDepartment SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", dept_code, dept_code, dept_name or dept_code, dept_name or dept_code, dept_code)
                            cur.execute("SELECT DepartmentKey FROM Dwh2.DimDepartment WHERE Code=?", dept_code)
                            r = cur.fetchone(); dept_key = int(r[0]) if r else None
                    chg_code, chg_desc = cl['ChargeCode']
                    chg_group_code, chg_group_desc = cl['ChargeGroup']
                    cred_key = ensure_organization_min(cur, cl.get('CreditorKey') or '') if cl.get('CreditorKey') else None
                    debt_key = ensure_organization_min(cur, cl.get('DebtorKey') or '') if cl.get('DebtorKey') else None
                    sell_pt = cl['Sell'].get('PostedTransaction') if cl.get('Sell') else None
                    rows.append((
                        None, None, fact_key,
                        'AR',
                        brk, dept_key,
                        chg_code or None,
                        chg_desc or None,
                        chg_group_code or None,
                        chg_group_desc or None,
                        cl.get('Description'),
                        int(cl.get('DisplaySequence') or '0') if str(cl.get('DisplaySequence') or '').isdigit() else 0,
                        cred_key, debt_key,
                        cl['Cost'].get('APInvoiceNumber') if cl.get('Cost') else None,
                        _datekey_from_iso(cl['Cost'].get('DueDate')) if cl.get('Cost') else None, _time_from_iso(cl['Cost'].get('DueDate')) if cl.get('Cost') else None,
                        (cl['Cost'].get('ExchangeRate') if cl.get('Cost') else None),
                        _datekey_from_iso(cl['Cost'].get('InvoiceDate')) if cl.get('Cost') else None, _time_from_iso(cl['Cost'].get('InvoiceDate')) if cl.get('Cost') else None,
                        1 if (str((cl['Cost'].get('IsPosted') if cl.get('Cost') else '') or '').lower() == 'true') else (0 if (str((cl['Cost'].get('IsPosted') if cl.get('Cost') else '') or '').lower() == 'false') else None),
                        (cl['Cost'].get('LocalAmount') if cl.get('Cost') else None),
                        (cl['Cost'].get('OSAmount') if cl.get('Cost') else None),
                        ensure_currency(cur, *(cl['Cost'].get('OSCurrency') or ('',''))) if cl.get('Cost') and cl['Cost'].get('OSCurrency') and cl['Cost'].get('OSCurrency')[0] else None,
                        (cl['Cost'].get('OSGSTVATAmount') if cl.get('Cost') else None),
                        (cl['Sell'].get('ExchangeRate') if cl.get('Sell') else None),
                        (cl['Sell'].get('GSTVAT') or (None,None))[0] if cl.get('Sell') else None,
                        (cl['Sell'].get('GSTVAT') or (None,None))[1] if cl.get('Sell') else None,
                        (cl['Sell'].get('InvoiceType') if cl.get('Sell') else None),
                        1 if (str((cl['Sell'].get('IsPosted') if cl.get('Sell') else '') or '').lower() == 'true') else (0 if (str((cl['Sell'].get('IsPosted') if cl.get('Sell') else '') or '').lower() == 'false') else None),
                        (cl['Sell'].get('LocalAmount') if cl.get('Sell') else None),
                        (cl['Sell'].get('OSAmount') if cl.get('Sell') else None),
                        ensure_currency(cur, *(cl['Sell'].get('OSCurrency') or ('',''))) if cl.get('Sell') and cl['Sell'].get('OSCurrency') and cl['Sell'].get('OSCurrency')[0] else None,
                        (cl['Sell'].get('OSGSTVATAmount') if cl.get('Sell') else None),
                        (sell_pt or (None,None,None,None,None,None))[0] if sell_pt else None,
                        (sell_pt or (None,None,None,None,None,None))[1] if sell_pt else None,
                        _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[2]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[2]) if sell_pt else None,
                        _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[3]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[3]) if sell_pt else None,
                        _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[4]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[4]) if sell_pt else None,
                        (sell_pt or (None,None,None,None,None,None))[5] if sell_pt else None,
                        cl.get('SupplierReference'),
                        cl.get('SellReference'),
                        company_key, simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                    ))
        if rows:
            try:
                if ENABLE_BATCH:
                    cur.fast_executemany = True
            except Exception:
                pass
            insert_placeholders = ",".join(["?"] * len(rows[0]))
            sql = (
                "IF NOT EXISTS (SELECT 1 FROM Dwh2.FactChargeLine WHERE FactAccountsReceivableTransactionKey=? "
                "AND [DisplaySequence]=? AND ISNULL([ChargeCode],'') = ISNULL(?,'')) "
                "INSERT INTO Dwh2.FactChargeLine ("
                "FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, BranchKey, DepartmentKey, "
                "ChargeCode, ChargeCodeDescription, ChargeCodeGroup, ChargeCodeGroupDescription, [Description], DisplaySequence, "
                "CreditorOrganizationKey, DebtorOrganizationKey, CostAPInvoiceNumber, CostDueDateKey, CostDueTime, CostExchangeRate, CostInvoiceDateKey, CostInvoiceTime, "
                "CostIsPosted, CostLocalAmount, CostOSAmount, CostOSCurrencyKey, CostOSGSTVATAmount, "
                "SellExchangeRate, SellGSTVATTaxCode, SellGSTVATDescription, SellInvoiceType, SellIsPosted, SellLocalAmount, SellOSAmount, SellOSCurrencyKey, SellOSGSTVATAmount, "
                "SellPostedTransactionNumber, SellPostedTransactionType, SellTransactionDateKey, SellTransactionTime, SellDueDateKey, SellDueTime, SellFullyPaidDateKey, SellFullyPaidTime, "
                "SellOutstandingAmount, SupplierReference, SellReference, CompanyKey, EventUserKey, DataProviderKey) VALUES (" + insert_placeholders + ")"
            )
            rows2 = []
            for r in rows:
                display_seq = r[11]
                charge_code = r[6]
                rows2.append((fact_key, display_seq, charge_code) + r)
            cur.executemany(sql, rows2)
            if counters:
                counters.add('Dwh2.FactChargeLine', added=len(rows))

    # AR TransportLegs (top-level only) -> FactTransportLeg (parent = AR)
    if fact_key:
        shf = fact.get('ShipmentFromAR') if isinstance(fact, dict) else None

        def _norm(s: Optional[str]) -> str:
            return (s or '').strip()

        # Build a set of leg keys from SubShipment legs to avoid duplicating them at AR level
        def _leg_key(leg: dict) -> tuple:
            pol = ((_norm((leg.get('PortOfLoading') or ('', ''))[0])).upper())
            pod = ((_norm((leg.get('PortOfDischarge') or ('', ''))[0])).upper())
            mode = _norm(leg.get('TransportMode')).lower()
            vessel = _norm(leg.get('VesselLloydsIMO')) or _norm(leg.get('VesselName')).upper()
            voyage = _norm(leg.get('VoyageFlightNo')).upper()
            order_ = leg.get('Order') or 0
            times = (
                _norm(leg.get('ActualArrival')), _norm(leg.get('ActualDeparture')),
                _norm(leg.get('EstimatedArrival')), _norm(leg.get('EstimatedDeparture')),
                _norm(leg.get('ScheduledArrival')), _norm(leg.get('ScheduledDeparture')),
            )
            return (order_, pol, pod, mode, vessel, voyage) + times

        sub_leg_keys: set[tuple] = set()
        if isinstance(shf, dict):
            for sub in (shf.get('SubShipments') or []):
                if isinstance(sub, dict):
                    for leg in (sub.get('TransportLegs') or []):
                        try:
                            sub_leg_keys.add(_leg_key(leg))
                        except Exception:
                            continue

        # Only AR top-level legs (not under Shipment/SubShipment)
        combined_sources: list[dict] = []
        combined_sources.extend(fact.get('TransportLegs') or [])

        seen: set[tuple] = set()
        ar_parent_legs: list[dict] = []
        for leg in combined_sources:
            try:
                k = _leg_key(leg)
            except Exception:
                continue
            if k in seen:
                continue
            seen.add(k)
            ar_parent_legs.append(leg)

        # Idempotency: clear any previous AR-parent legs for this AR before inserting the filtered set
        try:
            cur.execute("DELETE FROM Dwh2.FactTransportLeg WHERE FactAccountsReceivableTransactionKey = ?", fact_key)
        except Exception:
            pass

        if ar_parent_legs:
            rows = []
            for leg in ar_parent_legs:
                pol_code, pol_name = leg['PortOfLoading']
                pod_code, pod_name = leg['PortOfDischarge']
                polk = ensure_port(cur, pol_code, pol_name) if pol_code else None
                podk = ensure_port(cur, pod_code, pod_name) if pod_code else None
                bsc, bsd = leg.get('BookingStatus') if leg.get('BookingStatus') else (None, None)
                carr_code, carr_name, carr_country_code, carr_port_code = leg.get('Carrier') if leg.get('Carrier') else (None, None, None, None)
                cred_code, cred_name, cred_country_code, cred_port_code = leg.get('Creditor') if leg.get('Creditor') else (None, None, None, None)
                carr_key = ensure_organization_min(cur, carr_code, carr_name, carr_country_code, carr_port_code) if carr_code else None
                cred_key = ensure_organization_min(cur, cred_code, cred_name, cred_country_code, cred_port_code) if cred_code else None
                rows.append((
                    None, None, fact_key,
                    polk, podk, leg.get('Order'), leg.get('TransportMode'),
                    leg.get('VesselName'), leg.get('VesselLloydsIMO'), leg.get('VoyageFlightNo'),
                    leg.get('CarrierBookingReference'), bsc, bsd,
                    carr_key, cred_key,
                    _datekey_from_iso(leg.get('ActualArrival')), _time_from_iso(leg.get('ActualArrival')),
                    _datekey_from_iso(leg.get('ActualDeparture')), _time_from_iso(leg.get('ActualDeparture')),
                    _datekey_from_iso(leg.get('EstimatedArrival')), _time_from_iso(leg.get('EstimatedArrival')),
                    _datekey_from_iso(leg.get('EstimatedDeparture')), _time_from_iso(leg.get('EstimatedDeparture')),
                    _datekey_from_iso(leg.get('ScheduledArrival')), _time_from_iso(leg.get('ScheduledArrival')),
                    _datekey_from_iso(leg.get('ScheduledDeparture')), _time_from_iso(leg.get('ScheduledDeparture')),
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
            if rows:
                try:
                    if ENABLE_BATCH:
                        cur.fast_executemany = True
                except Exception:
                    pass
                cur.executemany(
                    "INSERT INTO Dwh2.FactTransportLeg (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, PortOfLoadingKey, PortOfDischargeKey, [Order], TransportMode, VesselName, VesselLloydsIMO, VoyageFlightNo, CarrierBookingReference, BookingStatusCode, BookingStatusDescription, CarrierOrganizationKey, CreditorOrganizationKey, ActualArrivalDateKey, ActualArrivalTime, ActualDepartureDateKey, ActualDepartureTime, EstimatedArrivalDateKey, EstimatedArrivalTime, EstimatedDepartureDateKey, EstimatedDepartureTime, ScheduledArrivalDateKey, ScheduledArrivalTime, ScheduledDepartureDateKey, ScheduledDepartureTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    rows
                )
                if counters:
                    counters.add('Dwh2.FactTransportLeg', added=len(rows))

    # Fallback: aggregate SubShipment JobCosting headers up to AR when AR-level header is missing
    if fact_key and fact.get('ShipmentFromAR') and not fact.get('JobCostingHeader'):
        shf = fact['ShipmentFromAR']  # type: ignore[assignment]
        subs = shf.get('SubShipments') if isinstance(shf, dict) else None
        headers = []
        if subs:
            for sub in subs:  # type: ignore[assignment]
                h2 = sub.get('JobCostingHeader') if isinstance(sub, dict) else None
                if h2:
                    headers.append(h2)
        if headers:
            def sum_field(name: str) -> Optional[float]:
                s = 0.0; has_any = False
                for hh in headers:
                    v = hh.get(name) if isinstance(hh, dict) else None
                    try:
                        if v is not None and v != '':
                            s += float(v)
                            has_any = True
                    except Exception:
                        continue
                return s if has_any else None
            cols = [
                ('FactShipmentKey', None),
                ('FactSubShipmentKey', None),
                ('FactAccountsReceivableTransactionKey', fact_key),
                ('Source', 'AR'),
                ('BranchKey', None), ('DepartmentKey', None), ('HomeBranchKey', None), ('OperationsStaffKey', None), ('CurrencyKey', None),
                ('ClientContractNumber', None),
                ('AccrualNotRecognized', sum_field('AccrualNotRecognized')),
                ('AccrualRecognized', sum_field('AccrualRecognized')),
                ('AgentRevenue', sum_field('AgentRevenue')),
                ('LocalClientRevenue', sum_field('LocalClientRevenue')),
                ('OtherDebtorRevenue', sum_field('OtherDebtorRevenue')),
                ('TotalAccrual', sum_field('TotalAccrual')),
                ('TotalCost', sum_field('TotalCost')),
                ('TotalJobProfit', sum_field('TotalJobProfit')),
                ('TotalRevenue', sum_field('TotalRevenue')),
                ('TotalWIP', sum_field('TotalWIP')),
                ('WIPNotRecognized', sum_field('WIPNotRecognized')),
                ('WIPRecognized', sum_field('WIPRecognized')),
                ('CompanyKey', company_key), ('EventUserKey', simple_keys.get('UserKey')), ('DataProviderKey', simple_keys.get('DataProviderKey')),
            ]
            col_names = [c for c,_ in cols]
            placeholders = ','.join(['?']*len(col_names))
            params = [v for _,v in cols]
            cur.execute(
                "IF EXISTS (SELECT 1 FROM Dwh2.FactJobCosting WHERE FactAccountsReceivableTransactionKey=?) "
                "UPDATE Dwh2.FactJobCosting SET [Source]=?, [BranchKey]=?, [DepartmentKey]=?, [HomeBranchKey]=?, [OperationsStaffKey]=?, [CurrencyKey]=?, [ClientContractNumber]=?, [AccrualNotRecognized]=?, [AccrualRecognized]=?, [AgentRevenue]=?, [LocalClientRevenue]=?, [OtherDebtorRevenue]=?, [TotalAccrual]=?, [TotalCost]=?, [TotalJobProfit]=?, [TotalRevenue]=?, [TotalWIP]=?, [WIPNotRecognized]=?, [WIPRecognized]=?, [CompanyKey]=?, [EventUserKey]=?, [DataProviderKey]=?, UpdatedAt=SYSUTCDATETIME() WHERE FactAccountsReceivableTransactionKey=? "
                "ELSE INSERT INTO Dwh2.FactJobCosting (" + ','.join('['+c+']' for c in col_names) + ") VALUES (" + placeholders + ")",
                fact_key,
                'AR', None, None, None, None, None, None,
                cols[11][1], cols[12][1], cols[13][1], cols[14][1], cols[15][1], cols[16][1], cols[17][1], cols[18][1], cols[19][1], cols[20][1], cols[21][1], cols[22][1],
                company_key, simple_keys.get('UserKey'), simple_keys.get('DataProviderKey'),
                fact_key,
                *params
            )
            try:
                cur.execute("SELECT 1 FROM Dwh2.FactJobCosting WHERE FactAccountsReceivableTransactionKey=?", fact_key)
                if cur.fetchone():
                    if counters:
                        counters.add('Dwh2.FactJobCosting', updated=1)
                else:
                    if counters:
                        counters.add('Dwh2.FactJobCosting', added=1)
            except Exception:
                pass

    # AdditionalReferenceCollection for AR -> FactAdditionalReference
    if fact_key and fact.get('AdditionalReferences'):
        rows = []
        for r in fact['AdditionalReferences']:
            coi_code, coi_name = r.get('CountryOfIssue') or (None, None)
            coi_key = ensure_country(cur, coi_code, coi_name) if coi_code else None
            rows.append((
                None, None, fact_key,
                'AR',
                r.get('TypeCode'), r.get('TypeDescription'), r.get('ReferenceNumber'), r.get('ContextInformation'),
                coi_key,
                _datekey_from_iso(r.get('IssueDate')), _time_from_iso(r.get('IssueDate')),
                company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
            ))
        if rows:
            try:
                if ENABLE_BATCH:
                    cur.fast_executemany = True
            except Exception:
                pass
            cur.executemany(
                "INSERT INTO Dwh2.FactAdditionalReference (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, TypeCode, TypeDescription, ReferenceNumber, ContextInformation, CountryOfIssueKey, IssueDateKey, IssueTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows
            )
            if counters:
                counters.add('Dwh2.FactAdditionalReference', added=len(rows))

    # AR-level PackingLineCollection -> attach to AR parent when available
    if fact_key and fact.get('ShipmentFromAR'):
        shf = fact['ShipmentFromAR']  # type: ignore[assignment]
        # First, attach ContainerCollection from AR to AR parent (both shipment and sub-shipment level from AR)
        try:
            cur.execute("DELETE FROM Dwh2.FactContainer WHERE FactAccountsReceivableTransactionKey = ? AND Source = 'AR'", fact_key)
        except Exception:
            pass
        cont_rows = []
        # Shipment-level containers from AR embedded Shipment
        conts_sh = shf.get('ContainersShipment') if isinstance(shf, dict) else None
        if conts_sh:
            for ct in conts_sh:  # type: ignore[assignment]
                len_code, len_desc = ct.get('LengthUnit') or (None, None)
                vol_code, vol_desc = ct.get('VolumeUnit') or (None, None)
                wei_code, wei_desc = ct.get('WeightUnit') or (None, None)
                lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                ct_code, ct_desc, ct_iso, cat_code, cat_desc = ct.get('ContainerType') or (None, None, None, None, None)
                fclc, fcld = ct.get('FCL_LCL_AIR') or (None, None)
                cont_rows.append((
                    None, None, fact_key,
                    'AR',
                    ct.get('ContainerJobID'), ct.get('ContainerNumber'), ct.get('Link'),
                    ct_code, ct_desc, ct_iso, cat_code, cat_desc,
                    ct.get('DeliveryMode'), fclc, fcld, ct.get('ContainerCount'),
                    ct.get('Seal'), ct.get('SealPartyTypeCode'), ct.get('SecondSeal'), ct.get('SecondSealPartyTypeCode'), ct.get('ThirdSeal'), ct.get('ThirdSealPartyTypeCode'), ct.get('StowagePosition'),
                    lenk, volk, weik,
                    ct.get('TotalHeight'), ct.get('TotalLength'), ct.get('TotalWidth'), ct.get('TareWeight'), ct.get('GrossWeight'), ct.get('GoodsWeight'), ct.get('VolumeCapacity'), ct.get('WeightCapacity'), ct.get('DunnageWeight'), ct.get('OverhangBack'), ct.get('OverhangFront'), ct.get('OverhangHeight'), ct.get('OverhangLeft'), ct.get('OverhangRight'), ct.get('HumidityPercent'), ct.get('AirVentFlow'), ct.get('AirVentFlowRateUnitCode'),
                    (1 if ct.get('NonOperatingReefer') is True else (0 if ct.get('NonOperatingReefer') is False else None)),
                    (1 if ct.get('IsCFSRegistered') is True else (0 if ct.get('IsCFSRegistered') is False else None)),
                    (1 if ct.get('IsControlledAtmosphere') is True else (0 if ct.get('IsControlledAtmosphere') is False else None)),
                    (1 if ct.get('IsDamaged') is True else (0 if ct.get('IsDamaged') is False else None)),
                    (1 if ct.get('IsEmptyContainer') is True else (0 if ct.get('IsEmptyContainer') is False else None)),
                    (1 if ct.get('IsSealOk') is True else (0 if ct.get('IsSealOk') is False else None)),
                    (1 if ct.get('IsShipperOwned') is True else (0 if ct.get('IsShipperOwned') is False else None)),
                    (1 if ct.get('ArrivalPickupByRail') is True else (0 if ct.get('ArrivalPickupByRail') is False else None)),
                    (1 if ct.get('DepartureDeliveryByRail') is True else (0 if ct.get('DepartureDeliveryByRail') is False else None)),
                    _datekey_from_iso(ct.get('ArrivalSlotDateTime')), _time_from_iso(ct.get('ArrivalSlotDateTime')),
                    _datekey_from_iso(ct.get('DepartureSlotDateTime')), _time_from_iso(ct.get('DepartureSlotDateTime')),
                    _datekey_from_iso(ct.get('EmptyReadyForReturn')), _time_from_iso(ct.get('EmptyReadyForReturn')),
                    _datekey_from_iso(ct.get('FCLWharfGateIn')), _time_from_iso(ct.get('FCLWharfGateIn')),
                    _datekey_from_iso(ct.get('FCLWharfGateOut')), _time_from_iso(ct.get('FCLWharfGateOut')),
                    _datekey_from_iso(ct.get('FCLStorageCommences')), _time_from_iso(ct.get('FCLStorageCommences')),
                    _datekey_from_iso(ct.get('LCLUnpack')), _time_from_iso(ct.get('LCLUnpack')),
                    _datekey_from_iso(ct.get('PackDate')), _time_from_iso(ct.get('PackDate')),
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
        # Sub-shipment-level containers from AR embedded Shipment
        sub_list = shf.get('SubShipments') if isinstance(shf, dict) else None
        if sub_list:
            for sub_obj in sub_list:  # type: ignore[assignment]
                conts2 = sub_obj.get('Containers') or []  # type: ignore[index]
                for ct in conts2:
                    len_code, len_desc = ct.get('LengthUnit') or (None, None)
                    vol_code, vol_desc = ct.get('VolumeUnit') or (None, None)
                    wei_code, wei_desc = ct.get('WeightUnit') or (None, None)
                    lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                    volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                    weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                    ct_code, ct_desc, ct_iso, cat_code, cat_desc = ct.get('ContainerType') or (None, None, None, None, None)
                    fclc, fcld = ct.get('FCL_LCL_AIR') or (None, None)
                    cont_rows.append((
                        None, None, fact_key,
                        'AR',
                        ct.get('ContainerJobID'), ct.get('ContainerNumber'), ct.get('Link'),
                        ct_code, ct_desc, ct_iso, cat_code, cat_desc,
                        ct.get('DeliveryMode'), fclc, fcld, ct.get('ContainerCount'),
                        ct.get('Seal'), ct.get('SealPartyTypeCode'), ct.get('SecondSeal'), ct.get('SecondSealPartyTypeCode'), ct.get('ThirdSeal'), ct.get('ThirdSealPartyTypeCode'), ct.get('StowagePosition'),
                        lenk, volk, weik,
                        ct.get('TotalHeight'), ct.get('TotalLength'), ct.get('TotalWidth'), ct.get('TareWeight'), ct.get('GrossWeight'), ct.get('GoodsWeight'), ct.get('VolumeCapacity'), ct.get('WeightCapacity'), ct.get('DunnageWeight'), ct.get('OverhangBack'), ct.get('OverhangFront'), ct.get('OverhangHeight'), ct.get('OverhangLeft'), ct.get('OverhangRight'), ct.get('HumidityPercent'), ct.get('AirVentFlow'), ct.get('AirVentFlowRateUnitCode'),
                        (1 if ct.get('NonOperatingReefer') is True else (0 if ct.get('NonOperatingReefer') is False else None)),
                        (1 if ct.get('IsCFSRegistered') is True else (0 if ct.get('IsCFSRegistered') is False else None)),
                        (1 if ct.get('IsControlledAtmosphere') is True else (0 if ct.get('IsControlledAtmosphere') is False else None)),
                        (1 if ct.get('IsDamaged') is True else (0 if ct.get('IsDamaged') is False else None)),
                        (1 if ct.get('IsEmptyContainer') is True else (0 if ct.get('IsEmptyContainer') is False else None)),
                        (1 if ct.get('IsSealOk') is True else (0 if ct.get('IsSealOk') is False else None)),
                        (1 if ct.get('IsShipperOwned') is True else (0 if ct.get('IsShipperOwned') is False else None)),
                        (1 if ct.get('ArrivalPickupByRail') is True else (0 if ct.get('ArrivalPickupByRail') is False else None)),
                        (1 if ct.get('DepartureDeliveryByRail') is True else (0 if ct.get('DepartureDeliveryByRail') is False else None)),
                        _datekey_from_iso(ct.get('ArrivalSlotDateTime')), _time_from_iso(ct.get('ArrivalSlotDateTime')),
                        _datekey_from_iso(ct.get('DepartureSlotDateTime')), _time_from_iso(ct.get('DepartureSlotDateTime')),
                        _datekey_from_iso(ct.get('EmptyReadyForReturn')), _time_from_iso(ct.get('EmptyReadyForReturn')),
                        _datekey_from_iso(ct.get('FCLWharfGateIn')), _time_from_iso(ct.get('FCLWharfGateIn')),
                        _datekey_from_iso(ct.get('FCLWharfGateOut')), _time_from_iso(ct.get('FCLWharfGateOut')),
                        _datekey_from_iso(ct.get('FCLStorageCommences')), _time_from_iso(ct.get('FCLStorageCommences')),
                        _datekey_from_iso(ct.get('LCLUnpack')), _time_from_iso(ct.get('LCLUnpack')),
                        _datekey_from_iso(ct.get('PackDate')), _time_from_iso(ct.get('PackDate')),
                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                    ))
        # Deduplicate containers across shipment and sub-shipment collections by business key
        if cont_rows:
            seen_keys = set()
            unique_rows = []
            for r in cont_rows:
                # r positions: (FactShipmentKey, FactSubShipmentKey, FactARKey, Source, ContainerJobID, ContainerNumber, Link, ...)
                biz_key = (r[4], r[5], r[6])
                if biz_key in seen_keys:
                    continue
                seen_keys.add(biz_key)
                unique_rows.append(r)
            cont_rows = unique_rows
        if cont_rows:
            try:
                if ENABLE_BATCH:
                    cur.fast_executemany = True
            except Exception:
                pass
            placeholders = ",".join(["?"] * len(cont_rows[0]))
            cur.executemany(
                "INSERT INTO Dwh2.FactContainer (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, ContainerJobID, ContainerNumber, [Link], ContainerTypeCode, ContainerTypeDescription, ContainerTypeISOCode, ContainerCategoryCode, ContainerCategoryDescription, DeliveryMode, FCL_LCL_AIR_Code, FCL_LCL_AIR_Description, ContainerCount, Seal, SealPartyTypeCode, SecondSeal, SecondSealPartyTypeCode, ThirdSeal, ThirdSealPartyTypeCode, StowagePosition, LengthUnitKey, VolumeUnitKey, WeightUnitKey, TotalHeight, TotalLength, TotalWidth, TareWeight, GrossWeight, GoodsWeight, VolumeCapacity, WeightCapacity, DunnageWeight, OverhangBack, OverhangFront, OverhangHeight, OverhangLeft, OverhangRight, HumidityPercent, AirVentFlow, AirVentFlowRateUnitCode, NonOperatingReefer, IsCFSRegistered, IsControlledAtmosphere, IsDamaged, IsEmptyContainer, IsSealOk, IsShipperOwned, ArrivalPickupByRail, DepartureDeliveryByRail, ArrivalSlotDateKey, ArrivalSlotTime, DepartureSlotDateKey, DepartureSlotTime, EmptyReadyForReturnDateKey, EmptyReadyForReturnTime, FCLWharfGateInDateKey, FCLWharfGateInTime, FCLWharfGateOutDateKey, FCLWharfGateOutTime, FCLStorageCommencesDateKey, FCLStorageCommencesTime, LCLUnpackDateKey, LCLUnpackTime, PackDateKey, PackTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (" + placeholders + ")",
                cont_rows
            )
            if counters:
                counters.add('Dwh2.FactContainer', added=len(cont_rows))
        # Delete existing AR-parent packing lines to ensure idempotency
        try:
            cur.execute("DELETE FROM Dwh2.FactPackingLine WHERE FactAccountsReceivableTransactionKey = ? AND Source = 'AR'", fact_key)
        except Exception:
            pass
        rows = []
        # Shipment-level lines from AR
        packs_sh = shf.get('PackingLinesShipment') if isinstance(shf, dict) else None
        if packs_sh:
            for pl in packs_sh:  # type: ignore[assignment]
                len_code, len_desc = pl.get('LengthUnit') or (None, None)
                vol_code, vol_desc = pl.get('VolumeUnit') or (None, None)
                wei_code, wei_desc = pl.get('WeightUnit') or (None, None)
                pack_code, pack_desc = pl.get('PackType') or (None, None)
                lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                packk = ensure_unit(cur, pack_code, pack_desc, 'Packs') if pack_code else None
                rows.append((
                    None, None, fact_key,
                    'AR',
                    pl.get('CommodityCode'), pl.get('CommodityDescription'), pl.get('ContainerLink'), pl.get('ContainerNumber'), pl.get('ContainerPackingOrder'), pl.get('CountryOfOriginCode'), pl.get('DetailedDescription'), pl.get('EndItemNo'), pl.get('ExportReferenceNumber'), pl.get('GoodsDescription'), pl.get('HarmonisedCode'),
                    pl.get('Height'), pl.get('Length'), pl.get('Width'), lenk,
                    pl.get('ImportReferenceNumber'), pl.get('ItemNo'),
                    pl.get('LastKnownCFSStatusCode'), _datekey_from_iso(pl.get('LastKnownCFSStatusDate')), _time_from_iso(pl.get('LastKnownCFSStatusDate')),
                    pl.get('LinePrice'), pl.get('Link'), pl.get('LoadingMeters'), pl.get('MarksAndNos'), pl.get('OutturnComment'), pl.get('OutturnDamagedQty'), pl.get('OutturnedHeight'), pl.get('OutturnedLength'), pl.get('OutturnedVolume'), pl.get('OutturnedWeight'), pl.get('OutturnedWidth'), pl.get('OutturnPillagedQty'), pl.get('OutturnQty'), pl.get('PackingLineID'), pl.get('PackQty'), packk, pl.get('ReferenceNumber'),
                    (1 if pl.get('RequiresTemperatureControl') is True else (0 if pl.get('RequiresTemperatureControl') is False else None)),
                    pl.get('Volume'), volk, pl.get('Weight'), weik,
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
        # Sub-shipment-level lines from AR, attach to AR when SubShipments not persisted
        sub_list = shf.get('SubShipments') if isinstance(shf, dict) else None
        if sub_list:
            for sub_obj in sub_list:  # type: ignore[assignment]
                packs2 = sub_obj.get('PackingLines') or []  # type: ignore[index]
                for pl in packs2:
                    len_code, len_desc = pl.get('LengthUnit') or (None, None)
                    vol_code, vol_desc = pl.get('VolumeUnit') or (None, None)
                    wei_code, wei_desc = pl.get('WeightUnit') or (None, None)
                    pack_code, pack_desc = pl.get('PackType') or (None, None)
                    lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                    volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                    weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                    packk = ensure_unit(cur, pack_code, pack_desc, 'Packs') if pack_code else None
                    rows.append((
                        None, None, fact_key,
                        'AR',
                        pl.get('CommodityCode'), pl.get('CommodityDescription'), pl.get('ContainerLink'), pl.get('ContainerNumber'), pl.get('ContainerPackingOrder'), pl.get('CountryOfOriginCode'), pl.get('DetailedDescription'), pl.get('EndItemNo'), pl.get('ExportReferenceNumber'), pl.get('GoodsDescription'), pl.get('HarmonisedCode'),
                        pl.get('Height'), pl.get('Length'), pl.get('Width'), lenk,
                        pl.get('ImportReferenceNumber'), pl.get('ItemNo'),
                        pl.get('LastKnownCFSStatusCode'), _datekey_from_iso(pl.get('LastKnownCFSStatusDate')), _time_from_iso(pl.get('LastKnownCFSStatusDate')),
                        pl.get('LinePrice'), pl.get('Link'), pl.get('LoadingMeters'), pl.get('MarksAndNos'), pl.get('OutturnComment'), pl.get('OutturnDamagedQty'), pl.get('OutturnedHeight'), pl.get('OutturnedLength'), pl.get('OutturnedVolume'), pl.get('OutturnedWeight'), pl.get('OutturnedWidth'), pl.get('OutturnPillagedQty'), pl.get('OutturnQty'), pl.get('PackingLineID'), pl.get('PackQty'), packk, pl.get('ReferenceNumber'),
                        (1 if pl.get('RequiresTemperatureControl') is True else (0 if pl.get('RequiresTemperatureControl') is False else None)),
                        pl.get('Volume'), volk, pl.get('Weight'), weik,
                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                    ))
        if rows:
            try:
                cur.fast_executemany = True
            except Exception:
                pass
            # Build placeholders dynamically to match values length (avoids param count mismatches)
            _ph = ",".join(["?"] * len(rows[0]))
            cur.executemany(
                "INSERT INTO Dwh2.FactPackingLine (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, CommodityCode, CommodityDescription, ContainerLink, ContainerNumber, ContainerPackingOrder, CountryOfOriginCode, DetailedDescription, EndItemNo, ExportReferenceNumber, GoodsDescription, HarmonisedCode, Height, Length, Width, LengthUnitKey, ImportReferenceNumber, ItemNo, LastKnownCFSStatusCode, LastKnownCFSStatusDateKey, LastKnownCFSStatusTime, LinePrice, [Link], LoadingMeters, MarksAndNos, OutturnComment, OutturnDamagedQty, OutturnedHeight, OutturnedLength, OutturnedVolume, OutturnedWeight, OutturnedWidth, OutturnPillagedQty, OutturnQty, PackingLineID, PackQty, PackTypeUnitKey, ReferenceNumber, RequiresTemperatureControl, Volume, VolumeUnitKey, Weight, WeightUnitKey, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (" + _ph + ")",
                rows
            )
            if counters:
                counters.add('Dwh2.FactPackingLine', added=len(rows))

    # Create minimal FactShipment from embedded Shipment in AR, but only if not already present (by ShipmentJobKey)
    if fact.get('ShipmentFromAR'):
        shf = fact['ShipmentFromAR']  # type: ignore[assignment]
        # Jobs
        _, consol_job_id = shf.get('ConsolJob', (None, None))  # type: ignore[assignment]
        _, shipment_job_id = shf.get('ShipmentJob', (None, None))  # type: ignore[assignment]
        consol_job_key = ensure_job(cur, 'ForwardingConsol', consol_job_id) if consol_job_id else None
        shipment_job_key = ensure_job(cur, 'ForwardingShipment', shipment_job_id) if shipment_job_id else None
        if shipment_job_key:
            # If already exists (likely loaded by CSL), do nothing to avoid overwriting richer data
            cur.execute("SELECT FactShipmentKey FROM Dwh2.FactShipment WHERE ShipmentJobKey = ?", shipment_job_key)
            r = cur.fetchone()
            if not r:
                # Ensure ports and dims
                port_keys = {}
                ports = shf.get('Ports') or {}
                for pname, (pcode, pname_) in ports.items():  # type: ignore[attr-defined]
                    port_keys[pname] = ensure_port(cur, pcode, pname_)
                awb_code, awb_desc = shf.get('AWB', ('',''))  # type: ignore[assignment]
                gateway_code, gateway_desc = shf.get('Gateway', ('',''))  # type: ignore[assignment]
                st_code, st_desc = shf.get('ShipmentType', ('',''))  # type: ignore[assignment]
                rt_code, rt_desc = shf.get('ReleaseType', ('',''))  # type: ignore[assignment]
                scr_code, scr_desc = shf.get('ScreeningStatus', ('',''))  # type: ignore[assignment]
                pm_code, pm_desc = shf.get('PaymentMethod', ('',''))  # type: ignore[assignment]
                cur_code, cur_desc = shf.get('Currency', ('',''))  # type: ignore[assignment]
                cm_code, cm_desc = shf.get('ContainerMode', ('',''))  # type: ignore[assignment]
                co2s_code, co2s_desc = shf.get('CO2eStatus', ('',''))  # type: ignore[assignment]
                co2u_code, co2u_desc = shf.get('CO2eUnit', ('',''))  # type: ignore[assignment]
                tvu_code, tvu_desc = shf.get('TotalVolumeUnit', ('',''))  # type: ignore[assignment]
                twu_code, twu_desc = shf.get('TotalWeightUnit', ('',''))  # type: ignore[assignment]
                pu_code, pu_desc = shf.get('PacksUnit', ('',''))  # type: ignore[assignment]
                awb_sl = ensure_service_level(cur, awb_code, awb_desc, 'AWB') if awb_code else None
                gateway_sl = ensure_service_level(cur, gateway_code, gateway_desc, 'Gateway') if gateway_code else None
                shipment_type_sl = ensure_service_level(cur, st_code, st_desc, 'ShipmentType') if st_code else None
                release_type_sl = ensure_service_level(cur, rt_code, rt_desc, 'ReleaseType') if rt_code else None
                screening_key = ensure_screening_status(cur, scr_code, scr_desc) if scr_code else None
                payment_key = ensure_payment_method(cur, pm_code, pm_desc) if pm_code else None
                currency_key = ensure_currency(cur, cur_code, cur_desc) if cur_code else None
                container_mode_key = ensure_container_mode(cur, cm_code, cm_desc) if cm_code else None
                co2e_status_key = ensure_co2e_status(cur, co2s_code, co2s_desc) if co2s_code else None
                co2e_unit_key = ensure_unit(cur, co2u_code, co2u_desc, 'CO2e') if co2u_code else None
                total_volume_unit_key = ensure_unit(cur, tvu_code, tvu_desc, 'Volume') if tvu_code else None
                total_weight_unit_key = ensure_unit(cur, twu_code, twu_desc, 'Weight') if twu_code else None
                packs_unit_key = ensure_unit(cur, pu_code, pu_desc, 'Packs') if pu_code else None
                measures_sh = shf.get('Measures', {})  # type: ignore[assignment]
                flags_sh = shf.get('Flags', {})  # type: ignore[assignment]
                cols = {
                    'CompanyKey': company_key,
                    'BranchKey': branch_key,
                    'DepartmentKey': simple_keys.get('DepartmentKey'),
                    'EventTypeKey': simple_keys.get('EventTypeKey'),
                    'ActionPurposeKey': simple_keys.get('ActionPurposeKey'),
                    'UserKey': simple_keys.get('UserKey'),
                    'EnterpriseKey': simple_keys.get('EnterpriseKey'),
                    'ServerKey': simple_keys.get('ServerKey'),
                    'DataProviderKey': simple_keys.get('DataProviderKey'),
                    'TriggerDateKey': fact.get('TriggerDateKey'),
                    'ConsolJobKey': consol_job_key,
                    'ShipmentJobKey': shipment_job_key,
                    'PlaceOfDeliveryKey': port_keys.get('PlaceOfDelivery'),
                    'PlaceOfIssueKey': port_keys.get('PlaceOfIssue'),
                    'PlaceOfReceiptKey': port_keys.get('PlaceOfReceipt'),
                    'PortFirstForeignKey': port_keys.get('PortFirstForeign'),
                    'PortLastForeignKey': port_keys.get('PortLastForeign'),
                    'PortOfDischargeKey': port_keys.get('PortOfDischarge'),
                    'PortOfFirstArrivalKey': port_keys.get('PortOfFirstArrival'),
                    'PortOfLoadingKey': port_keys.get('PortOfLoading'),
                    'EventBranchHomePortKey': port_keys.get('EventBranchHomePort'),
                    'AWBServiceLevelKey': awb_sl,
                    'GatewayServiceLevelKey': gateway_sl,
                    'ShipmentTypeKey': shipment_type_sl,
                    'ReleaseTypeKey': release_type_sl,
                    'ScreeningStatusKey': screening_key,
                    'PaymentMethodKey': payment_key,
                    'FreightRateCurrencyKey': currency_key,
                    'TotalVolumeUnitKey': total_volume_unit_key,
                    'TotalWeightUnitKey': total_weight_unit_key,
                    'CO2eUnitKey': co2e_unit_key,
                    'PacksUnitKey': packs_unit_key,
                    'ContainerModeKey': container_mode_key,
                    'Co2eStatusKey': co2e_status_key,
                    'ContainerCount': measures_sh.get('ContainerCount'),
                    'ChargeableRate': measures_sh.get('ChargeableRate'),
                    'DocumentedChargeable': measures_sh.get('DocumentedChargeable'),
                    'DocumentedVolume': measures_sh.get('DocumentedVolume'),
                    'DocumentedWeight': measures_sh.get('DocumentedWeight'),
                    'FreightRate': measures_sh.get('FreightRate'),
                    'GreenhouseGasEmissionCO2e': measures_sh.get('GreenhouseGasEmissionCO2e'),
                    'ManifestedChargeable': measures_sh.get('ManifestedChargeable'),
                    'ManifestedVolume': measures_sh.get('ManifestedVolume'),
                    'ManifestedWeight': measures_sh.get('ManifestedWeight'),
                    'MaximumAllowablePackageHeight': measures_sh.get('MaximumAllowablePackageHeight'),
                    'MaximumAllowablePackageLength': measures_sh.get('MaximumAllowablePackageLength'),
                    'MaximumAllowablePackageWidth': measures_sh.get('MaximumAllowablePackageWidth'),
                    'NoCopyBills': measures_sh.get('NoCopyBills'),
                    'NoOriginalBills': measures_sh.get('NoOriginalBills'),
                    'OuterPacks': measures_sh.get('OuterPacks'),
                    'TotalNoOfPacks': measures_sh.get('TotalNoOfPacks'),
                    'TotalPreallocatedChargeable': measures_sh.get('TotalPreallocatedChargeable'),
                    'TotalPreallocatedVolume': measures_sh.get('TotalPreallocatedVolume'),
                    'TotalPreallocatedWeight': measures_sh.get('TotalPreallocatedWeight'),
                    'TotalVolume': measures_sh.get('TotalVolume'),
                    'TotalWeight': measures_sh.get('TotalWeight'),
                    'IsCFSRegistered': 1 if flags_sh.get('IsCFSRegistered') is True else (0 if flags_sh.get('IsCFSRegistered') is False else None),
                    'IsDirectBooking': 1 if flags_sh.get('IsDirectBooking') is True else (0 if flags_sh.get('IsDirectBooking') is False else None),
                    'IsForwardRegistered': 1 if flags_sh.get('IsForwardRegistered') is True else (0 if flags_sh.get('IsForwardRegistered') is False else None),
                    'IsHazardous': 1 if flags_sh.get('IsHazardous') is True else (0 if flags_sh.get('IsHazardous') is False else None),
                    'IsNeutralMaster': 1 if flags_sh.get('IsNeutralMaster') is True else (0 if flags_sh.get('IsNeutralMaster') is False else None),
                    'RequiresTemperatureControl': 1 if flags_sh.get('RequiresTemperatureControl') is True else (0 if flags_sh.get('RequiresTemperatureControl') is False else None),
                }
                col_names = list(cols.keys())
                placeholders = ','.join(['?'] * len(col_names))
                cur.execute("INSERT INTO Dwh2.FactShipment ([" + "],[".join(col_names) + "]) VALUES (" + placeholders + ")", *list(cols.values()))
                if counters:
                    counters.add('Dwh2.FactShipment', added=1)
                # Get the new FactShipmentKey to attach SubShipments
                cur.execute("SELECT FactShipmentKey FROM Dwh2.FactShipment WHERE ShipmentJobKey = ?", shipment_job_key)
                r2 = cur.fetchone()
                fact_ship_key2 = int(r2[0]) if r2 else None

                # ContainerCollection under shipment level from AR fallback Shipment
                conts_sh2 = shf.get('ContainersShipment') if isinstance(shf, dict) else None
                if fact_ship_key2 and conts_sh2:
                    try:
                        cur.execute("DELETE FROM Dwh2.FactContainer WHERE FactShipmentKey = ? AND Source = 'AR'", fact_ship_key2)
                    except Exception:
                        pass
                    rows = []
                    for ct in conts_sh2:  # type: ignore[assignment]
                        len_code, len_desc = ct.get('LengthUnit') or (None, None)
                        vol_code, vol_desc = ct.get('VolumeUnit') or (None, None)
                        wei_code, wei_desc = ct.get('WeightUnit') or (None, None)
                        lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                        volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                        weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                        ct_code, ct_desc, ct_iso, cat_code, cat_desc = ct.get('ContainerType') or (None, None, None, None, None)
                        fclc, fcld = ct.get('FCL_LCL_AIR') or (None, None)
                        rows.append((
                            fact_ship_key2, None, None,
                            'AR',
                            ct.get('ContainerJobID'), ct.get('ContainerNumber'), ct.get('Link'),
                            ct_code, ct_desc, ct_iso, cat_code, cat_desc,
                            ct.get('DeliveryMode'), fclc, fcld, ct.get('ContainerCount'),
                            ct.get('Seal'), ct.get('SealPartyTypeCode'), ct.get('SecondSeal'), ct.get('SecondSealPartyTypeCode'), ct.get('ThirdSeal'), ct.get('ThirdSealPartyTypeCode'), ct.get('StowagePosition'),
                            lenk, volk, weik,
                            ct.get('TotalHeight'), ct.get('TotalLength'), ct.get('TotalWidth'), ct.get('TareWeight'), ct.get('GrossWeight'), ct.get('GoodsWeight'), ct.get('VolumeCapacity'), ct.get('WeightCapacity'), ct.get('DunnageWeight'), ct.get('OverhangBack'), ct.get('OverhangFront'), ct.get('OverhangHeight'), ct.get('OverhangLeft'), ct.get('OverhangRight'), ct.get('HumidityPercent'), ct.get('AirVentFlow'), ct.get('AirVentFlowRateUnitCode'),
                            (1 if ct.get('NonOperatingReefer') is True else (0 if ct.get('NonOperatingReefer') is False else None)),
                            (1 if ct.get('IsCFSRegistered') is True else (0 if ct.get('IsCFSRegistered') is False else None)),
                            (1 if ct.get('IsControlledAtmosphere') is True else (0 if ct.get('IsControlledAtmosphere') is False else None)),
                            (1 if ct.get('IsDamaged') is True else (0 if ct.get('IsDamaged') is False else None)),
                            (1 if ct.get('IsEmptyContainer') is True else (0 if ct.get('IsEmptyContainer') is False else None)),
                            (1 if ct.get('IsSealOk') is True else (0 if ct.get('IsSealOk') is False else None)),
                            (1 if ct.get('IsShipperOwned') is True else (0 if ct.get('IsShipperOwned') is False else None)),
                            (1 if ct.get('ArrivalPickupByRail') is True else (0 if ct.get('ArrivalPickupByRail') is False else None)),
                            (1 if ct.get('DepartureDeliveryByRail') is True else (0 if ct.get('DepartureDeliveryByRail') is False else None)),
                            _datekey_from_iso(ct.get('ArrivalSlotDateTime')), _time_from_iso(ct.get('ArrivalSlotDateTime')),
                            _datekey_from_iso(ct.get('DepartureSlotDateTime')), _time_from_iso(ct.get('DepartureSlotDateTime')),
                            _datekey_from_iso(ct.get('EmptyReadyForReturn')), _time_from_iso(ct.get('EmptyReadyForReturn')),
                            _datekey_from_iso(ct.get('FCLWharfGateIn')), _time_from_iso(ct.get('FCLWharfGateIn')),
                            _datekey_from_iso(ct.get('FCLWharfGateOut')), _time_from_iso(ct.get('FCLWharfGateOut')),
                            _datekey_from_iso(ct.get('FCLStorageCommences')), _time_from_iso(ct.get('FCLStorageCommences')),
                            _datekey_from_iso(ct.get('LCLUnpack')), _time_from_iso(ct.get('LCLUnpack')),
                            _datekey_from_iso(ct.get('PackDate')), _time_from_iso(ct.get('PackDate')),
                            company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                        ))
                    if rows:
                        try:
                            if ENABLE_BATCH:
                                cur.fast_executemany = True
                        except Exception:
                            pass
                        insert_placeholders = ",".join(["?"] * len(rows[0]))
                        sql = (
                            "INSERT INTO Dwh2.FactContainer (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, ContainerJobID, ContainerNumber, [Link], ContainerTypeCode, ContainerTypeDescription, ContainerTypeISOCode, ContainerCategoryCode, ContainerCategoryDescription, DeliveryMode, FCL_LCL_AIR_Code, FCL_LCL_AIR_Description, ContainerCount, Seal, SealPartyTypeCode, SecondSeal, SecondSealPartyTypeCode, ThirdSeal, ThirdSealPartyTypeCode, StowagePosition, LengthUnitKey, VolumeUnitKey, WeightUnitKey, TotalHeight, TotalLength, TotalWidth, TareWeight, GrossWeight, GoodsWeight, VolumeCapacity, WeightCapacity, DunnageWeight, OverhangBack, OverhangFront, OverhangHeight, OverhangLeft, OverhangRight, HumidityPercent, AirVentFlow, AirVentFlowRateUnitCode, NonOperatingReefer, IsCFSRegistered, IsControlledAtmosphere, IsDamaged, IsEmptyContainer, IsSealOk, IsShipperOwned, ArrivalPickupByRail, DepartureDeliveryByRail, ArrivalSlotDateKey, ArrivalSlotTime, DepartureSlotDateKey, DepartureSlotTime, EmptyReadyForReturnDateKey, EmptyReadyForReturnTime, FCLWharfGateInDateKey, FCLWharfGateInTime, FCLWharfGateOutDateKey, FCLWharfGateOutTime, FCLStorageCommencesDateKey, FCLStorageCommencesTime, LCLUnpackDateKey, LCLUnpackTime, PackDateKey, PackTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (" + insert_placeholders + ")"
                        )
                        cur.executemany(sql, rows)
                        if counters:
                            counters.add('Dwh2.FactContainer', added=len(rows))

                # DateCollection under shipment level from AR fallback Shipment -> FactEventDate (parent = Shipment, Source='AR')
                date_sh2 = shf.get('DateCollection') if isinstance(shf, dict) else None
                # Merge with AR header-level DateCollection to ensure no loss if parser missed some at shipment level
                if isinstance(fact, dict) and (fact.get('DateCollection')):
                    if date_sh2:
                        date_sh2 = list(date_sh2) + list(fact.get('DateCollection') or [])
                    else:
                        date_sh2 = list(fact.get('DateCollection') or [])
                if fact_ship_key2 and date_sh2:
                    # Dedupe shipment-level dates by removing ones also present at sub-shipment level and intra-level dups
                    sub_level_keys = set()
                    for sub_obj in (shf.get('SubShipments') or []):
                        for d0 in (sub_obj.get('DateCollection') or []):
                            sub_level_keys.add(((d0.get('DateTypeCode') or ''), d0.get('DateKey'), d0.get('Time')))
                    seen_ship = set()
                    date_sh2 = [
                        d for d in date_sh2
                        if not (
                            ((d.get('DateTypeCode') or ''), d.get('DateKey'), d.get('Time')) in sub_level_keys
                            or ((d.get('DateTypeCode') or ''), d.get('DateKey'), d.get('Time')) in seen_ship
                        ) and not seen_ship.add(((d.get('DateTypeCode') or ''), d.get('DateKey'), d.get('Time')))
                    ]
                    try:
                        cur.execute("DELETE FROM Dwh2.FactEventDate WHERE FactShipmentKey = ? AND Source = 'AR'", fact_ship_key2)
                    except Exception:
                        pass
                    try:
                        if ENABLE_BATCH:
                            cur.fast_executemany = True
                    except Exception:
                        pass
                    rows_ed = []
                    for r in date_sh2:
                        rows_ed.append((
                            fact_ship_key2,  # FactShipmentKey
                            None,            # FactSubShipmentKey
                            None,            # FactARKey
                            'AR',
                            r.get('DateTypeCode'), r.get('DateTypeDescription'), r.get('DateKey'), r.get('Time'), r.get('DateTimeText'), r.get('IsEstimate'), r.get('Value'), r.get('TimeZone'),
                            company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                        ))
                    if rows_ed:
                        if rows_ed:
                            cur.executemany(
                                "INSERT INTO Dwh2.FactEventDate (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, DateTypeCode, DateTypeDescription, DateKey, [Time], DateTimeText, IsEstimate, [Value], TimeZone, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                rows_ed
                            )
                            if counters:
                                counters.add('Dwh2.FactEventDate', added=len(rows_ed))

                # Packing lines under shipment level from AR fallback Shipment
                packs_sh = shf.get('PackingLinesShipment') if isinstance(shf, dict) else None
                if fact_ship_key2 and packs_sh:
                    try:
                        cur.execute("DELETE FROM Dwh2.FactPackingLine WHERE FactShipmentKey = ? AND Source = 'AR'", fact_ship_key2)
                    except Exception:
                        pass
                    rows = []
                    for pl in packs_sh:  # type: ignore[assignment]
                        len_code, len_desc = pl.get('LengthUnit') or (None, None)
                        vol_code, vol_desc = pl.get('VolumeUnit') or (None, None)
                        wei_code, wei_desc = pl.get('WeightUnit') or (None, None)
                        pack_code, pack_desc = pl.get('PackType') or (None, None)
                        lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                        volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                        weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                        packk = ensure_unit(cur, pack_code, pack_desc, 'Packs') if pack_code else None
                        rows.append((
                            fact_ship_key2, None, None,
                            'AR',
                            pl.get('CommodityCode'), pl.get('CommodityDescription'), pl.get('ContainerLink'), pl.get('ContainerNumber'), pl.get('ContainerPackingOrder'), pl.get('CountryOfOriginCode'), pl.get('DetailedDescription'), pl.get('EndItemNo'), pl.get('ExportReferenceNumber'), pl.get('GoodsDescription'), pl.get('HarmonisedCode'),
                            pl.get('Height'), pl.get('Length'), pl.get('Width'), lenk,
                            pl.get('ImportReferenceNumber'), pl.get('ItemNo'),
                            pl.get('LastKnownCFSStatusCode'), _datekey_from_iso(pl.get('LastKnownCFSStatusDate')), _time_from_iso(pl.get('LastKnownCFSStatusDate')),
                            pl.get('LinePrice'), pl.get('Link'), pl.get('LoadingMeters'), pl.get('MarksAndNos'), pl.get('OutturnComment'), pl.get('OutturnDamagedQty'), pl.get('OutturnedHeight'), pl.get('OutturnedLength'), pl.get('OutturnedVolume'), pl.get('OutturnedWeight'), pl.get('OutturnedWidth'), pl.get('OutturnPillagedQty'), pl.get('OutturnQty'), pl.get('PackingLineID'), pl.get('PackQty'), packk, pl.get('ReferenceNumber'),
                            (1 if pl.get('RequiresTemperatureControl') is True else (0 if pl.get('RequiresTemperatureControl') is False else None)),
                            pl.get('Volume'), volk, pl.get('Weight'), weik,
                            company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                        ))
                    if rows:
                        try:
                            if ENABLE_BATCH:
                                cur.fast_executemany = True
                        except Exception:
                            pass
                        cur.executemany(
                            "INSERT INTO Dwh2.FactPackingLine (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, CommodityCode, CommodityDescription, ContainerLink, ContainerNumber, ContainerPackingOrder, CountryOfOriginCode, DetailedDescription, EndItemNo, ExportReferenceNumber, GoodsDescription, HarmonisedCode, Height, Length, Width, LengthUnitKey, ImportReferenceNumber, ItemNo, LastKnownCFSStatusCode, LastKnownCFSStatusDateKey, LastKnownCFSStatusTime, LinePrice, [Link], LoadingMeters, MarksAndNos, OutturnComment, OutturnDamagedQty, OutturnedHeight, OutturnedLength, OutturnedVolume, OutturnedWeight, OutturnedWidth, OutturnPillagedQty, OutturnQty, PackingLineID, PackQty, PackTypeUnitKey, ReferenceNumber, RequiresTemperatureControl, Volume, VolumeUnitKey, Weight, WeightUnitKey, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            rows
                        )

                # Transport legs at shipment level (AR-created shipment)
                legs_sh2 = shf.get('TransportLegsShipment') if isinstance(shf, dict) else None
                if fact_ship_key2 and legs_sh2:
                    # Intra-batch dedupe and idempotent replace per shipment
                    def _norm2(s: Optional[str]) -> str:
                        return (s or '').strip()
                    def _leg_key2(leg: dict) -> tuple:
                        pol = ((_norm2((leg.get('PortOfLoading') or ('', ''))[0])).upper())
                        pod = ((_norm2((leg.get('PortOfDischarge') or ('', ''))[0])).upper())
                        mode = _norm2(leg.get('TransportMode')).lower()
                        vessel = _norm2(leg.get('VesselLloydsIMO')) or _norm2(leg.get('VesselName')).upper()
                        voyage = _norm2(leg.get('VoyageFlightNo')).upper()
                        order_ = leg.get('Order') or 0
                        times = (
                            _norm2(leg.get('ActualArrival')), _norm2(leg.get('ActualDeparture')),
                            _norm2(leg.get('EstimatedArrival')), _norm2(leg.get('EstimatedDeparture')),
                            _norm2(leg.get('ScheduledArrival')), _norm2(leg.get('ScheduledDeparture')),
                        )
                        return (order_, pol, pod, mode, vessel, voyage) + times
                    seen_keys2: set[tuple] = set()
                    legs_clean = [l for l in (legs_sh2 or []) if not (_leg_key2(l) in seen_keys2) and not seen_keys2.add(_leg_key2(l))]
                    try:
                        cur.execute("DELETE FROM Dwh2.FactTransportLeg WHERE FactShipmentKey = ?", fact_ship_key2)
                    except Exception:
                        pass
                    rows_tl = []
                    for leg in legs_clean:
                        pol_code, pol_name = leg['PortOfLoading']
                        pod_code, pod_name = leg['PortOfDischarge']
                        polk = ensure_port(cur, pol_code, pol_name) if pol_code else None
                        podk = ensure_port(cur, pod_code, pod_name) if pod_code else None
                        bsc, bsd = leg.get('BookingStatus') if leg.get('BookingStatus') else (None, None)
                        carr_code, carr_name, carr_country_code, carr_port_code = leg.get('Carrier') if leg.get('Carrier') else (None, None, None, None)
                        cred_code, cred_name, cred_country_code, cred_port_code = leg.get('Creditor') if leg.get('Creditor') else (None, None, None, None)
                        carr_key = ensure_organization_min(cur, carr_code, carr_name, carr_country_code, carr_port_code) if carr_code else None
                        cred_key = ensure_organization_min(cur, cred_code, cred_name, cred_country_code, cred_port_code) if cred_code else None
                        rows_tl.append((
                            fact_ship_key2, None, None,
                            polk, podk, leg.get('Order'), leg.get('TransportMode'),
                            leg.get('VesselName'), leg.get('VesselLloydsIMO'), leg.get('VoyageFlightNo'),
                            leg.get('CarrierBookingReference'), bsc, bsd,
                            carr_key, cred_key,
                            _datekey_from_iso(leg.get('ActualArrival')), _time_from_iso(leg.get('ActualArrival')),
                            _datekey_from_iso(leg.get('ActualDeparture')), _time_from_iso(leg.get('ActualDeparture')),
                            _datekey_from_iso(leg.get('EstimatedArrival')), _time_from_iso(leg.get('EstimatedArrival')),
                            _datekey_from_iso(leg.get('EstimatedDeparture')), _time_from_iso(leg.get('EstimatedDeparture')),
                            _datekey_from_iso(leg.get('ScheduledArrival')), _time_from_iso(leg.get('ScheduledArrival')),
                            _datekey_from_iso(leg.get('ScheduledDeparture')), _time_from_iso(leg.get('ScheduledDeparture')),
                            company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                        ))
                    if rows_tl:
                        try:
                            if ENABLE_BATCH:
                                cur.fast_executemany = True
                        except Exception:
                            pass
                        cur.executemany(
                            "INSERT INTO Dwh2.FactTransportLeg (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, PortOfLoadingKey, PortOfDischargeKey, [Order], TransportMode, VesselName, VesselLloydsIMO, VoyageFlightNo, CarrierBookingReference, BookingStatusCode, BookingStatusDescription, CarrierOrganizationKey, CreditorOrganizationKey, ActualArrivalDateKey, ActualArrivalTime, ActualDepartureDateKey, ActualDepartureTime, EstimatedArrivalDateKey, EstimatedArrivalTime, EstimatedDepartureDateKey, EstimatedDepartureTime, ScheduledArrivalDateKey, ScheduledArrivalTime, ScheduledDepartureDateKey, ScheduledDepartureTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            rows_tl
                        )
                        if counters:
                            counters.add('Dwh2.FactTransportLeg', added=len(rows_tl))

                # Insert SubShipments from AR (if present)
                sub_list = shf.get('SubShipments') if isinstance(shf, dict) else None
                if fact_ship_key2 and sub_list:
                    for sub_obj in sub_list:  # type: ignore[assignment]
                        ports2 = sub_obj['Ports']  # type: ignore[index]
                        polk = ensure_port(cur, *ports2['PortOfLoading']) if ports2['PortOfLoading'][0] else None
                        podk = ensure_port(cur, *ports2['PortOfDischarge']) if ports2['PortOfDischarge'][0] else None
                        pfak = ensure_port(cur, *ports2['PortOfFirstArrival']) if ports2['PortOfFirstArrival'][0] else None
                        podstk = ensure_port(cur, *ports2['PortOfDestination']) if ports2['PortOfDestination'][0] else None
                        porgk = ensure_port(cur, *ports2['PortOfOrigin']) if ports2['PortOfOrigin'][0] else None
                        ebpk = ensure_port(cur, *ports2['EventBranchHomePort']) if ports2['EventBranchHomePort'][0] else None
                        dims_sub = sub_obj['Dims']  # type: ignore[index]
                        slk = ensure_service_level(cur, *dims_sub['ServiceLevel'], 'Service') if dims_sub['ServiceLevel'][0] else None
                        stk = ensure_service_level(cur, *dims_sub['ShipmentType'], 'ShipmentType') if dims_sub['ShipmentType'][0] else None
                        rtk = ensure_service_level(cur, *dims_sub['ReleaseType'], 'ReleaseType') if dims_sub['ReleaseType'][0] else None
                        cmk = ensure_container_mode(cur, *dims_sub['ContainerMode']) if dims_sub['ContainerMode'][0] else None
                        frck = ensure_currency(cur, *dims_sub['FreightRateCurrency']) if dims_sub['FreightRateCurrency'][0] else None
                        gvck = ensure_currency(cur, *dims_sub['GoodsValueCurrency']) if dims_sub['GoodsValueCurrency'][0] else None
                        ivck = ensure_currency(cur, *dims_sub['InsuranceValueCurrency']) if dims_sub['InsuranceValueCurrency'][0] else None
                        tvuk = ensure_unit(cur, *dims_sub['TotalVolumeUnit'], 'Volume') if dims_sub['TotalVolumeUnit'][0] else None
                        twuk = ensure_unit(cur, *dims_sub['TotalWeightUnit'], 'Weight') if dims_sub['TotalWeightUnit'][0] else None
                        puk = ensure_unit(cur, *dims_sub['PacksUnit'], 'Packs') if dims_sub['PacksUnit'][0] else None
                        co2euk = ensure_unit(cur, *dims_sub['CO2eUnit'], 'CO2e') if dims_sub['CO2eUnit'][0] else None
                        attrs = sub_obj['Attrs']  # type: ignore[index]
                        def as_int(s: Optional[str]) -> Optional[int]:
                            return int(s) if s and s.isdigit() else None
                        cols_sub = {
                            'FactShipmentKey': fact_ship_key2,
                            'FactAccountsReceivableTransactionKey': None,
                            'EventBranchHomePortKey': ebpk,
                            'PortOfLoadingKey': polk,
                            'PortOfDischargeKey': podk,
                            'PortOfFirstArrivalKey': pfak,
                            'PortOfDestinationKey': podstk,
                            'PortOfOriginKey': porgk,
                            'ServiceLevelKey': slk,
                            'ShipmentTypeKey': stk,
                            'ReleaseTypeKey': rtk,
                            'ContainerModeKey': cmk,
                            'FreightRateCurrencyKey': frck,
                            'GoodsValueCurrencyKey': gvck,
                            'InsuranceValueCurrencyKey': ivck,
                            'TotalVolumeUnitKey': tvuk,
                            'TotalWeightUnitKey': twuk,
                            'PacksUnitKey': puk,
                            'CO2eUnitKey': co2euk,
                            'WayBillNumber': attrs.get('WayBillNumber'),
                            'WayBillTypeCode': (attrs.get('WayBillType') or (None, None))[0],
                            'WayBillTypeDescription': (attrs.get('WayBillType') or (None, None))[1],
                            'VesselName': attrs.get('VesselName'),
                            'VoyageFlightNo': attrs.get('VoyageFlightNo'),
                            'LloydsIMO': attrs.get('LloydsIMO'),
                            'TransportMode': attrs.get('TransportMode'),
                            'ContainerCount': as_int(attrs.get('ContainerCount')),
                            'ActualChargeable': attrs.get('ActualChargeable'),
                            'DocumentedChargeable': attrs.get('DocumentedChargeable'),
                            'DocumentedVolume': attrs.get('DocumentedVolume'),
                            'DocumentedWeight': attrs.get('DocumentedWeight'),
                            'GoodsValue': attrs.get('GoodsValue'),
                            'InsuranceValue': attrs.get('InsuranceValue'),
                            'FreightRate': attrs.get('FreightRate'),
                            'TotalVolume': attrs.get('TotalVolume'),
                            'TotalWeight': attrs.get('TotalWeight'),
                            'TotalNoOfPacks': as_int(attrs.get('TotalNoOfPacks')),
                            'OuterPacks': as_int(attrs.get('OuterPacks')),
                            'GreenhouseGasEmissionCO2e': attrs.get('GreenhouseGasEmissionCO2e'),
                            'IsBooking': 1 if (str(attrs.get('IsBooking') or '').lower() == 'true') else (0 if (str(attrs.get('IsBooking') or '').lower() == 'false') else None),
                            'IsCancelled': 1 if (str(attrs.get('IsCancelled') or '').lower() == 'true') else (0 if (str(attrs.get('IsCancelled') or '').lower() == 'false') else None),
                            'IsCFSRegistered': 1 if (str(attrs.get('IsCFSRegistered') or '').lower() == 'true') else (0 if (str(attrs.get('IsCFSRegistered') or '').lower() == 'false') else None),
                            'IsDirectBooking': 1 if (str(attrs.get('IsDirectBooking') or '').lower() == 'true') else (0 if (str(attrs.get('IsDirectBooking') or '').lower() == 'false') else None),
                            'IsForwardRegistered': 1 if (str(attrs.get('IsForwardRegistered') or '').lower() == 'true') else (0 if (str(attrs.get('IsForwardRegistered') or '').lower() == 'false') else None),
                            'IsHighRisk': 1 if (str(attrs.get('IsHighRisk') or '').lower() == 'true') else (0 if (str(attrs.get('IsHighRisk') or '').lower() == 'false') else None),
                            'IsNeutralMaster': 1 if (str(attrs.get('IsNeutralMaster') or '').lower() == 'true') else (0 if (str(attrs.get('IsNeutralMaster') or '').lower() == 'false') else None),
                            'IsShipping': 1 if (str(attrs.get('IsShipping') or '').lower() == 'true') else (0 if (str(attrs.get('IsShipping') or '').lower() == 'false') else None),
                            'IsSplitShipment': 1 if (str(attrs.get('IsSplitShipment') or '').lower() == 'true') else (0 if (str(attrs.get('IsSplitShipment') or '').lower() == 'false') else None),
                        }
                        col_names_sub = list(cols_sub.keys()) + ['CompanyKey', 'DepartmentKey', 'EventUserKey', 'DataProviderKey']
                        col_vals_sub = list(cols_sub.values()) + [company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')]
                        placeholders_sub = ','.join(['?'] * len(col_names_sub))
                        cur.execute(
                            "INSERT INTO Dwh2.FactSubShipment ([" + "],[".join(col_names_sub) + "]) VALUES (" + placeholders_sub + ")",
                            *col_vals_sub
                        )
                        cur.execute("SELECT SCOPE_IDENTITY()")
                        rsub = cur.fetchone()
                        sub_key2 = int(rsub[0]) if rsub and rsub[0] is not None else None
                        if counters and sub_key2:
                            counters.add('Dwh2.FactSubShipment', added=1)

                        # DateCollection under SubShipment (AR) -> FactEventDate (parent = SubShipment, Source='AR')
                        sub_dates2 = sub_obj.get('DateCollection') or []  # type: ignore[index]
                        if sub_key2 and sub_dates2:
                            # Dedupe intra sub-shipment dates
                            seen_sub = set()
                            sub_dates2 = [
                                d for d in sub_dates2
                                if not (((d.get('DateTypeCode') or ''), d.get('DateKey'), d.get('Time')) in seen_sub)
                                and not seen_sub.add(((d.get('DateTypeCode') or ''), d.get('DateKey'), d.get('Time')))
                            ]
                            try:
                                cur.execute("DELETE FROM Dwh2.FactEventDate WHERE FactSubShipmentKey = ? AND Source='AR'", sub_key2)
                            except Exception:
                                pass
                            try:
                                if ENABLE_BATCH:
                                    cur.fast_executemany = True
                            except Exception:
                                pass
                            rows_ed2 = []
                            for r in sub_dates2:
                                rows_ed2.append((
                                    None,            # FactShipmentKey must be NULL when SubShipment parent is set
                                    sub_key2,        # FactSubShipmentKey
                                    None,            # FactARKey
                                    'AR',
                                    r.get('DateTypeCode'), r.get('DateTypeDescription'), r.get('DateKey'), r.get('Time'), r.get('DateTimeText'), r.get('IsEstimate'), r.get('Value'), r.get('TimeZone'),
                                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                ))
                            if rows_ed2:
                                if rows_ed2:
                                    if rows_ed2:
                                        if rows_ed2:
                                            if rows_ed2:
                                                cur.executemany(
                                                    "INSERT INTO Dwh2.FactEventDate (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, DateTypeCode, DateTypeDescription, DateKey, [Time], DateTimeText, IsEstimate, [Value], TimeZone, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                                    rows_ed2
                                                )
                                                if counters:
                                                    counters.add('Dwh2.FactEventDate', added=len(rows_ed2))

                        # Insert JobCosting header for this AR-created SubShipment if present
                        if sub_key2 and sub_obj.get('JobCostingHeader'):
                            h2 = sub_obj['JobCostingHeader']  # type: ignore[index]
                            brk2 = None; dpk2 = None; hb2 = None; osk2 = None; curk2 = None
                            br_code2, br_name2 = (h2.get('Branch') or (None, None))
                            if br_code2:
                                brk2 = ensure_branch(cur, br_code2, br_name2)
                            dp_code2, dp_name2 = (h2.get('Department') or (None, None))
                            if dp_code2:
                                try:
                                    dpk2 = _upsert_scalar_dim(cur, 'Dwh2.DimDepartment', 'Code', dp_code2, 'Name', dp_name2 or dp_code2, key_col='DepartmentKey')
                                except Exception:
                                    cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimDepartment WHERE Code=?) INSERT INTO Dwh2.DimDepartment (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimDepartment SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", dp_code2, dp_code2, dp_name2 or dp_code2, dp_name2 or dp_code2, dp_code2)
                                    cur.execute("SELECT DepartmentKey FROM Dwh2.DimDepartment WHERE Code=?", dp_code2)
                                    r = cur.fetchone(); dpk2 = int(r[0]) if r else None
                            hb_code2, hb_name2 = (h2.get('HomeBranch') or (None, None))
                            if hb_code2:
                                hb2 = ensure_branch(cur, hb_code2, hb_name2)
                            os_code2, os_name2 = (h2.get('OperationsStaff') or (None, None))
                            if os_code2:
                                try:
                                    osk2 = _upsert_scalar_dim(cur, 'Dwh2.DimUser', 'Code', os_code2, 'Name', os_name2 or os_code2, key_col='UserKey')
                                except Exception:
                                    cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimUser WHERE Code=?) INSERT INTO Dwh2.DimUser (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimUser SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", os_code2, os_code2, os_name2 or os_code2, os_name2 or os_code2, os_code2)
                                    cur.execute("SELECT UserKey FROM Dwh2.DimUser WHERE Code=?", os_code2)
                                    r = cur.fetchone(); osk2 = int(r[0]) if r else None
                            cur_code2, cur_desc2 = (h2.get('Currency') or (None, None))
                            if cur_code2:
                                curk2 = ensure_currency(cur, cur_code2, cur_desc2)
                            def dec2(v: Optional[str]) -> Optional[float]:
                                try:
                                    return float(v) if v is not None and v != '' else None
                                except Exception:
                                    return None
                            cols2 = [
                                ('FactShipmentKey', None),
                                ('FactSubShipmentKey', sub_key2),
                                ('FactAccountsReceivableTransactionKey', None),
                                ('Source', 'AR'),
                                ('BranchKey', brk2), ('DepartmentKey', dpk2), ('HomeBranchKey', hb2), ('OperationsStaffKey', osk2), ('CurrencyKey', curk2),
                                ('ClientContractNumber', h2.get('ClientContractNumber')),
                                ('AccrualNotRecognized', dec2(h2.get('AccrualNotRecognized'))),
                                ('AccrualRecognized', dec2(h2.get('AccrualRecognized'))),
                                ('AgentRevenue', dec2(h2.get('AgentRevenue'))),
                                ('LocalClientRevenue', dec2(h2.get('LocalClientRevenue'))),
                                ('OtherDebtorRevenue', dec2(h2.get('OtherDebtorRevenue'))),
                                ('TotalAccrual', dec2(h2.get('TotalAccrual'))),
                                ('TotalCost', dec2(h2.get('TotalCost'))),
                                ('TotalJobProfit', dec2(h2.get('TotalJobProfit'))),
                                ('TotalRevenue', dec2(h2.get('TotalRevenue'))),
                                ('TotalWIP', dec2(h2.get('TotalWIP'))),
                                ('WIPNotRecognized', dec2(h2.get('WIPNotRecognized'))),
                                ('WIPRecognized', dec2(h2.get('WIPRecognized'))),
                                ('CompanyKey', company_key), ('EventUserKey', simple_keys.get('UserKey')), ('DataProviderKey', simple_keys.get('DataProviderKey')),
                            ]
                            coln2 = [c for c,_ in cols2]
                            vals2 = [v for _,v in cols2]
                            ph2 = ','.join(['?'] * len(coln2))
                            cur.execute("INSERT INTO Dwh2.FactJobCosting (" + ','.join('['+c+']' for c in coln2) + ") VALUES (" + ph2 + ")", *vals2)
                            if counters:
                                counters.add('Dwh2.FactJobCosting', added=1)


                        # Transport legs under sub-shipment
                        tlegs2 = sub_obj.get('TransportLegs') or []  # type: ignore[index]
                        if sub_key2 and tlegs2:
                            # Intra-batch de-duplication by a normalized composite key
                            def _norm2(s: Optional[str]) -> str:
                                return (s or '').strip()
                            def _leg_key2(leg: dict) -> tuple:
                                pol = ((_norm2((leg.get('PortOfLoading') or ('', ''))[0])).upper())
                                pod = ((_norm2((leg.get('PortOfDischarge') or ('', ''))[0])).upper())
                                mode = _norm2(leg.get('TransportMode')).lower()
                                vessel = _norm2(leg.get('VesselLloydsIMO')) or _norm2(leg.get('VesselName')).upper()
                                voyage = _norm2(leg.get('VoyageFlightNo')).upper()
                                order_ = leg.get('Order') or 0
                                times = (
                                    _norm2(leg.get('ActualArrival')), _norm2(leg.get('ActualDeparture')),
                                    _norm2(leg.get('EstimatedArrival')), _norm2(leg.get('EstimatedDeparture')),
                                    _norm2(leg.get('ScheduledArrival')), _norm2(leg.get('ScheduledDeparture')),
                                )
                                return (order_, pol, pod, mode, vessel, voyage) + times
                            seen_keys2: set[tuple] = set()
                            tlegs2 = [
                                l for l in tlegs2
                                if not (_leg_key2(l) in seen_keys2) and not seen_keys2.add(_leg_key2(l))
                            ]
                            # Idempotency: clear any previous legs for this SubShipment
                            try:
                                cur.execute("DELETE FROM Dwh2.FactTransportLeg WHERE FactSubShipmentKey = ?", sub_key2)
                            except Exception:
                                pass
                            rows = []
                            for leg in tlegs2:
                                pol_code, pol_name = leg['PortOfLoading']
                                pod_code, pod_name = leg['PortOfDischarge']
                                polk2 = ensure_port(cur, pol_code, pol_name) if pol_code else None
                                podk2 = ensure_port(cur, pod_code, pod_name) if pod_code else None
                                bsc, bsd = leg['BookingStatus']
                                carr_code, carr_name, carr_country_code, carr_port_code = leg['Carrier']
                                cred_code, cred_name, cred_country_code, cred_port_code = leg['Creditor']
                                carr_key2 = ensure_organization_min(cur, carr_code, carr_name, carr_country_code, carr_port_code) if carr_code else None
                                cred_key2 = ensure_organization_min(cur, cred_code, cred_name, cred_country_code, cred_port_code) if cred_code else None
                                rows.append((
                                    None, sub_key2, None,
                                    polk2, podk2, leg.get('Order'), leg.get('TransportMode'),
                                    leg.get('VesselName'), leg.get('VesselLloydsIMO'), leg.get('VoyageFlightNo'),
                                    leg.get('CarrierBookingReference'), bsc, bsd,
                                    carr_key2, cred_key2,
                                    _datekey_from_iso(leg.get('ActualArrival')), _time_from_iso(leg.get('ActualArrival')),
                                    _datekey_from_iso(leg.get('ActualDeparture')), _time_from_iso(leg.get('ActualDeparture')),
                                    _datekey_from_iso(leg.get('EstimatedArrival')), _time_from_iso(leg.get('EstimatedArrival')),
                                    _datekey_from_iso(leg.get('EstimatedDeparture')), _time_from_iso(leg.get('EstimatedDeparture')),
                                    _datekey_from_iso(leg.get('ScheduledArrival')), _time_from_iso(leg.get('ScheduledArrival')),
                                    _datekey_from_iso(leg.get('ScheduledDeparture')), _time_from_iso(leg.get('ScheduledDeparture')),
                                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                ))
                            if rows:
                                try:
                                    if ENABLE_BATCH:
                                        cur.fast_executemany = True
                                except Exception:
                                    pass
                                cur.executemany(
                                    "INSERT INTO Dwh2.FactTransportLeg (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, PortOfLoadingKey, PortOfDischargeKey, [Order], TransportMode, VesselName, VesselLloydsIMO, VoyageFlightNo, CarrierBookingReference, BookingStatusCode, BookingStatusDescription, CarrierOrganizationKey, CreditorOrganizationKey, ActualArrivalDateKey, ActualArrivalTime, ActualDepartureDateKey, ActualDepartureTime, EstimatedArrivalDateKey, EstimatedArrivalTime, EstimatedDepartureDateKey, EstimatedDepartureTime, ScheduledArrivalDateKey, ScheduledArrivalTime, ScheduledDepartureDateKey, ScheduledDepartureTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    rows
                                )

                        # Charge lines under sub-shipment (source AR)
                        charge_lines2 = sub_obj.get('ChargeLines') or []  # type: ignore[index]
                        if sub_key2 and charge_lines2:
                            rows = []
                            for cl in charge_lines2:
                                br_code, br_name = cl['Branch']
                                brk2 = ensure_branch(cur, br_code, br_name) if br_code else None
                                dept_code, dept_name = cl['Department']
                                dept_key2 = None
                                if dept_code:
                                    try:
                                        dept_key2 = _upsert_scalar_dim(cur, 'Dwh2.DimDepartment', 'Code', dept_code, 'Name', dept_name or dept_code, key_col='DepartmentKey')
                                    except Exception:
                                        cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimDepartment WHERE Code=?) INSERT INTO Dwh2.DimDepartment (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimDepartment SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", dept_code, dept_code, dept_name or dept_code, dept_name or dept_code, dept_code)
                                        cur.execute("SELECT DepartmentKey FROM Dwh2.DimDepartment WHERE Code=?", dept_code)
                                        r = cur.fetchone(); dept_key2 = int(r[0]) if r else None
                                chg_code, chg_desc = cl['ChargeCode']
                                chg_group_code, chg_group_desc = cl['ChargeGroup']
                                cred_key2 = ensure_organization_min(cur, cl.get('CreditorKey') or '') if cl.get('CreditorKey') else None
                                debt_key2 = ensure_organization_min(cur, cl.get('DebtorKey') or '') if cl.get('DebtorKey') else None
                                sell_pt = cl['Sell'].get('PostedTransaction') if cl.get('Sell') else None
                                rows.append((
                                    None, sub_key2, None,
                                    'AR',
                                    brk2, dept_key2,
                                    chg_code or None,
                                    chg_desc or None,
                                    chg_group_code or None,
                                    chg_group_desc or None,
                                    cl.get('Description'),
                                    int(cl.get('DisplaySequence') or '0') if str(cl.get('DisplaySequence') or '').isdigit() else 0,
                                    cred_key2, debt_key2,
                                    cl['Cost'].get('APInvoiceNumber') if cl.get('Cost') else None,
                                    _datekey_from_iso(cl['Cost'].get('DueDate')) if cl.get('Cost') else None, _time_from_iso(cl['Cost'].get('DueDate')) if cl.get('Cost') else None,
                                    (cl['Cost'].get('ExchangeRate') if cl.get('Cost') else None),
                                    _datekey_from_iso(cl['Cost'].get('InvoiceDate')) if cl.get('Cost') else None, _time_from_iso(cl['Cost'].get('InvoiceDate')) if cl.get('Cost') else None,
                                    1 if (str((cl['Cost'].get('IsPosted') if cl.get('Cost') else '') or '').lower() == 'true') else (0 if (str((cl['Cost'].get('IsPosted') if cl.get('Cost') else '') or '').lower() == 'false') else None),
                                    (cl['Cost'].get('LocalAmount') if cl.get('Cost') else None),
                                    (cl['Cost'].get('OSAmount') if cl.get('Cost') else None),
                                    ensure_currency(cur, *(cl['Cost'].get('OSCurrency') or ('',''))) if cl.get('Cost') and cl['Cost'].get('OSCurrency') and cl['Cost'].get('OSCurrency')[0] else None,
                                    (cl['Cost'].get('OSGSTVATAmount') if cl.get('Cost') else None),
                                    (cl['Sell'].get('ExchangeRate') if cl.get('Sell') else None),
                                    (cl['Sell'].get('GSTVAT') or (None,None))[0] if cl.get('Sell') else None,
                                    (cl['Sell'].get('GSTVAT') or (None,None))[1] if cl.get('Sell') else None,
                                    (cl['Sell'].get('InvoiceType') if cl.get('Sell') else None),
                                    1 if (str((cl['Sell'].get('IsPosted') if cl.get('Sell') else '') or '').lower() == 'true') else (0 if (str((cl['Sell'].get('IsPosted') if cl.get('Sell') else '') or '').lower() == 'false') else None),
                                    (cl['Sell'].get('LocalAmount') if cl.get('Sell') else None),
                                    (cl['Sell'].get('OSAmount') if cl.get('Sell') else None),
                                    ensure_currency(cur, *(cl['Sell'].get('OSCurrency') or ('',''))) if cl.get('Sell') and cl['Sell'].get('OSCurrency') and cl['Sell'].get('OSCurrency')[0] else None,
                                    (cl['Sell'].get('OSGSTVATAmount') if cl.get('Sell') else None),
                                    (sell_pt or (None,None,None,None,None,None))[0] if sell_pt else None,
                                    (sell_pt or (None,None,None,None,None,None))[1] if sell_pt else None,
                                    _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[2]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[2]) if sell_pt else None,
                                    _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[3]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[3]) if sell_pt else None,
                                    _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[4]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[4]) if sell_pt else None,
                                    (sell_pt or (None,None,None,None,None,None))[5] if sell_pt else None,
                                    cl.get('SupplierReference'),
                                    cl.get('SellReference'),
                                    company_key, simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                ))
                            if rows:
                                try:
                                    if ENABLE_BATCH:
                                        cur.fast_executemany = True
                                except Exception:
                                    pass
                                insert_placeholders = ",".join(["?"] * len(rows[0]))
                                sql = (
                                    "IF NOT EXISTS (SELECT 1 FROM Dwh2.FactChargeLine WHERE FactSubShipmentKey=? "
                                    "AND [DisplaySequence]=? AND ISNULL([ChargeCode],'') = ISNULL(?,'')) "
                                    "INSERT INTO Dwh2.FactChargeLine ("
                                    "FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, BranchKey, DepartmentKey, "
                                    "ChargeCode, ChargeCodeDescription, ChargeCodeGroup, ChargeCodeGroupDescription, [Description], DisplaySequence, "
                                    "CreditorOrganizationKey, DebtorOrganizationKey, CostAPInvoiceNumber, CostDueDateKey, CostDueTime, CostExchangeRate, CostInvoiceDateKey, CostInvoiceTime, "
                                    "CostIsPosted, CostLocalAmount, CostOSAmount, CostOSCurrencyKey, CostOSGSTVATAmount, "
                                    "SellExchangeRate, SellGSTVATTaxCode, SellGSTVATDescription, SellInvoiceType, SellIsPosted, SellLocalAmount, SellOSAmount, SellOSCurrencyKey, SellOSGSTVATAmount, "
                                    "SellPostedTransactionNumber, SellPostedTransactionType, SellTransactionDateKey, SellTransactionTime, SellDueDateKey, SellDueTime, SellFullyPaidDateKey, SellFullyPaidTime, "
                                    "SellOutstandingAmount, SupplierReference, SellReference, CompanyKey, EventUserKey, DataProviderKey) VALUES (" + insert_placeholders + ")"
                                )
                                rows2 = []
                                for r in rows:
                                    display_seq = r[11]
                                    charge_code = r[6]
                                    rows2.append((sub_key2, display_seq, charge_code) + r)
                                cur.executemany(sql, rows2)

                        # Additional references under sub-shipment
                        add_refs2 = sub_obj.get('AdditionalReferences') or []  # type: ignore[index]
                        if sub_key2 and add_refs2:
                            rows = []
                            for r in add_refs2:
                                coi_code, coi_name = r.get('CountryOfIssue') or (None, None)
                                coi_key2 = ensure_country(cur, coi_code, coi_name) if coi_code else None
                                rows.append((
                                    None, sub_key2, None,
                                    'AR',
                                    r.get('TypeCode'), r.get('TypeDescription'), r.get('ReferenceNumber'), r.get('ContextInformation'),
                                    coi_key2,
                                    _datekey_from_iso(r.get('IssueDate')), _time_from_iso(r.get('IssueDate')),
                                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                ))
                            try:
                                if ENABLE_BATCH:
                                    cur.fast_executemany = True
                            except Exception:
                                pass
                            cur.executemany(
                                "INSERT INTO Dwh2.FactAdditionalReference (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, TypeCode, TypeDescription, ReferenceNumber, ContextInformation, CountryOfIssueKey, IssueDateKey, IssueTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                rows
                            )

                        # ContainerCollection under sub-shipment (AR)
                        conts2 = sub_obj.get('Containers') or []  # type: ignore[index]
                        if sub_key2 and conts2:
                            try:
                                cur.execute("DELETE FROM Dwh2.FactContainer WHERE FactSubShipmentKey = ? AND Source = 'AR'", sub_key2)
                            except Exception:
                                pass
                            rows = []
                            for ct in conts2:
                                len_code, len_desc = ct.get('LengthUnit') or (None, None)
                                vol_code, vol_desc = ct.get('VolumeUnit') or (None, None)
                                wei_code, wei_desc = ct.get('WeightUnit') or (None, None)
                                lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                                volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                                weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                                ct_code, ct_desc, ct_iso, cat_code, cat_desc = ct.get('ContainerType') or (None, None, None, None, None)
                                fclc, fcld = ct.get('FCL_LCL_AIR') or (None, None)
                                rows.append((
                                    None, sub_key2, None,
                                    'AR',
                                    ct.get('ContainerJobID'), ct.get('ContainerNumber'), ct.get('Link'),
                                    ct_code, ct_desc, ct_iso, cat_code, cat_desc,
                                    ct.get('DeliveryMode'), fclc, fcld, ct.get('ContainerCount'),
                                    ct.get('Seal'), ct.get('SealPartyTypeCode'), ct.get('SecondSeal'), ct.get('SecondSealPartyTypeCode'), ct.get('ThirdSeal'), ct.get('ThirdSealPartyTypeCode'), ct.get('StowagePosition'),
                                    lenk, volk, weik,
                                    ct.get('TotalHeight'), ct.get('TotalLength'), ct.get('TotalWidth'), ct.get('TareWeight'), ct.get('GrossWeight'), ct.get('GoodsWeight'), ct.get('VolumeCapacity'), ct.get('WeightCapacity'), ct.get('DunnageWeight'), ct.get('OverhangBack'), ct.get('OverhangFront'), ct.get('OverhangHeight'), ct.get('OverhangLeft'), ct.get('OverhangRight'), ct.get('HumidityPercent'), ct.get('AirVentFlow'), ct.get('AirVentFlowRateUnitCode'),
                                    (1 if ct.get('NonOperatingReefer') is True else (0 if ct.get('NonOperatingReefer') is False else None)),
                                    (1 if ct.get('IsCFSRegistered') is True else (0 if ct.get('IsCFSRegistered') is False else None)),
                                    (1 if ct.get('IsControlledAtmosphere') is True else (0 if ct.get('IsControlledAtmosphere') is False else None)),
                                    (1 if ct.get('IsDamaged') is True else (0 if ct.get('IsDamaged') is False else None)),
                                    (1 if ct.get('IsEmptyContainer') is True else (0 if ct.get('IsEmptyContainer') is False else None)),
                                    (1 if ct.get('IsSealOk') is True else (0 if ct.get('IsSealOk') is False else None)),
                                    (1 if ct.get('IsShipperOwned') is True else (0 if ct.get('IsShipperOwned') is False else None)),
                                    (1 if ct.get('ArrivalPickupByRail') is True else (0 if ct.get('ArrivalPickupByRail') is False else None)),
                                    (1 if ct.get('DepartureDeliveryByRail') is True else (0 if ct.get('DepartureDeliveryByRail') is False else None)),
                                    _datekey_from_iso(ct.get('ArrivalSlotDateTime')), _time_from_iso(ct.get('ArrivalSlotDateTime')),
                                    _datekey_from_iso(ct.get('DepartureSlotDateTime')), _time_from_iso(ct.get('DepartureSlotDateTime')),
                                    _datekey_from_iso(ct.get('EmptyReadyForReturn')), _time_from_iso(ct.get('EmptyReadyForReturn')),
                                    _datekey_from_iso(ct.get('FCLWharfGateIn')), _time_from_iso(ct.get('FCLWharfGateIn')),
                                    _datekey_from_iso(ct.get('FCLWharfGateOut')), _time_from_iso(ct.get('FCLWharfGateOut')),
                                    _datekey_from_iso(ct.get('FCLStorageCommences')), _time_from_iso(ct.get('FCLStorageCommences')),
                                    _datekey_from_iso(ct.get('LCLUnpack')), _time_from_iso(ct.get('LCLUnpack')),
                                    _datekey_from_iso(ct.get('PackDate')), _time_from_iso(ct.get('PackDate')),
                                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                ))
                            if rows:
                                try:
                                    if ENABLE_BATCH:
                                        cur.fast_executemany = True
                                except Exception:
                                    pass
                                insert_placeholders = ",".join(["?"] * len(rows[0]))
                                sql = (
                                    "INSERT INTO Dwh2.FactContainer (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, ContainerJobID, ContainerNumber, [Link], ContainerTypeCode, ContainerTypeDescription, ContainerTypeISOCode, ContainerCategoryCode, ContainerCategoryDescription, DeliveryMode, FCL_LCL_AIR_Code, FCL_LCL_AIR_Description, ContainerCount, Seal, SealPartyTypeCode, SecondSeal, SecondSealPartyTypeCode, ThirdSeal, ThirdSealPartyTypeCode, StowagePosition, LengthUnitKey, VolumeUnitKey, WeightUnitKey, TotalHeight, TotalLength, TotalWidth, TareWeight, GrossWeight, GoodsWeight, VolumeCapacity, WeightCapacity, DunnageWeight, OverhangBack, OverhangFront, OverhangHeight, OverhangLeft, OverhangRight, HumidityPercent, AirVentFlow, AirVentFlowRateUnitCode, NonOperatingReefer, IsCFSRegistered, IsControlledAtmosphere, IsDamaged, IsEmptyContainer, IsSealOk, IsShipperOwned, ArrivalPickupByRail, DepartureDeliveryByRail, ArrivalSlotDateKey, ArrivalSlotTime, DepartureSlotDateKey, DepartureSlotTime, EmptyReadyForReturnDateKey, EmptyReadyForReturnTime, FCLWharfGateInDateKey, FCLWharfGateInTime, FCLWharfGateOutDateKey, FCLWharfGateOutTime, FCLStorageCommencesDateKey, FCLStorageCommencesTime, LCLUnpackDateKey, LCLUnpackTime, PackDateKey, PackTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (" + insert_placeholders + ")"
                                )
                                cur.executemany(sql, rows)
                                if counters:
                                    counters.add('Dwh2.FactContainer', added=len(rows))

                        # Packing lines under sub-shipment (AR)
                        packs2 = sub_obj.get('PackingLines') or []  # type: ignore[index]
                        if sub_key2 and packs2:
                            try:
                                cur.execute("DELETE FROM Dwh2.FactPackingLine WHERE FactSubShipmentKey = ? AND Source = 'AR'", sub_key2)
                            except Exception:
                                pass
                            rows = []
                            for pl in packs2:
                                len_code, len_desc = pl.get('LengthUnit') or (None, None)
                                vol_code, vol_desc = pl.get('VolumeUnit') or (None, None)
                                wei_code, wei_desc = pl.get('WeightUnit') or (None, None)
                                pack_code, pack_desc = pl.get('PackType') or (None, None)
                                lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                                volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                                weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                                packk = ensure_unit(cur, pack_code, pack_desc, 'Packs') if pack_code else None
                                rows.append((
                                    None, sub_key2, None,
                                    'AR',
                                    pl.get('CommodityCode'), pl.get('CommodityDescription'), pl.get('ContainerLink'), pl.get('ContainerNumber'), pl.get('ContainerPackingOrder'), pl.get('CountryOfOriginCode'), pl.get('DetailedDescription'), pl.get('EndItemNo'), pl.get('ExportReferenceNumber'), pl.get('GoodsDescription'), pl.get('HarmonisedCode'),
                                    pl.get('Height'), pl.get('Length'), pl.get('Width'), lenk,
                                    pl.get('ImportReferenceNumber'), pl.get('ItemNo'),
                                    pl.get('LastKnownCFSStatusCode'), _datekey_from_iso(pl.get('LastKnownCFSStatusDate')), _time_from_iso(pl.get('LastKnownCFSStatusDate')),
                                    pl.get('LinePrice'), pl.get('Link'), pl.get('LoadingMeters'), pl.get('MarksAndNos'), pl.get('OutturnComment'), pl.get('OutturnDamagedQty'), pl.get('OutturnedHeight'), pl.get('OutturnedLength'), pl.get('OutturnedVolume'), pl.get('OutturnedWeight'), pl.get('OutturnedWidth'), pl.get('OutturnPillagedQty'), pl.get('OutturnQty'), pl.get('PackingLineID'), pl.get('PackQty'), packk, pl.get('ReferenceNumber'),
                                    (1 if pl.get('RequiresTemperatureControl') is True else (0 if pl.get('RequiresTemperatureControl') is False else None)),
                                    pl.get('Volume'), volk, pl.get('Weight'), weik,
                                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                ))
                            if rows:
                                try:
                                    if ENABLE_BATCH:
                                        cur.fast_executemany = True
                                except Exception:
                                    pass
                                cur.executemany(
                                    "INSERT INTO Dwh2.FactPackingLine (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, CommodityCode, CommodityDescription, ContainerLink, ContainerNumber, ContainerPackingOrder, CountryOfOriginCode, DetailedDescription, EndItemNo, ExportReferenceNumber, GoodsDescription, HarmonisedCode, Height, Length, Width, LengthUnitKey, ImportReferenceNumber, ItemNo, LastKnownCFSStatusCode, LastKnownCFSStatusDateKey, LastKnownCFSStatusTime, LinePrice, [Link], LoadingMeters, MarksAndNos, OutturnComment, OutturnDamagedQty, OutturnedHeight, OutturnedLength, OutturnedVolume, OutturnedWeight, OutturnedWidth, OutturnPillagedQty, OutturnQty, PackingLineID, PackQty, PackTypeUnitKey, ReferenceNumber, RequiresTemperatureControl, Volume, VolumeUnitKey, Weight, WeightUnitKey, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    rows
                                )
            else:
                # Shipment exists; if it has no SubShipments yet and AR provides some, insert them now
                fact_ship_key_exist = int(r[0])
                # Always handle shipment-level DateCollection from AR, even if AR has no SubShipments
                date_sh_exist = shf.get('DateCollection') if isinstance(shf, dict) else None
                # Merge with AR header-level DateCollection to ensure completeness on first pass
                if isinstance(fact, dict) and (fact.get('DateCollection')):
                    if date_sh_exist:
                        date_sh_exist = list(date_sh_exist) + list(fact.get('DateCollection') or [])
                    else:
                        date_sh_exist = list(fact.get('DateCollection') or [])
                if date_sh_exist:
                    # Build sub-level exclusion keys if AR provided sub-shipments; otherwise only intra-level dedupe
                    sub_list = shf.get('SubShipments') if isinstance(shf, dict) else None
                    sub_level_keys2 = set()
                    if sub_list:
                        for sub_obj in (sub_list or []):
                            for d0 in (sub_obj.get('DateCollection') or []):
                                sub_level_keys2.add(((d0.get('DateTypeCode') or ''), d0.get('DateKey'), d0.get('Time')))
                    seen_ship2 = set()
                    date_sh_exist = [
                        d for d in date_sh_exist
                        if not (
                            ((d.get('DateTypeCode') or ''), d.get('DateKey'), d.get('Time')) in sub_level_keys2
                            or ((d.get('DateTypeCode') or ''), d.get('DateKey'), d.get('Time')) in seen_ship2
                        ) and not seen_ship2.add(((d.get('DateTypeCode') or ''), d.get('DateKey'), d.get('Time')))
                    ]
                    try:
                        cur.execute("DELETE FROM Dwh2.FactEventDate WHERE FactShipmentKey = ? AND Source='AR'", fact_ship_key_exist)
                    except Exception:
                        pass
                    try:
                        if ENABLE_BATCH:
                            cur.fast_executemany = True
                    except Exception:
                        pass
                    rows_ed = []
                    for r2 in date_sh_exist:
                        rows_ed.append((
                            fact_ship_key_exist, None, None, 'AR',
                            r2.get('DateTypeCode'), r2.get('DateTypeDescription'), r2.get('DateKey'), r2.get('Time'), r2.get('DateTimeText'), r2.get('IsEstimate'), r2.get('Value'), r2.get('TimeZone'),
                            company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                        ))
                    if rows_ed:
                        cur.executemany(
                            "INSERT INTO Dwh2.FactEventDate (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, DateTypeCode, DateTypeDescription, DateKey, [Time], DateTimeText, IsEstimate, [Value], TimeZone, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            rows_ed
                        )
                        if counters:
                            counters.add('Dwh2.FactEventDate', added=len(rows_ed))

                sub_list = shf.get('SubShipments') if isinstance(shf, dict) else None
                if sub_list:
                    cur.execute("SELECT COUNT(1) FROM Dwh2.FactSubShipment WHERE FactShipmentKey=?", fact_ship_key_exist)
                    rcnt = cur.fetchone(); sub_count = int(rcnt[0]) if rcnt else 0
                    # If the shipment already has SubShipments, update sub-shipment AR DateCollections
                    if sub_count > 0:
                        # (Shipment-level AR dates already handled above)

                        # Also attach/replace shipment-level transport legs from AR (even when sub-shipments exist)
                        legs_sh_exist = shf.get('TransportLegsShipment') if isinstance(shf, dict) else None
                        if fact_ship_key_exist and legs_sh_exist:
                            def _norm2(s: Optional[str]) -> str:
                                return (s or '').strip()
                            def _leg_key2(leg: dict) -> tuple:
                                pol = ((_norm2((leg.get('PortOfLoading') or ('', ''))[0])).upper())
                                pod = ((_norm2((leg.get('PortOfDischarge') or ('', ''))[0])).upper())
                                mode = _norm2(leg.get('TransportMode')).lower()
                                vessel = _norm2(leg.get('VesselLloydsIMO')) or _norm2(leg.get('VesselName')).upper()
                                voyage = _norm2(leg.get('VoyageFlightNo')).upper()
                                order_ = leg.get('Order') or 0
                                times = (
                                    _norm2(leg.get('ActualArrival')), _norm2(leg.get('ActualDeparture')),
                                    _norm2(leg.get('EstimatedArrival')), _norm2(leg.get('EstimatedDeparture')),
                                    _norm2(leg.get('ScheduledArrival')), _norm2(leg.get('ScheduledDeparture')),
                                )
                                return (order_, pol, pod, mode, vessel, voyage) + times
                            seen_keys2: set[tuple] = set()
                            legs_clean_exist = [l for l in (legs_sh_exist or []) if not (_leg_key2(l) in seen_keys2) and not seen_keys2.add(_leg_key2(l))]
                            try:
                                cur.execute("DELETE FROM Dwh2.FactTransportLeg WHERE FactShipmentKey = ?", fact_ship_key_exist)
                            except Exception:
                                pass
                            rows_tl_exist = []
                            for leg in legs_clean_exist:
                                pol_code, pol_name = leg['PortOfLoading']
                                pod_code, pod_name = leg['PortOfDischarge']
                                polk3 = ensure_port(cur, pol_code, pol_name) if pol_code else None
                                podk3 = ensure_port(cur, pod_code, pod_name) if pod_code else None
                                bsc, bsd = leg.get('BookingStatus') if leg.get('BookingStatus') else (None, None)
                                carr_code, carr_name, carr_country_code, carr_port_code = leg.get('Carrier') if leg.get('Carrier') else (None, None, None, None)
                                cred_code, cred_name, cred_country_code, cred_port_code = leg.get('Creditor') if leg.get('Creditor') else (None, None, None, None)
                                carr_key3 = ensure_organization_min(cur, carr_code, carr_name, carr_country_code, carr_port_code) if carr_code else None
                                cred_key3 = ensure_organization_min(cur, cred_code, cred_name, cred_country_code, cred_port_code) if cred_code else None
                                rows_tl_exist.append((
                                    fact_ship_key_exist, None, None,
                                    polk3, podk3, leg.get('Order'), leg.get('TransportMode'),
                                    leg.get('VesselName'), leg.get('VesselLloydsIMO'), leg.get('VoyageFlightNo'),
                                    leg.get('CarrierBookingReference'), bsc, bsd,
                                    carr_key3, cred_key3,
                                    _datekey_from_iso(leg.get('ActualArrival')), _time_from_iso(leg.get('ActualArrival')),
                                    _datekey_from_iso(leg.get('ActualDeparture')), _time_from_iso(leg.get('ActualDeparture')),
                                    _datekey_from_iso(leg.get('EstimatedArrival')), _time_from_iso(leg.get('EstimatedArrival')),
                                    _datekey_from_iso(leg.get('EstimatedDeparture')), _time_from_iso(leg.get('EstimatedDeparture')),
                                    _datekey_from_iso(leg.get('ScheduledArrival')), _time_from_iso(leg.get('ScheduledArrival')),
                                    _datekey_from_iso(leg.get('ScheduledDeparture')), _time_from_iso(leg.get('ScheduledDeparture')),
                                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                ))
                            if rows_tl_exist:
                                try:
                                    if ENABLE_BATCH:
                                        cur.fast_executemany = True
                                except Exception:
                                    pass
                                cur.executemany(
                                    "INSERT INTO Dwh2.FactTransportLeg (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, PortOfLoadingKey, PortOfDischargeKey, [Order], TransportMode, VesselName, VesselLloydsIMO, VoyageFlightNo, CarrierBookingReference, BookingStatusCode, BookingStatusDescription, CarrierOrganizationKey, CreditorOrganizationKey, ActualArrivalDateKey, ActualArrivalTime, ActualDepartureDateKey, ActualDepartureTime, EstimatedArrivalDateKey, EstimatedArrivalTime, EstimatedDepartureDateKey, EstimatedDepartureTime, ScheduledArrivalDateKey, ScheduledArrivalTime, ScheduledDepartureDateKey, ScheduledDepartureTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    rows_tl_exist
                                )
                                if counters:
                                    counters.add('Dwh2.FactTransportLeg', added=len(rows_tl_exist))
                        for sub_obj in (sub_list or []):  # type: ignore[assignment]
                            attrs = sub_obj.get('Attrs') if isinstance(sub_obj, dict) else None
                            waybill = attrs.get('WayBillNumber') if isinstance(attrs, dict) else None
                            if waybill is None:
                                waybill = ''
                            try:
                                cur.execute("SELECT FactSubShipmentKey FROM Dwh2.FactSubShipment WHERE FactShipmentKey=? AND ISNULL(WayBillNumber,'')=?", fact_ship_key_exist, waybill)
                                rr = cur.fetchone()
                                sub_key_exist = int(rr[0]) if rr else None
                            except Exception:
                                sub_key_exist = None
                            if not sub_key_exist:
                                continue
                            sub_dates_exist = sub_obj.get('DateCollection') if isinstance(sub_obj, dict) else None
                            if sub_dates_exist:
                                # Dedupe intra sub-shipment dates
                                seen_sub2 = set()
                                sub_dates_exist = [
                                    d for d in sub_dates_exist
                                    if not (((d.get('DateTypeCode') or ''), d.get('DateKey'), d.get('Time')) in seen_sub2)
                                    and not seen_sub2.add(((d.get('DateTypeCode') or ''), d.get('DateKey'), d.get('Time')))
                                ]
                                try:
                                    cur.execute("DELETE FROM Dwh2.FactEventDate WHERE FactSubShipmentKey = ? AND Source='AR'", sub_key_exist)
                                except Exception:
                                    pass
                                try:
                                    if ENABLE_BATCH:
                                        cur.fast_executemany = True
                                except Exception:
                                    pass
                                rows_ed2 = []
                                for r2 in sub_dates_exist:
                                    rows_ed2.append((
                                        None,                 # FactShipmentKey must be NULL when SubShipment parent is set
                                        sub_key_exist,        # FactSubShipmentKey
                                        None,                 # FactAccountsReceivableTransactionKey
                                        'AR',
                                        r2.get('DateTypeCode'), r2.get('DateTypeDescription'), r2.get('DateKey'), r2.get('Time'), r2.get('DateTimeText'), r2.get('IsEstimate'), r2.get('Value'), r2.get('TimeZone'),
                                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                    ))
                                if rows_ed2:
                                    cur.executemany(
                                        "INSERT INTO Dwh2.FactEventDate (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, DateTypeCode, DateTypeDescription, DateKey, [Time], DateTimeText, IsEstimate, [Value], TimeZone, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                        rows_ed2
                                    )
                                    if counters:
                                        counters.add('Dwh2.FactEventDate', added=len(rows_ed2))
                            # Transport legs under existing sub-shipment (AR)
                            tlegs_exist = sub_obj.get('TransportLegs') if isinstance(sub_obj, dict) else None
                            if sub_key_exist and tlegs_exist:
                                # Intra-batch de-duplication by a normalized composite key
                                def _norm2(s: Optional[str]) -> str:
                                    return (s or '').strip()
                                def _leg_key2(leg: dict) -> tuple:
                                    pol = ((_norm2((leg.get('PortOfLoading') or ('', ''))[0])).upper())
                                    pod = ((_norm2((leg.get('PortOfDischarge') or ('', ''))[0])).upper())
                                    mode = _norm2(leg.get('TransportMode')).lower()
                                    vessel = _norm2(leg.get('VesselLloydsIMO')) or _norm2(leg.get('VesselName')).upper()
                                    voyage = _norm2(leg.get('VoyageFlightNo')).upper()
                                    order_ = leg.get('Order') or 0
                                    times = (
                                        _norm2(leg.get('ActualArrival')), _norm2(leg.get('ActualDeparture')),
                                        _norm2(leg.get('EstimatedArrival')), _norm2(leg.get('EstimatedDeparture')),
                                        _norm2(leg.get('ScheduledArrival')), _norm2(leg.get('ScheduledDeparture')),
                                    )
                                    return (order_, pol, pod, mode, vessel, voyage) + times
                                seen_keys2: set[tuple] = set()
                                tlegs_clean = [
                                    l for l in (tlegs_exist or [])
                                    if not (_leg_key2(l) in seen_keys2) and not seen_keys2.add(_leg_key2(l))
                                ]
                                try:
                                    cur.execute("DELETE FROM Dwh2.FactTransportLeg WHERE FactSubShipmentKey = ?", sub_key_exist)
                                except Exception:
                                    pass
                                rows_tl2 = []
                                for leg in tlegs_clean:
                                    pol_code, pol_name = leg['PortOfLoading']
                                    pod_code, pod_name = leg['PortOfDischarge']
                                    polk2 = ensure_port(cur, pol_code, pol_name) if pol_code else None
                                    podk2 = ensure_port(cur, pod_code, pod_name) if pod_code else None
                                    bsc, bsd = leg['BookingStatus']
                                    carr_code, carr_name, carr_country_code, carr_port_code = leg['Carrier']
                                    cred_code, cred_name, cred_country_code, cred_port_code = leg['Creditor']
                                    carr_key2 = ensure_organization_min(cur, carr_code, carr_name, carr_country_code, carr_port_code) if carr_code else None
                                    cred_key2 = ensure_organization_min(cur, cred_code, cred_name, cred_country_code, cred_port_code) if cred_code else None
                                    rows_tl2.append((
                                        None, sub_key_exist, None,
                                        polk2, podk2, leg.get('Order'), leg.get('TransportMode'),
                                        leg.get('VesselName'), leg.get('VesselLloydsIMO'), leg.get('VoyageFlightNo'),
                                        leg.get('CarrierBookingReference'), bsc, bsd,
                                        carr_key2, cred_key2,
                                        _datekey_from_iso(leg.get('ActualArrival')), _time_from_iso(leg.get('ActualArrival')),
                                        _datekey_from_iso(leg.get('ActualDeparture')), _time_from_iso(leg.get('ActualDeparture')),
                                        _datekey_from_iso(leg.get('EstimatedArrival')), _time_from_iso(leg.get('EstimatedArrival')),
                                        _datekey_from_iso(leg.get('EstimatedDeparture')), _time_from_iso(leg.get('EstimatedDeparture')),
                                        _datekey_from_iso(leg.get('ScheduledArrival')), _time_from_iso(leg.get('ScheduledArrival')),
                                        _datekey_from_iso(leg.get('ScheduledDeparture')), _time_from_iso(leg.get('ScheduledDeparture')),
                                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                    ))
                                if rows_tl2:
                                    try:
                                        if ENABLE_BATCH:
                                            cur.fast_executemany = True
                                    except Exception:
                                        pass
                                    cur.executemany(
                                        "INSERT INTO Dwh2.FactTransportLeg (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, PortOfLoadingKey, PortOfDischargeKey, [Order], TransportMode, VesselName, VesselLloydsIMO, VoyageFlightNo, CarrierBookingReference, BookingStatusCode, BookingStatusDescription, CarrierOrganizationKey, CreditorOrganizationKey, ActualArrivalDateKey, ActualArrivalTime, ActualDepartureDateKey, ActualDepartureTime, EstimatedArrivalDateKey, EstimatedArrivalTime, EstimatedDepartureDateKey, EstimatedDepartureTime, ScheduledArrivalDateKey, ScheduledArrivalTime, ScheduledDepartureDateKey, ScheduledDepartureTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                        rows_tl2
                                    )
                    if sub_count == 0:
                        # (Shipment-level AR dates already handled above)
                        for sub_obj in sub_list:  # type: ignore[assignment]
                            ports2 = sub_obj['Ports']  # type: ignore[index]
                            polk = ensure_port(cur, *ports2['PortOfLoading']) if ports2['PortOfLoading'][0] else None
                            podk = ensure_port(cur, *ports2['PortOfDischarge']) if ports2['PortOfDischarge'][0] else None
                            pfak = ensure_port(cur, *ports2['PortOfFirstArrival']) if ports2['PortOfFirstArrival'][0] else None
                            podstk = ensure_port(cur, *ports2['PortOfDestination']) if ports2['PortOfDestination'][0] else None
                            porgk = ensure_port(cur, *ports2['PortOfOrigin']) if ports2['PortOfOrigin'][0] else None
                            ebpk = ensure_port(cur, *ports2['EventBranchHomePort']) if ports2['EventBranchHomePort'][0] else None
                            dims_sub = sub_obj['Dims']  # type: ignore[index]
                            slk = ensure_service_level(cur, *dims_sub['ServiceLevel'], 'Service') if dims_sub['ServiceLevel'][0] else None
                            stk = ensure_service_level(cur, *dims_sub['ShipmentType'], 'ShipmentType') if dims_sub['ShipmentType'][0] else None
                            rtk = ensure_service_level(cur, *dims_sub['ReleaseType'], 'ReleaseType') if dims_sub['ReleaseType'][0] else None
                            cmk = ensure_container_mode(cur, *dims_sub['ContainerMode']) if dims_sub['ContainerMode'][0] else None
                            frck = ensure_currency(cur, *dims_sub['FreightRateCurrency']) if dims_sub['FreightRateCurrency'][0] else None
                            gvck = ensure_currency(cur, *dims_sub['GoodsValueCurrency']) if dims_sub['GoodsValueCurrency'][0] else None
                            ivck = ensure_currency(cur, *dims_sub['InsuranceValueCurrency']) if dims_sub['InsuranceValueCurrency'][0] else None
                            tvuk = ensure_unit(cur, *dims_sub['TotalVolumeUnit'], 'Volume') if dims_sub['TotalVolumeUnit'][0] else None
                            twuk = ensure_unit(cur, *dims_sub['TotalWeightUnit'], 'Weight') if dims_sub['TotalWeightUnit'][0] else None
                            puk = ensure_unit(cur, *dims_sub['PacksUnit'], 'Packs') if dims_sub['PacksUnit'][0] else None
                            co2euk = ensure_unit(cur, *dims_sub['CO2eUnit'], 'CO2e') if dims_sub['CO2eUnit'][0] else None
                            attrs = sub_obj['Attrs']  # type: ignore[index]
                            def as_int2(s: Optional[str]) -> Optional[int]:
                                return int(s) if s and s.isdigit() else None
                            cols_sub = {
                                'FactShipmentKey': fact_ship_key_exist,
                                'FactAccountsReceivableTransactionKey': None,
                                'EventBranchHomePortKey': ebpk,
                                'PortOfLoadingKey': polk,
                                'PortOfDischargeKey': podk,
                                'PortOfFirstArrivalKey': pfak,
                                'PortOfDestinationKey': podstk,
                                'PortOfOriginKey': porgk,
                                'ServiceLevelKey': slk,
                                'ShipmentTypeKey': stk,
                                'ReleaseTypeKey': rtk,
                                'ContainerModeKey': cmk,
                                'FreightRateCurrencyKey': frck,
                                'GoodsValueCurrencyKey': gvck,
                                'InsuranceValueCurrencyKey': ivck,
                                'TotalVolumeUnitKey': tvuk,
                                'TotalWeightUnitKey': twuk,
                                'PacksUnitKey': puk,
                                'CO2eUnitKey': co2euk,
                                'WayBillNumber': attrs.get('WayBillNumber'),
                                'WayBillTypeCode': (attrs.get('WayBillType') or (None, None))[0],
                                'WayBillTypeDescription': (attrs.get('WayBillType') or (None, None))[1],
                                'VesselName': attrs.get('VesselName'),
                                'VoyageFlightNo': attrs.get('VoyageFlightNo'),
                                'LloydsIMO': attrs.get('LloydsIMO'),
                                'TransportMode': attrs.get('TransportMode'),
                                'ContainerCount': as_int2(attrs.get('ContainerCount')),
                                'ActualChargeable': attrs.get('ActualChargeable'),
                                'DocumentedChargeable': attrs.get('DocumentedChargeable'),
                                'DocumentedVolume': attrs.get('DocumentedVolume'),
                                'DocumentedWeight': attrs.get('DocumentedWeight'),
                                'GoodsValue': attrs.get('GoodsValue'),
                                'InsuranceValue': attrs.get('InsuranceValue'),
                                'FreightRate': attrs.get('FreightRate'),
                                'TotalVolume': attrs.get('TotalVolume'),
                                'TotalWeight': attrs.get('TotalWeight'),
                                'TotalNoOfPacks': as_int2(attrs.get('TotalNoOfPacks')),
                                'OuterPacks': as_int2(attrs.get('OuterPacks')),
                                'GreenhouseGasEmissionCO2e': attrs.get('GreenhouseGasEmissionCO2e'),
                                'IsBooking': 1 if (str(attrs.get('IsBooking') or '').lower() == 'true') else (0 if (str(attrs.get('IsBooking') or '').lower() == 'false') else None),
                                'IsCancelled': 1 if (str(attrs.get('IsCancelled') or '').lower() == 'true') else (0 if (str(attrs.get('IsCancelled') or '').lower() == 'false') else None),
                                'IsCFSRegistered': 1 if (str(attrs.get('IsCFSRegistered') or '').lower() == 'true') else (0 if (str(attrs.get('IsCFSRegistered') or '').lower() == 'false') else None),
                                'IsDirectBooking': 1 if (str(attrs.get('IsDirectBooking') or '').lower() == 'true') else (0 if (str(attrs.get('IsDirectBooking') or '').lower() == 'false') else None),
                                'IsForwardRegistered': 1 if (str(attrs.get('IsForwardRegistered') or '').lower() == 'true') else (0 if (str(attrs.get('IsForwardRegistered') or '').lower() == 'false') else None),
                                'IsHighRisk': 1 if (str(attrs.get('IsHighRisk') or '').lower() == 'true') else (0 if (str(attrs.get('IsHighRisk') or '').lower() == 'false') else None),
                                'IsNeutralMaster': 1 if (str(attrs.get('IsNeutralMaster') or '').lower() == 'true') else (0 if (str(attrs.get('IsNeutralMaster') or '').lower() == 'false') else None),
                                'IsShipping': 1 if (str(attrs.get('IsShipping') or '').lower() == 'true') else (0 if (str(attrs.get('IsShipping') or '').lower() == 'false') else None),
                                'IsSplitShipment': 1 if (str(attrs.get('IsSplitShipment') or '').lower() == 'true') else (0 if (str(attrs.get('IsSplitShipment') or '').lower() == 'false') else None),
                            }
                            col_names_sub = list(cols_sub.keys()) + ['CompanyKey', 'DepartmentKey', 'EventUserKey', 'DataProviderKey']
                            col_vals_sub = list(cols_sub.values()) + [company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')]
                            placeholders_sub = ','.join(['?'] * len(col_names_sub))
                            cur.execute(
                                "INSERT INTO Dwh2.FactSubShipment ([" + "],[".join(col_names_sub) + "]) VALUES (" + placeholders_sub + ")",
                                *col_vals_sub
                            )
                            cur.execute("SELECT SCOPE_IDENTITY()")
                            rsub = cur.fetchone()
                            sub_key2 = int(rsub[0]) if rsub and rsub[0] is not None else None

                            # DateCollection under new SubShipment (AR) -> FactEventDate
                            sub_dates2 = sub_obj.get('DateCollection') or []  # type: ignore[index]
                            if sub_key2 and sub_dates2:
                                # Dedupe intra sub-shipment dates
                                seen_sub3 = set()
                                sub_dates2 = [
                                    d for d in sub_dates2
                                    if not (((d.get('DateTypeCode') or ''), d.get('DateKey'), d.get('Time')) in seen_sub3)
                                    and not seen_sub3.add(((d.get('DateTypeCode') or ''), d.get('DateKey'), d.get('Time')))
                                ]
                                try:
                                    cur.execute("DELETE FROM Dwh2.FactEventDate WHERE FactSubShipmentKey = ? AND Source='AR'", sub_key2)
                                except Exception:
                                    pass
                                try:
                                    if ENABLE_BATCH:
                                        cur.fast_executemany = True
                                except Exception:
                                    pass
                                rows_ed2 = []
                                for r in sub_dates2:
                                    rows_ed2.append((
                                        None,                 # FactShipmentKey must be NULL when SubShipment parent is set
                                        sub_key2,             # FactSubShipmentKey
                                        None,                 # FactAccountsReceivableTransactionKey
                                        'AR',
                                        r.get('DateTypeCode'), r.get('DateTypeDescription'), r.get('DateKey'), r.get('Time'), r.get('DateTimeText'), r.get('IsEstimate'), r.get('Value'), r.get('TimeZone'),
                                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                    ))
                                if rows_ed2:
                                    cur.executemany(
                                        "INSERT INTO Dwh2.FactEventDate (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, DateTypeCode, DateTypeDescription, DateKey, [Time], DateTimeText, IsEstimate, [Value], TimeZone, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                        rows_ed2
                                    )
                                    if counters:
                                        counters.add('Dwh2.FactEventDate', added=len(rows_ed2))
                            # Shipment-level TransportLegs (existing shipment with no subs yet)
                            legs_sh3 = shf.get('TransportLegsShipment') if isinstance(shf, dict) else None
                            if fact_ship_key_exist and legs_sh3:
                                def _norm2(s: Optional[str]) -> str:
                                    return (s or '').strip()
                                def _leg_key2(leg: dict) -> tuple:
                                    pol = ((_norm2((leg.get('PortOfLoading') or ('', ''))[0])).upper())
                                    pod = ((_norm2((leg.get('PortOfDischarge') or ('', ''))[0])).upper())
                                    mode = _norm2(leg.get('TransportMode')).lower()
                                    vessel = _norm2(leg.get('VesselLloydsIMO')) or _norm2(leg.get('VesselName')).upper()
                                    voyage = _norm2(leg.get('VoyageFlightNo')).upper()
                                    order_ = leg.get('Order') or 0
                                    times = (
                                        _norm2(leg.get('ActualArrival')), _norm2(leg.get('ActualDeparture')),
                                        _norm2(leg.get('EstimatedArrival')), _norm2(leg.get('EstimatedDeparture')),
                                        _norm2(leg.get('ScheduledArrival')), _norm2(leg.get('ScheduledDeparture')),
                                    )
                                    return (order_, pol, pod, mode, vessel, voyage) + times
                                seen_keys2: set[tuple] = set()
                                legs_clean3 = [l for l in (legs_sh3 or []) if not (_leg_key2(l) in seen_keys2) and not seen_keys2.add(_leg_key2(l))]
                                try:
                                    cur.execute("DELETE FROM Dwh2.FactTransportLeg WHERE FactShipmentKey = ?", fact_ship_key_exist)
                                except Exception:
                                    pass
                                rows_tl3 = []
                                for leg in legs_clean3:
                                    pol_code, pol_name = leg['PortOfLoading']
                                    pod_code, pod_name = leg['PortOfDischarge']
                                    polk3 = ensure_port(cur, pol_code, pol_name) if pol_code else None
                                    podk3 = ensure_port(cur, pod_code, pod_name) if pod_code else None
                                    bsc, bsd = leg.get('BookingStatus') if leg.get('BookingStatus') else (None, None)
                                    carr_code, carr_name, carr_country_code, carr_port_code = leg.get('Carrier') if leg.get('Carrier') else (None, None, None, None)
                                    cred_code, cred_name, cred_country_code, cred_port_code = leg.get('Creditor') if leg.get('Creditor') else (None, None, None, None)
                                    carr_key3 = ensure_organization_min(cur, carr_code, carr_name, carr_country_code, carr_port_code) if carr_code else None
                                    cred_key3 = ensure_organization_min(cur, cred_code, cred_name, cred_country_code, cred_port_code) if cred_code else None
                                    rows_tl3.append((
                                        fact_ship_key_exist, None, None,
                                        polk3, podk3, leg.get('Order'), leg.get('TransportMode'),
                                        leg.get('VesselName'), leg.get('VesselLloydsIMO'), leg.get('VoyageFlightNo'),
                                        leg.get('CarrierBookingReference'), bsc, bsd,
                                        carr_key3, cred_key3,
                                        _datekey_from_iso(leg.get('ActualArrival')), _time_from_iso(leg.get('ActualArrival')),
                                        _datekey_from_iso(leg.get('ActualDeparture')), _time_from_iso(leg.get('ActualDeparture')),
                                        _datekey_from_iso(leg.get('EstimatedArrival')), _time_from_iso(leg.get('EstimatedArrival')),
                                        _datekey_from_iso(leg.get('EstimatedDeparture')), _time_from_iso(leg.get('EstimatedDeparture')),
                                        _datekey_from_iso(leg.get('ScheduledArrival')), _time_from_iso(leg.get('ScheduledArrival')),
                                        _datekey_from_iso(leg.get('ScheduledDeparture')), _time_from_iso(leg.get('ScheduledDeparture')),
                                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                    ))
                                if rows_tl3:
                                    try:
                                        if ENABLE_BATCH:
                                            cur.fast_executemany = True
                                    except Exception:
                                        pass
                                    cur.executemany(
                                        "INSERT INTO Dwh2.FactTransportLeg (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, PortOfLoadingKey, PortOfDischargeKey, [Order], TransportMode, VesselName, VesselLloydsIMO, VoyageFlightNo, CarrierBookingReference, BookingStatusCode, BookingStatusDescription, CarrierOrganizationKey, CreditorOrganizationKey, ActualArrivalDateKey, ActualArrivalTime, ActualDepartureDateKey, ActualDepartureTime, EstimatedArrivalDateKey, EstimatedArrivalTime, EstimatedDepartureDateKey, EstimatedDepartureTime, ScheduledArrivalDateKey, ScheduledArrivalTime, ScheduledDepartureDateKey, ScheduledDepartureTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                        rows_tl3
                                    )
                            # Transport legs under sub-shipment
                            tlegs2 = sub_obj.get('TransportLegs') or []  # type: ignore[index]
                            if sub_key2 and tlegs2:
                                # Intra-batch de-duplication by a normalized composite key
                                def _norm2(s: Optional[str]) -> str:
                                    return (s or '').strip()
                                def _leg_key2(leg: dict) -> tuple:
                                    pol = ((_norm2((leg.get('PortOfLoading') or ('', ''))[0])).upper())
                                    pod = ((_norm2((leg.get('PortOfDischarge') or ('', ''))[0])).upper())
                                    mode = _norm2(leg.get('TransportMode')).lower()
                                    vessel = _norm2(leg.get('VesselLloydsIMO')) or _norm2(leg.get('VesselName')).upper()
                                    voyage = _norm2(leg.get('VoyageFlightNo')).upper()
                                    order_ = leg.get('Order') or 0
                                    times = (
                                        _norm2(leg.get('ActualArrival')), _norm2(leg.get('ActualDeparture')),
                                        _norm2(leg.get('EstimatedArrival')), _norm2(leg.get('EstimatedDeparture')),
                                        _norm2(leg.get('ScheduledArrival')), _norm2(leg.get('ScheduledDeparture')),
                                    )
                                    return (order_, pol, pod, mode, vessel, voyage) + times
                                seen_keys2: set[tuple] = set()
                                tlegs2 = [
                                    l for l in tlegs2
                                    if not (_leg_key2(l) in seen_keys2) and not seen_keys2.add(_leg_key2(l))
                                ]
                                # Idempotency: clear any previous legs for this SubShipment
                                try:
                                    cur.execute("DELETE FROM Dwh2.FactTransportLeg WHERE FactSubShipmentKey = ?", sub_key2)
                                except Exception:
                                    pass
                                rows = []
                                for leg in tlegs2:
                                    pol_code, pol_name = leg['PortOfLoading']
                                    pod_code, pod_name = leg['PortOfDischarge']
                                    polk2 = ensure_port(cur, pol_code, pol_name) if pol_code else None
                                    podk2 = ensure_port(cur, pod_code, pod_name) if pod_code else None
                                    bsc, bsd = leg['BookingStatus']
                                    carr_code, carr_name, carr_country_code, carr_port_code = leg['Carrier']
                                    cred_code, cred_name, cred_country_code, cred_port_code = leg['Creditor']
                                    carr_key2 = ensure_organization_min(cur, carr_code, carr_name, carr_country_code, carr_port_code) if carr_code else None
                                    cred_key2 = ensure_organization_min(cur, cred_code, cred_name, cred_country_code, cred_port_code) if cred_code else None
                                    rows.append((
                                        None, sub_key2, None,
                                        polk2, podk2, leg.get('Order'), leg.get('TransportMode'),
                                        leg.get('VesselName'), leg.get('VesselLloydsIMO'), leg.get('VoyageFlightNo'),
                                        leg.get('CarrierBookingReference'), bsc, bsd,
                                        carr_key2, cred_key2,
                                        _datekey_from_iso(leg.get('ActualArrival')), _time_from_iso(leg.get('ActualArrival')),
                                        _datekey_from_iso(leg.get('ActualDeparture')), _time_from_iso(leg.get('ActualDeparture')),
                                        _datekey_from_iso(leg.get('EstimatedArrival')), _time_from_iso(leg.get('EstimatedArrival')),
                                        _datekey_from_iso(leg.get('EstimatedDeparture')), _time_from_iso(leg.get('EstimatedDeparture')),
                                        _datekey_from_iso(leg.get('ScheduledArrival')), _time_from_iso(leg.get('ScheduledArrival')),
                                        _datekey_from_iso(leg.get('ScheduledDeparture')), _time_from_iso(leg.get('ScheduledDeparture')),
                                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                    ))
                                if rows:
                                    try:
                                        if ENABLE_BATCH:
                                            cur.fast_executemany = True
                                    except Exception:
                                        pass
                                    cur.executemany(
                                        "INSERT INTO Dwh2.FactTransportLeg (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, PortOfLoadingKey, PortOfDischargeKey, [Order], TransportMode, VesselName, VesselLloydsIMO, VoyageFlightNo, CarrierBookingReference, BookingStatusCode, BookingStatusDescription, CarrierOrganizationKey, CreditorOrganizationKey, ActualArrivalDateKey, ActualArrivalTime, ActualDepartureDateKey, ActualDepartureTime, EstimatedArrivalDateKey, EstimatedArrivalTime, EstimatedDepartureDateKey, EstimatedDepartureTime, ScheduledArrivalDateKey, ScheduledArrivalTime, ScheduledDepartureDateKey, ScheduledDepartureTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                        rows
                                    )

                            # Charge lines under sub-shipment (source AR)
                            charge_lines2 = sub_obj.get('ChargeLines') or []  # type: ignore[index]
                            if sub_key2 and charge_lines2:
                                rows = []
                                for cl in charge_lines2:
                                    br_code, br_name = cl['Branch']
                                    brk2 = ensure_branch(cur, br_code, br_name) if br_code else None
                                    dept_code, dept_name = cl['Department']
                                    dept_key2 = None
                                    if dept_code:
                                        try:
                                            dept_key2 = _upsert_scalar_dim(cur, 'Dwh2.DimDepartment', 'Code', dept_code, 'Name', dept_name or dept_code, key_col='DepartmentKey')
                                        except Exception:
                                            cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimDepartment WHERE Code=?) INSERT INTO Dwh2.DimDepartment (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimDepartment SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", dept_code, dept_code, dept_name or dept_code, dept_name or dept_code, dept_code)
                                            cur.execute("SELECT DepartmentKey FROM Dwh2.DimDepartment WHERE Code=?", dept_code)
                                            r = cur.fetchone(); dept_key2 = int(r[0]) if r else None
                                    chg_code, chg_desc = cl['ChargeCode']
                                    chg_group_code, chg_group_desc = cl['ChargeGroup']
                                    cred_key2 = ensure_organization_min(cur, cl.get('CreditorKey') or '') if cl.get('CreditorKey') else None
                                    debt_key2 = ensure_organization_min(cur, cl.get('DebtorKey') or '') if cl.get('DebtorKey') else None
                                    sell_pt = cl['Sell'].get('PostedTransaction') if cl.get('Sell') else None
                                    rows.append((
                                        None, sub_key2, None,
                                        'AR',
                                        brk2, dept_key2,
                                        chg_code or None,
                                        chg_desc or None,
                                        chg_group_code or None,
                                        chg_group_desc or None,
                                        cl.get('Description'),
                                        int(cl.get('DisplaySequence') or '0') if str(cl.get('DisplaySequence') or '').isdigit() else 0,
                                        cred_key2, debt_key2,
                                        cl['Cost'].get('APInvoiceNumber') if cl.get('Cost') else None,
                                        _datekey_from_iso(cl['Cost'].get('DueDate')) if cl.get('Cost') else None, _time_from_iso(cl['Cost'].get('DueDate')) if cl.get('Cost') else None,
                                        (cl['Cost'].get('ExchangeRate') if cl.get('Cost') else None),
                                        _datekey_from_iso(cl['Cost'].get('InvoiceDate')) if cl.get('Cost') else None, _time_from_iso(cl['Cost'].get('InvoiceDate')) if cl.get('Cost') else None,
                                        1 if (str((cl['Cost'].get('IsPosted') if cl.get('Cost') else '') or '').lower() == 'true') else (0 if (str((cl['Cost'].get('IsPosted') if cl.get('Cost') else '') or '').lower() == 'false') else None),
                                        (cl['Cost'].get('LocalAmount') if cl.get('Cost') else None),
                                        (cl['Cost'].get('OSAmount') if cl.get('Cost') else None),
                                        ensure_currency(cur, *(cl['Cost'].get('OSCurrency') or ('',''))) if cl.get('Cost') and cl['Cost'].get('OSCurrency') and cl['Cost'].get('OSCurrency')[0] else None,
                                        (cl['Cost'].get('OSGSTVATAmount') if cl.get('Cost') else None),
                                        (cl['Sell'].get('ExchangeRate') if cl.get('Sell') else None),
                                        (cl['Sell'].get('GSTVAT') or (None,None))[0] if cl.get('Sell') else None,
                                        (cl['Sell'].get('GSTVAT') or (None,None))[1] if cl.get('Sell') else None,
                                        (cl['Sell'].get('InvoiceType') if cl.get('Sell') else None),
                                        1 if (str((cl['Sell'].get('IsPosted') if cl.get('Sell') else '') or '').lower() == 'true') else (0 if (str((cl['Sell'].get('IsPosted') if cl.get('Sell') else '') or '').lower() == 'false') else None),
                                        (cl['Sell'].get('LocalAmount') if cl.get('Sell') else None),
                                        (cl['Sell'].get('OSAmount') if cl.get('Sell') else None),
                                        ensure_currency(cur, *(cl['Sell'].get('OSCurrency') or ('',''))) if cl.get('Sell') and cl['Sell'].get('OSCurrency') and cl['Sell'].get('OSCurrency')[0] else None,
                                        (cl['Sell'].get('OSGSTVATAmount') if cl.get('Sell') else None),
                                        (sell_pt or (None,None,None,None,None,None))[0] if sell_pt else None,
                                        (sell_pt or (None,None,None,None,None,None))[1] if sell_pt else None,
                                        _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[2]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[2]) if sell_pt else None,
                                        _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[3]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[3]) if sell_pt else None,
                                        _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[4]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[4]) if sell_pt else None,
                                        (sell_pt or (None,None,None,None,None,None))[5] if sell_pt else None,
                                        cl.get('SupplierReference'),
                                        cl.get('SellReference'),
                                        company_key, simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                    ))
                                if rows:
                                    try:
                                        if ENABLE_BATCH:
                                            cur.fast_executemany = True
                                    except Exception:
                                        pass
                                    insert_placeholders = ",".join(["?"] * len(rows[0]))
                                    sql = (
                                        "IF NOT EXISTS (SELECT 1 FROM Dwh2.FactChargeLine WHERE FactSubShipmentKey=? "
                                        "AND [DisplaySequence]=? AND ISNULL([ChargeCode],'') = ISNULL(?,'')) "
                                        "INSERT INTO Dwh2.FactChargeLine ("
                                        "FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, BranchKey, DepartmentKey, "
                                        "ChargeCode, ChargeCodeDescription, ChargeCodeGroup, ChargeCodeGroupDescription, [Description], DisplaySequence, "
                                        "CreditorOrganizationKey, DebtorOrganizationKey, CostAPInvoiceNumber, CostDueDateKey, CostDueTime, CostExchangeRate, CostInvoiceDateKey, CostInvoiceTime, "
                                        "CostIsPosted, CostLocalAmount, CostOSAmount, CostOSCurrencyKey, CostOSGSTVATAmount, "
                                        "SellExchangeRate, SellGSTVATTaxCode, SellGSTVATDescription, SellInvoiceType, SellIsPosted, SellLocalAmount, SellOSAmount, SellOSCurrencyKey, SellOSGSTVATAmount, "
                                        "SellPostedTransactionNumber, SellPostedTransactionType, SellTransactionDateKey, SellTransactionTime, SellDueDateKey, SellDueTime, SellFullyPaidDateKey, SellFullyPaidTime, "
                                        "SellOutstandingAmount, SupplierReference, SellReference, CompanyKey, EventUserKey, DataProviderKey) VALUES (" + insert_placeholders + ")"
                                    )
                                    rows2 = []
                                    for r3 in rows:
                                        display_seq = r3[11]
                                        charge_code = r3[6]
                                        rows2.append((sub_key2, display_seq, charge_code) + r3)
                                    cur.executemany(sql, rows2)

                            # Additional references under sub-shipment
                            add_refs2 = sub_obj.get('AdditionalReferences') or []  # type: ignore[index]
                            if sub_key2 and add_refs2:
                                rows = []
                                for r4 in add_refs2:
                                    coi_code, coi_name = r4.get('CountryOfIssue') or (None, None)
                                    coi_key2 = ensure_country(cur, coi_code, coi_name) if coi_code else None
                                    rows.append((
                                        None, sub_key2, None,
                                        'AR',
                                        r4.get('TypeCode'), r4.get('TypeDescription'), r4.get('ReferenceNumber'), r4.get('ContextInformation'),
                                        coi_key2,
                                        _datekey_from_iso(r4.get('IssueDate')), _time_from_iso(r4.get('IssueDate')),
                                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                    ))
                                try:
                                    if ENABLE_BATCH:
                                        cur.fast_executemany = True
                                except Exception:
                                    pass
                                cur.executemany(
                                    "INSERT INTO Dwh2.FactAdditionalReference (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, TypeCode, TypeDescription, ReferenceNumber, ContextInformation, CountryOfIssueKey, IssueDateKey, IssueTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    rows
                                )

                                # Packing lines under sub-shipment (AR)
                                packs2 = sub_obj.get('PackingLines') or []  # type: ignore[index]
                                if sub_key2 and packs2:
                                    try:
                                        cur.execute("DELETE FROM Dwh2.FactPackingLine WHERE FactSubShipmentKey = ? AND Source = 'AR'", sub_key2)
                                    except Exception:
                                        pass
                                    rows = []
                                    for pl in packs2:
                                        len_code, len_desc = pl.get('LengthUnit') or (None, None)
                                        vol_code, vol_desc = pl.get('VolumeUnit') or (None, None)
                                        wei_code, wei_desc = pl.get('WeightUnit') or (None, None)
                                        pack_code, pack_desc = pl.get('PackType') or (None, None)
                                        lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                                        volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                                        weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                                        packk = ensure_unit(cur, pack_code, pack_desc, 'Packs') if pack_code else None
                                        rows.append((
                                            None, sub_key2, None,
                                            'AR',
                                            pl.get('CommodityCode'), pl.get('CommodityDescription'), pl.get('ContainerLink'), pl.get('ContainerNumber'), pl.get('ContainerPackingOrder'), pl.get('CountryOfOriginCode'), pl.get('DetailedDescription'), pl.get('EndItemNo'), pl.get('ExportReferenceNumber'), pl.get('GoodsDescription'), pl.get('HarmonisedCode'),
                                            pl.get('Height'), pl.get('Length'), pl.get('Width'), lenk,
                                            pl.get('ImportReferenceNumber'), pl.get('ItemNo'),
                                            pl.get('LastKnownCFSStatusCode'), _datekey_from_iso(pl.get('LastKnownCFSStatusDate')), _time_from_iso(pl.get('LastKnownCFSStatusDate')),
                                            pl.get('LinePrice'), pl.get('Link'), pl.get('LoadingMeters'), pl.get('MarksAndNos'), pl.get('OutturnComment'), pl.get('OutturnDamagedQty'), pl.get('OutturnedHeight'), pl.get('OutturnedLength'), pl.get('OutturnedVolume'), pl.get('OutturnedWeight'), pl.get('OutturnedWidth'), pl.get('OutturnPillagedQty'), pl.get('OutturnQty'), pl.get('PackingLineID'), pl.get('PackQty'), packk, pl.get('ReferenceNumber'),
                                            (1 if pl.get('RequiresTemperatureControl') is True else (0 if pl.get('RequiresTemperatureControl') is False else None)),
                                            pl.get('Volume'), volk, pl.get('Weight'), weik,
                                            company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                                        ))
                                    if rows:
                                        try:
                                            if ENABLE_BATCH:
                                                cur.fast_executemany = True
                                        except Exception:
                                            pass
                                        cur.executemany(
                                            "INSERT INTO Dwh2.FactPackingLine (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, CommodityCode, CommodityDescription, ContainerLink, ContainerNumber, ContainerPackingOrder, CountryOfOriginCode, DetailedDescription, EndItemNo, ExportReferenceNumber, GoodsDescription, HarmonisedCode, Height, Length, Width, LengthUnitKey, ImportReferenceNumber, ItemNo, LastKnownCFSStatusCode, LastKnownCFSStatusDateKey, LastKnownCFSStatusTime, LinePrice, [Link], LoadingMeters, MarksAndNos, OutturnComment, OutturnDamagedQty, OutturnedHeight, OutturnedLength, OutturnedVolume, OutturnedWeight, OutturnedWidth, OutturnPillagedQty, OutturnQty, PackingLineID, PackQty, PackTypeUnitKey, ReferenceNumber, RequiresTemperatureControl, Volume, VolumeUnitKey, Weight, WeightUnitKey, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                            rows
                                        )
    # PostingJournalCollection for AR -> FactPostingJournal + Details
    if fact_key and fact.get('PostingJournals'):
        for pj in fact['PostingJournals']:
            h = pj['Header']
            br_code, br_name = h.get('Branch') or ('','')
            brk = ensure_branch(cur, br_code, br_name) if br_code else None
            dept_code, dept_name = h.get('Department') or ('','')
            dept_key = None
            if dept_code:
                try:
                    dept_key = _upsert_scalar_dim(cur, 'Dwh2.DimDepartment', 'Code', dept_code, 'Name', dept_name or dept_code, key_col='DepartmentKey')
                except Exception:
                    cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimDepartment WHERE Code=?) INSERT INTO Dwh2.DimDepartment (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimDepartment SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", dept_code, dept_code, dept_name or dept_code, dept_name or dept_code, dept_code)
                    cur.execute("SELECT DepartmentKey FROM Dwh2.DimDepartment WHERE Code=?", dept_code)
                    r = cur.fetchone(); dept_key = int(r[0]) if r else None
            lcur = ensure_currency(cur, *(h.get('LocalCurrency') or ('',''))) if (h.get('LocalCurrency') and h.get('LocalCurrency')[0]) else None
            ocur = ensure_currency(cur, *(h.get('OSCurrency') or ('',''))) if (h.get('OSCurrency') and h.get('OSCurrency')[0]) else None
            ccur = ensure_currency(cur, *(h.get('ChargeCurrency') or ('',''))) if (h.get('ChargeCurrency') and h.get('ChargeCurrency')[0]) else None
            job_type, job_key_text = h.get('Job') or (None, None)
            job_dim_key = ensure_job(cur, job_type, job_key_text) if job_type and job_key_text else None
            org_key = ensure_organization_min(cur, h.get('OrganizationKey') or '') if h.get('OrganizationKey') else None
            # Insert header with dynamic placeholders
            pj_cols = [
                "FactAccountsReceivableTransactionKey",
                "BranchKey","DepartmentKey","LocalCurrencyKey","OSCurrencyKey","ChargeCurrencyKey",
                "Sequence","Description","ChargeCode","ChargeTypeCode","ChargeTypeDescription","ChargeClassCode","ChargeClassDescription","ChargeCodeDescription",
                "ChargeExchangeRate","ChargeTotalAmount","ChargeTotalExVATAmount",
                "GLAccountCode","GLAccountDescription","GLPostDateKey","GLPostTime",
                "JobDimKey","JobRecognitionDateKey","JobRecognitionTime",
                "LocalAmount","LocalGSTVATAmount","LocalTotalAmount",
                "OrganizationKey",
                "OSAmount","OSGSTVATAmount","OSTotalAmount",
                "RevenueRecognitionType","TaxDateKey","TransactionCategory","TransactionType",
                "VATTaxCode","VATTaxDescription","VATTaxRate","VATTaxTypeCode",
                "IsFinalCharge",
                "CompanyKey","EventUserKey","DataProviderKey"
            ]
            pj_vals = [
                fact_key,
                brk, dept_key, lcur, ocur, ccur,
                (int(h.get('Sequence')) if (h.get('Sequence') or '').isdigit() else None),
                h.get('Description'),
                h.get('ChargeCode'),
                (h.get('ChargeType') or (None,None))[0], (h.get('ChargeType') or (None,None))[1],
                (h.get('ChargeClass') or (None,None))[0], (h.get('ChargeClass') or (None,None))[1],
                h.get('ChargeCodeDescription'),
                h.get('ChargeExchangeRate'), h.get('ChargeTotalAmount'), h.get('ChargeTotalExVATAmount'),
                (h.get('GLAccount') or (None,None))[0], (h.get('GLAccount') or (None,None))[1],
                _datekey_from_iso(h.get('GLPostDate')), _time_from_iso(h.get('GLPostDate')),
                job_dim_key,
                _datekey_from_iso(h.get('JobRecognitionDate')), _time_from_iso(h.get('JobRecognitionDate')),
                h.get('LocalAmount'), h.get('LocalGSTVATAmount'), h.get('LocalTotalAmount'),
                org_key,
                h.get('OSAmount'), h.get('OSGSTVATAmount'), h.get('OSTotalAmount'),
                h.get('RevenueRecognitionType'),
                parse_datekey(h.get('TaxDate') or '') if h.get('TaxDate') else None,
                h.get('TransactionCategory'), h.get('TransactionType'),
                (h.get('VATTaxID') or (None,None,None,None))[0],
                (h.get('VATTaxID') or (None,None,None,None))[1],
                (h.get('VATTaxID') or (None,None,None,None))[2],
                (h.get('VATTaxID') or (None,None,None,None))[3],
                (1 if (str(h.get('IsFinalCharge') or '').lower() == 'true') else (0 if (str(h.get('IsFinalCharge') or '').lower() == 'false') else None)),
                company_key, simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
            ]
            pj_placeholders = ",".join(["?"] * len(pj_vals))
            cols_sql = ", ".join([f"[{c}]" for c in pj_cols])
            # Idempotent insert: unique on (FactAccountsReceivableTransactionKey, Sequence)
            seq_val = pj_vals[pj_cols.index("Sequence")]
            pj_key = None
            if seq_val is not None:
                # Insert if missing, then always fetch the key by (AR, Sequence)
                cur.execute(
                    "IF NOT EXISTS (SELECT 1 FROM Dwh2.FactPostingJournal WHERE FactAccountsReceivableTransactionKey=? AND [Sequence]=?) "
                    + "INSERT INTO Dwh2.FactPostingJournal (" + cols_sql + ") VALUES (" + pj_placeholders + ")",
                    pj_vals[0], seq_val, *pj_vals
                )
                # Retrieve header key deterministically
                cur.execute(
                    "SELECT FactPostingJournalKey FROM Dwh2.FactPostingJournal WHERE FactAccountsReceivableTransactionKey=? AND [Sequence]=?",
                    pj_vals[0], seq_val
                )
                rpk = cur.fetchone()
                pj_key = int(rpk[0]) if rpk else None
                if counters and pj_key:
                    # We can't know if it was newly inserted or pre-existing; count only when newly inserted.
                    # Heuristic: if no details exist yet for this header, consider it a new logical insert for our counters.
                    try:
                        cur.execute("SELECT COUNT(1) FROM Dwh2.FactPostingJournalDetail WHERE FactPostingJournalKey=?", pj_key)
                        cr = cur.fetchone()
                        if cr and int(cr[0]) == 0:
                            counters.add('Dwh2.FactPostingJournal', added=1)
                    except Exception:
                        pass
            else:
                # No sequence -> insert directly, then use SCOPE_IDENTITY in this scope
                pj_sql = "INSERT INTO Dwh2.FactPostingJournal (" + cols_sql + ") VALUES (" + pj_placeholders + ")"
                cur.execute(pj_sql, *pj_vals)
                cur.execute("SELECT SCOPE_IDENTITY()")
                rp = cur.fetchone(); pj_key = int(rp[0]) if rp and rp[0] is not None else None
                if counters and pj_key:
                    counters.add('Dwh2.FactPostingJournal', added=1)
            # Details
            if pj_key and pj.get('Details'):
                # Idempotent details load per header: replace existing
                try:
                    cur.execute("DELETE FROM Dwh2.FactPostingJournalDetail WHERE FactPostingJournalKey=?", pj_key)
                except Exception:
                    pass
                rows = []
                for d in pj['Details']:
                    pcur = ensure_currency(cur, *(d.get('PostingCurrency') or ('',''))) if (d.get('PostingCurrency') and d.get('PostingCurrency')[0]) else None
                    rows.append((
                        pj_key,
                        (d.get('CreditGL') or (None,None))[0], (d.get('CreditGL') or (None,None))[1],
                        (d.get('DebitGL') or (None,None))[0], (d.get('DebitGL') or (None,None))[1],
                        d.get('PostingAmount'), pcur,
                        _datekey_from_iso(d.get('PostingDate')), _time_from_iso(d.get('PostingDate')),
                        d.get('PostingPeriod')
                    ))
                try:
                    if ENABLE_BATCH:
                        cur.fast_executemany = True
                except Exception:
                    pass
                cur.executemany(
                    "INSERT INTO Dwh2.FactPostingJournalDetail (FactPostingJournalKey, CreditGLAccountCode, CreditGLAccountDescription, DebitGLAccountCode, DebitGLAccountDescription, PostingAmount, PostingCurrencyKey, PostingDateKey, PostingTime, PostingPeriod) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    rows
                )
                if counters:
                    counters.add('Dwh2.FactPostingJournalDetail', added=len(rows))


# ---------- CSL (UniversalShipment) minimal ----------

def ensure_port(cur: pyodbc.Cursor, code: str, name: str) -> Optional[int]:
    code = _clean_str(code) or ""
    name = _clean_str(name) or code

    def _country_key_for_port_code(c: str) -> Optional[int]:
        if not c or len(c) < 2:
            return None
        cc = (c[:2] or "").upper()
        if not cc:
            return None
        cur.execute("SELECT CountryKey FROM Dwh2.DimCountry WHERE Code = ?", cc)
        rr = cur.fetchone()
        return int(rr[0]) if rr else None

    ckey = _country_key_for_port_code(code)
    try:
        extra = {"CountryKey": ckey} if ckey is not None else None
        return _upsert_scalar_dim(cur, "Dwh2.DimPort", "Code", code, "Name", name, extra_cols=extra, key_col="PortKey")
    except Exception:
        # Fallback: upsert with CountryKey if available
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimPort] WHERE [Code]=?) "
            "INSERT INTO [Dwh2].[DimPort] ([Code],[Name],[CountryKey]) VALUES (?,?,?); "
            "ELSE UPDATE [Dwh2].[DimPort] SET [Name]=?, [CountryKey]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
            code, code, name, ckey, name, ckey, code,
        )
        cur.execute("SELECT [PortKey] FROM [Dwh2].[DimPort] WHERE [Code]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None


def ensure_service_level(cur: pyodbc.Cursor, code: str, desc: str, sl_type: str) -> Optional[int]:
    code = _clean_str(code) or ""
    desc = _clean_str(desc) or code
    sl_type = _clean_str(sl_type) or ""
    try:
        return _upsert_scalar_dim(cur, "Dwh2.DimServiceLevel", "Code", code, "Description", desc, {"ServiceLevelType": sl_type}, key_col="ServiceLevelKey")
    except Exception:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimServiceLevel] WHERE [Code]=?) "
            "INSERT INTO [Dwh2].[DimServiceLevel] ([Code],[Description],[ServiceLevelType]) VALUES (?,?,?); "
            "ELSE UPDATE [Dwh2].[DimServiceLevel] SET [Description]=?, [ServiceLevelType]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
            code, code, desc, sl_type, desc, sl_type, code,
        )
        cur.execute("SELECT [ServiceLevelKey] FROM [Dwh2].[DimServiceLevel] WHERE [Code]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None


def ensure_unit(cur: pyodbc.Cursor, code: str, desc: str, unit_type: str) -> Optional[int]:
    code = _clean_str(code) or ""
    desc = _clean_str(desc) or code
    unit_type = _clean_str(unit_type) or ""
    try:
        return _upsert_scalar_dim(cur, "Dwh2.DimUnit", "Code", code, "Description", desc, {"UnitType": unit_type}, key_col="UnitKey")
    except Exception:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimUnit] WHERE [Code]=?) "
            "INSERT INTO [Dwh2].[DimUnit] ([Code],[Description],[UnitType]) VALUES (?,?,?); "
            "ELSE UPDATE [Dwh2].[DimUnit] SET [Description]=?, [UnitType]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
            code, code, desc, unit_type, desc, unit_type, code,
        )
        cur.execute("SELECT [UnitKey] FROM [Dwh2].[DimUnit] WHERE [Code]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None


def ensure_payment_method(cur: pyodbc.Cursor, code: str, desc: str) -> Optional[int]:
    code = _clean_str(code) or ""
    desc = _clean_str(desc) or code
    try:
        return _upsert_scalar_dim(cur, "Dwh2.DimPaymentMethod", "Code", code, "Description", desc, key_col="PaymentMethodKey")
    except Exception:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimPaymentMethod] WHERE [Code]=?) "
            "INSERT INTO [Dwh2].[DimPaymentMethod] ([Code],[Description]) VALUES (?,?); "
            "ELSE UPDATE [Dwh2].[DimPaymentMethod] SET [Description]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
            code, code, desc, desc, code,
        )
        cur.execute("SELECT [PaymentMethodKey] FROM [Dwh2].[DimPaymentMethod] WHERE [Code]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None


def ensure_container_mode(cur: pyodbc.Cursor, code: str, desc: str) -> Optional[int]:
    code = _clean_str(code) or ""
    desc = _clean_str(desc) or code
    try:
        return _upsert_scalar_dim(cur, "Dwh2.DimContainerMode", "Code", code, "Description", desc, key_col="ContainerModeKey")
    except Exception:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimContainerMode] WHERE [Code]=?) "
            "INSERT INTO [Dwh2].[DimContainerMode] ([Code],[Description]) VALUES (?,?); "
            "ELSE UPDATE [Dwh2].[DimContainerMode] SET [Description]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
            code, code, desc, desc, code,
        )
        cur.execute("SELECT [ContainerModeKey] FROM [Dwh2].[DimContainerMode] WHERE [Code]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None


def ensure_screening_status(cur: pyodbc.Cursor, code: str, desc: str) -> Optional[int]:
    code = _clean_str(code) or ""
    desc = _clean_str(desc) or code
    try:
        return _upsert_scalar_dim(cur, "Dwh2.DimScreeningStatus", "Code", code, "Description", desc, key_col="ScreeningStatusKey")
    except Exception:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimScreeningStatus] WHERE [Code]=?) "
            "INSERT INTO [Dwh2].[DimScreeningStatus] ([Code],[Description]) VALUES (?,?); "
            "ELSE UPDATE [Dwh2].[DimScreeningStatus] SET [Description]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
            code, code, desc, desc, code,
        )
        cur.execute("SELECT [ScreeningStatusKey] FROM [Dwh2].[DimScreeningStatus] WHERE [Code]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None


def ensure_co2e_status(cur: pyodbc.Cursor, code: str, desc: str) -> Optional[int]:
    code = _clean_str(code) or ""
    desc = _clean_str(desc) or code
    try:
        return _upsert_scalar_dim(cur, "Dwh2.DimCo2eStatus", "Code", code, "Description", desc, key_col="Co2eStatusKey")
    except Exception:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimCo2eStatus] WHERE [Code]=?) "
            "INSERT INTO [Dwh2].[DimCo2eStatus] ([Code],[Description]) VALUES (?,?); "
            "ELSE UPDATE [Dwh2].[DimCo2eStatus] SET [Description]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
            code, code, desc, desc, code,
        )
        cur.execute("SELECT [Co2eStatusKey] FROM [Dwh2].[DimCo2eStatus] WHERE [Code]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None

def ensure_currency(cur: pyodbc.Cursor, code: str, desc: str) -> Optional[int]:
    code = _clean_str(code) or ""
    desc = _clean_str(desc) or code
    try:
        return _upsert_scalar_dim(cur, "Dwh2.DimCurrency", "Code", code, "Description", desc, key_col="CurrencyKey")
    except Exception:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimCurrency] WHERE [Code]=?) "
            "INSERT INTO [Dwh2].[DimCurrency] ([Code],[Description]) VALUES (?,?); "
            "ELSE UPDATE [Dwh2].[DimCurrency] SET [Description]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
            code, code, desc, desc, code,
        )
        cur.execute("SELECT [CurrencyKey] FROM [Dwh2].[DimCurrency] WHERE [Code]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None

def ensure_account_group(cur: pyodbc.Cursor, code: str, desc: str, ag_type: str) -> Optional[int]:
    code = _clean_str(code) or ""
    desc = _clean_str(desc) or code
    ag_type = _clean_str(ag_type) or ""
    try:
        return _upsert_scalar_dim(cur, "Dwh2.DimAccountGroup", "Code", code, "Description", desc, {"Type": ag_type}, key_col="AccountGroupKey")
    except Exception:
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimAccountGroup] WHERE [Code]=?) "
            "INSERT INTO [Dwh2].[DimAccountGroup] ([Code],[Description],[Type]) VALUES (?,?,?); "
            "ELSE UPDATE [Dwh2].[DimAccountGroup] SET [Description]=?, [Type]=?, UpdatedAt=SYSUTCDATETIME() WHERE [Code]=?;",
            code, code, desc, ag_type, desc, ag_type, code,
        )
        cur.execute("SELECT [AccountGroupKey] FROM [Dwh2].[DimAccountGroup] WHERE [Code]=?", code)
        r = cur.fetchone()
        return int(r[0]) if r else None


def ensure_job(cur: pyodbc.Cursor, job_type: str, job_key: str) -> Optional[int]:
    if not job_type or not job_key:
        return None
    # Upsert DimJob (unique on (JobType, JobKey))
    cur.execute("SELECT JobDimKey FROM Dwh2.DimJob WHERE JobType = ? AND JobKey = ?", job_type, job_key)
    r = cur.fetchone()
    if r:
        return int(r[0])
    cur.execute("INSERT INTO Dwh2.DimJob (JobType, JobKey) VALUES (?, ?)", job_type, job_key)
    cur.execute("SELECT JobDimKey FROM Dwh2.DimJob WHERE JobType = ? AND JobKey = ?", job_type, job_key)
    r = cur.fetchone()
    return int(r[0]) if r else None


def parse_csl(path: str) -> Tuple[Dict, Dict]:
    parser = etree.XMLParser(remove_blank_text=False, ns_clean=True)
    doc = etree.parse(path, parser)
    root = doc.getroot()
    sh = root.find("u:Shipment", NS)
    dc = sh.find("u:DataContext", NS) if sh is not None else None

    company = dc.find("u:Company", NS) if dc is not None else None
    company_code = text(company.find("u:Code", NS)) if company is not None else ""
    company_name = text(company.find("u:Name", NS)) if company is not None else ""
    comp_country = company.find("u:Country", NS) if company is not None else None
    comp_country_code = text(comp_country.find("u:Code", NS)) if comp_country is not None else ""
    comp_country_name = text(comp_country.find("u:Name", NS)) if comp_country is not None else ""

    dept = dc.find("u:EventDepartment", NS) if dc is not None else None
    dept_code = text(dept.find("u:Code", NS)) if dept is not None else ""
    dept_name = text(dept.find("u:Name", NS)) if dept is not None else ""

    et = dc.find("u:EventType", NS) if dc is not None else None
    et_code = text(et.find("u:Code", NS)) if et is not None else ""
    et_desc = text(et.find("u:Description", NS)) if et is not None else ""

    ap = dc.find("u:ActionPurpose", NS) if dc is not None else None
    ap_code = text(ap.find("u:Code", NS)) if ap is not None else ""
    ap_desc = text(ap.find("u:Description", NS)) if ap is not None else ""

    usr = dc.find("u:EventUser", NS) if dc is not None else None
    usr_code = text(usr.find("u:Code", NS)) if usr is not None else ""
    usr_name = text(usr.find("u:Name", NS)) if usr is not None else ""

    # Event Branch
    evb = dc.find("u:EventBranch", NS) if dc is not None else None
    evb_code = text(evb.find("u:Code", NS)) if evb is not None else ""
    evb_name = text(evb.find("u:Name", NS)) if evb is not None else ""

    ent_id = text(dc.find("u:EnterpriseID", NS)) if dc is not None else ""
    srv_id = text(dc.find("u:ServerID", NS)) if dc is not None else ""
    provider = text(dc.find("u:DataProvider", NS)) if dc is not None else ""

    trigger_date = text(dc.find("u:TriggerDate", NS)) if dc is not None else ""
    trigger_datekey = parse_datekey(trigger_date)

    # DataSources -> jobs
    consol_job_key = None
    shipment_job_key = None
    dsc = dc.find("u:DataSourceCollection", NS) if dc is not None else None
    if dsc is not None:
        for ds in dsc.findall("u:DataSource", NS):
            ds_type = text(ds.find("u:Type", NS))
            ds_key = text(ds.find("u:Key", NS))
            if ds_type == "ForwardingConsol":
                consol_job_key = ds_key
            elif ds_type == "ForwardingShipment":
                shipment_job_key = ds_key

    # Shipment fields
    def node(path_: str) -> Optional[etree._Element]:
        return sh.find(path_, NS) if sh is not None else None

    def simple_code_name(elem: Optional[etree._Element]) -> Tuple[str, str]:
        return text(elem.find("u:Code", NS)) if elem is not None else "", text(elem.find("u:Name", NS)) if elem is not None else ""

    # Ports
    ports = {
        "PlaceOfDelivery": simple_code_name(node("u:PlaceOfDelivery")),
        "PlaceOfIssue": simple_code_name(node("u:PlaceOfIssue")),
        "PlaceOfReceipt": simple_code_name(node("u:PlaceOfReceipt")),
        "PortFirstForeign": simple_code_name(node("u:PortFirstForeign")),
        "PortLastForeign": simple_code_name(node("u:PortLastForeign")),
        "PortOfDischarge": simple_code_name(node("u:PortOfDischarge")),
        "PortOfFirstArrival": simple_code_name(node("u:PortOfFirstArrival")),
        "PortOfLoading": simple_code_name(node("u:PortOfLoading")),
        "EventBranchHomePort": simple_code_name(node("u:EventBranchHomePort")),
    }

    # Service levels / types etc.
    def simple_code_desc(elem: Optional[etree._Element]) -> Tuple[str, str]:
        return text(elem.find("u:Code", NS)) if elem is not None else "", text(elem.find("u:Description", NS)) if elem is not None else ""

    awb = simple_code_desc(node("u:AWBServiceLevel"))
    gateway = simple_code_desc(node("u:GatewayServiceLevel"))
    shipment_type = simple_code_desc(node("u:ShipmentType"))
    release_type = simple_code_desc(node("u:ReleaseType"))
    screening_status = simple_code_desc(node("u:ScreeningStatus"))
    payment_method = simple_code_desc(node("u:PaymentMethod"))
    currency = simple_code_desc(node("u:FreightRateCurrency"))
    container_mode = simple_code_desc(node("u:ContainerMode"))
    co2e_status = ("", "")
    co2e_unit = ("", "")
    ghg = node("u:GreenhouseGasEmission")
    if ghg is not None:
        cds = ghg.find("u:CO2eDescriptiveStatus", NS)
        co2e_status = simple_code_desc(cds)
        cu = ghg.find("u:CO2eUnit", NS)
        co2e_unit = simple_code_desc(cu)

    # Units
    total_volume_unit = simple_code_desc(node("u:TotalVolumeUnit"))
    total_weight_unit = simple_code_desc(node("u:TotalWeightUnit"))
    packs_unit = simple_code_desc(node("u:TotalNoOfPacksPackageType"))

    # Measures
    def dec(p: str) -> Optional[str]:
        return (text(node(p)) or None)

    measures = {
        "ContainerCount": text(node("u:ContainerCount")) or None,
        "ChargeableRate": dec("u:ChargeableRate"),
        "DocumentedChargeable": dec("u:DocumentedChargeable"),
        "DocumentedVolume": dec("u:DocumentedVolume"),
        "DocumentedWeight": dec("u:DocumentedWeight"),
        "FreightRate": dec("u:FreightRate"),
        "GreenhouseGasEmissionCO2e": text(ghg.find("u:CO2e", NS)) if ghg is not None else None,
        "ManifestedChargeable": dec("u:ManifestedChargeable"),
        "ManifestedVolume": dec("u:ManifestedVolume"),
        "ManifestedWeight": dec("u:ManifestedWeight"),
        "MaximumAllowablePackageHeight": dec("u:MaximumAllowablePackageHeight"),
        "MaximumAllowablePackageLength": dec("u:MaximumAllowablePackageLength"),
        "MaximumAllowablePackageWidth": dec("u:MaximumAllowablePackageWidth"),
        "NoCopyBills": text(node("u:NoCopyBills")) or None,
        "NoOriginalBills": text(node("u:NoOriginalBills")) or None,
        "OuterPacks": text(node("u:OuterPacks")) or None,
        "TotalNoOfPacks": text(node("u:TotalNoOfPacks")) or None,
        "TotalPreallocatedChargeable": dec("u:TotalPreallocatedChargeable"),
        "TotalPreallocatedVolume": dec("u:TotalPreallocatedVolume"),
        "TotalPreallocatedWeight": dec("u:TotalPreallocatedWeight"),
        "TotalVolume": dec("u:TotalVolume"),
        "TotalWeight": dec("u:TotalWeight"),
    }

    flags = {
        "IsCFSRegistered": (text(node("u:IsCFSRegistered")).lower() == "true") if node("u:IsCFSRegistered") is not None else None,
        "IsDirectBooking": (text(node("u:IsDirectBooking")).lower() == "true") if node("u:IsDirectBooking") is not None else None,
        "IsForwardRegistered": (text(node("u:IsForwardRegistered")).lower() == "true") if node("u:IsForwardRegistered") is not None else None,
        "IsHazardous": (text(node("u:IsHazardous")).lower() == "true") if node("u:IsHazardous") is not None else None,
        "IsNeutralMaster": (text(node("u:IsNeutralMaster")).lower() == "true") if node("u:IsNeutralMaster") is not None else None,
        "RequiresTemperatureControl": (text(node("u:RequiresTemperatureControl")).lower() == "true") if node("u:RequiresTemperatureControl") is not None else None,
    }

    # Exceptions
    exceptions = []
    # DateCollection
    date_items = []
    dtc = root.find('.//u:DateCollection', NS)
    if dtc is not None:
        for di in dtc.findall('u:Date', NS):
            dtype = di.find('u:Type', NS)
            # Support both nested Code/Description and plain-text Type
            d_code = None
            d_desc = None
            if dtype is not None:
                d_code_candidate = text(dtype.find('u:Code', NS)) if dtype is not None else ""
                d_desc_candidate = text(dtype.find('u:Description', NS)) if dtype is not None else ""
                if d_code_candidate or d_desc_candidate:
                    d_code = d_code_candidate or None
                    d_desc = d_desc_candidate or None
                else:
                    d_code = text(dtype) or None
                    d_desc = None
            dtxt = text(di.find('u:DateTime', NS)) or None
            is_est_txt = text(di.find('u:IsEstimate', NS)) or ""
            is_est = 1 if is_est_txt.lower() in ("true", "1", "y", "yes") else (0 if is_est_txt.lower() in ("false", "0", "n", "no") else None)
            dval = text(di.find('u:Value', NS)) or None
            tz = text(di.find('u:TimeZone', NS)) or None
            eff_dt = dtxt or (dval if dval and re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", dval) else None)
            date_items.append({
                'Source': 'CSL',
                'DateTypeCode': d_code,
                'DateTypeDescription': d_desc,
                'DateKey': _datekey_from_iso(eff_dt),
                'Time': _time_from_iso(eff_dt),
                'DateTimeText': dtxt,
                'IsEstimate': is_est,
                'Value': dval,
                'TimeZone': tz,
            })
    # DateCollection at SubShipment level
    sub_dates: List[Dict] = []
    for sub in sh.findall('.//u:SubShipment', NS) if sh is not None else []:
        sdc = sub.find('u:DateCollection', NS)
        if sdc is None:
            continue
        for di in sdc.findall('u:Date', NS):
            dtype = di.find('u:Type', NS)
            d_code = None
            d_desc = None
            if dtype is not None:
                d_code_candidate = text(dtype.find('u:Code', NS)) if dtype is not None else ""
                d_desc_candidate = text(dtype.find('u:Description', NS)) if dtype is not None else ""
                if d_code_candidate or d_desc_candidate:
                    d_code = d_code_candidate or None
                    d_desc = d_desc_candidate or None
                else:
                    d_code = text(dtype) or None
                    d_desc = None
            dtxt = text(di.find('u:DateTime', NS)) or None
            is_est_txt = text(di.find('u:IsEstimate', NS)) or ""
            is_est = 1 if is_est_txt.lower() in ("true", "1", "y", "yes") else (0 if is_est_txt.lower() in ("false", "0", "n", "no") else None)
            dval = text(di.find('u:Value', NS)) or None
            tz = text(di.find('u:TimeZone', NS)) or None
            eff_dt = dtxt or (dval if dval and re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", dval) else None)
            sub_dates.append({
                'Source': 'CSL',
                'DateTypeCode': d_code,
                'DateTypeDescription': d_desc,
                'DateKey': _datekey_from_iso(eff_dt),
                'Time': _time_from_iso(eff_dt),
                'DateTimeText': dtxt,
                'IsEstimate': is_est,
                'Value': dval,
                'TimeZone': tz,
            })
    # Exceptions: collect all Exception nodes anywhere in the document
    for ex in root.findall('.//u:Exception', NS):
            code = text(ex.find('u:Code', NS)) or None
            typ = text(ex.find('u:Type', NS)) or None
            sev = text(ex.find('u:Severity', NS)) or None
            status = text(ex.find('u:Status', NS)) or None
            desc = text(ex.find('u:Description', NS)) or None
            is_resolved_txt = text(ex.find('u:IsResolved', NS))
            is_resolved = None if is_resolved_txt == '' else (1 if is_resolved_txt.lower() == 'true' else 0)
            raised = text(ex.find('u:RaisedDate', NS)) or None
            resolved = text(ex.find('u:ResolvedDate', NS)) or None
            # Rich fields (presence may vary in CSL)
            exc_id = text(ex.find('u:ExceptionID', NS)) or None
            actioned_txt = text(ex.find('u:Actioned', NS))
            actioned = None if actioned_txt == '' else (1 if actioned_txt.lower() in ('true','1','y','yes') else 0)
            actioned_date = text(ex.find('u:ActionedDate', NS)) or None
            category = text(ex.find('u:Category', NS)) or None
            ev_date = text(ex.find('u:Date', NS)) or None
            duration_hours_txt = text(ex.find('u:DurationHours', NS)) or None
            try:
                duration_hours = int(duration_hours_txt) if (duration_hours_txt and duration_hours_txt.isdigit()) else None
            except Exception:
                duration_hours = None
            loc_code = text(ex.find('u:Location/u:Code', NS)) or None
            loc_name = text(ex.find('u:Location/u:Name', NS)) or None
            notes = text(ex.find('u:Notes', NS)) or None
            staff_code = text(ex.find('u:Staff/u:Code', NS)) or None
            staff_name = text(ex.find('u:Staff/u:Name', NS)) or None
            exceptions.append({
                'Source': 'CSL',
                'ExceptionId': exc_id,
                'Code': code,
                'Type': typ,
                'Severity': sev,
                'Status': status,
                'Description': desc,
                'IsResolved': is_resolved,
                'Actioned': actioned,
                'ActionedDateKey': _datekey_from_iso(actioned_date),
                'ActionedTime': _time_from_iso(actioned_date),
                'Category': category,
                'EventDateKey': _datekey_from_iso(ev_date),
                'EventTime': _time_from_iso(ev_date),
                'DurationHours': duration_hours,
                'LocationCode': loc_code,
                'LocationName': loc_name,
                'Notes': notes,
                'StaffCode': staff_code,
                'StaffName': staff_name,
                'RaisedDateKey': _datekey_from_iso(raised),
                'RaisedTime': _time_from_iso(raised),
                'ResolvedDateKey': _datekey_from_iso(resolved),
                'ResolvedTime': _time_from_iso(resolved),
            })

    # Message numbers (CSL)
    message_numbers = []
    mnc = root.find(".//u:MessageNumberCollection", NS)
    if mnc is not None:
        for mn in mnc.findall("u:MessageNumber", NS):
            mtype = mn.get("Type") or ""
            mval = (mn.text or "").strip()
            if mval:
                message_numbers.append((mtype, mval))

    # Organizations in CSL: may appear at ShipmentHeader level
    org_list = []
    for org in root.findall(".//u:OrganizationAddress", NS):
        org_code = text(org.find("u:OrganizationCode", NS))
        org_company = text(org.find("u:CompanyName", NS))
        address_type = text(org.find("u:AddressType", NS))
        oc = org.find("u:Country", NS)
        org_country_code = text(oc.find("u:Code", NS)) if oc is not None else ""
        org_country_name = text(oc.find("u:Name", NS)) if oc is not None else ""
        op = org.find("u:Port", NS)
        org_port_code = text(op.find("u:Code", NS)) if op is not None else ""
        org_port_name = text(op.find("u:Name", NS)) if op is not None else ""
        org_list.append({
            "OrganizationCode": org_code,
            "CompanyName": org_company,
            "AddressType": address_type,
            "Country": (org_country_code, org_country_name),
            "Port": (org_port_code, org_port_name),
        })

    # Milestones at header (optional)
    milestones = []
    msc = root.find('.//u:MilestoneCollection', NS)
    if msc is not None:
        for ms in msc.findall('u:Milestone', NS):
            desc = text(ms.find('u:Description', NS)) or None
            code = text(ms.find('u:EventCode', NS)) or None
            seq = text(ms.find('u:Sequence', NS))
            ad = text(ms.find('u:ActualDate', NS)) or None
            ed = text(ms.find('u:EstimatedDate', NS)) or None
            cr = text(ms.find('u:ConditionReference', NS)) or None
            ct = text(ms.find('u:ConditionType', NS)) or None
            milestones.append({
                'Source': 'CSL',
                'Description': desc,
                'EventCode': code,
                'Sequence': int(seq) if seq.isdigit() else None,
                'ActualDateKey': _datekey_from_iso(ad),
                'ActualTime': _time_from_iso(ad),
                'EstimatedDateKey': _datekey_from_iso(ed),
                'EstimatedTime': _time_from_iso(ed),
                'ConditionReference': cr,
                'ConditionType': ct,
            })

    # Notes at header (optional) for CSL
    notes = []
    for nc in root.findall('.//u:NoteCollection', NS):
        content_attr = nc.get('Content') if hasattr(nc, 'get') else None
        for n in nc.findall('u:Note', NS):
            notes.append({
                'Source': 'CSL',
                'Description': text(n.find('u:Description', NS)) or None,
                'IsCustomDescription': (text(n.find('u:IsCustomDescription', NS)).lower() == 'true') if n.find('u:IsCustomDescription', NS) is not None else None,
                'NoteText': text(n.find('u:NoteText', NS)) or None,
                'NoteContextCode': text(n.find('u:NoteContext/u:Code', NS)) or None,
                'NoteContextDescription': text(n.find('u:NoteContext/u:Description', NS)) or None,
                'VisibilityCode': text(n.find('u:Visibility/u:Code', NS)) or None,
                'VisibilityDescription': text(n.find('u:Visibility/u:Description', NS)) or None,
                'Content': content_attr or None,
            })

    # Shipment-level TransportLegCollection
    transport_legs_sh = []
    tlc = root.find('.//u:Shipment/u:TransportLegCollection', NS)
    if tlc is not None:
        for leg in tlc.findall('u:TransportLeg', NS):
            order_txt = text(leg.find('u:LegOrder', NS))
            bs = leg.find('u:BookingStatus', NS)
            transport_legs_sh.append({
                'PortOfLoading': (text(leg.find('u:PortOfLoading/u:Code', NS)), text(leg.find('u:PortOfLoading/u:Name', NS))),
                'PortOfDischarge': (text(leg.find('u:PortOfDischarge/u:Code', NS)), text(leg.find('u:PortOfDischarge/u:Name', NS))),
                'Order': int(order_txt) if order_txt.isdigit() else None,
                'TransportMode': text(leg.find('u:TransportMode', NS)) or None,
                'VesselName': text(leg.find('u:VesselName', NS)) or None,
                'VesselLloydsIMO': text(leg.find('u:VesselLloydsIMO', NS)) or None,
                'VoyageFlightNo': text(leg.find('u:VoyageFlightNo', NS)) or None,
                'CarrierBookingReference': text(leg.find('u:CarrierBookingReference', NS)) or None,
                'BookingStatus': (text(bs.find('u:Code', NS)) if bs is not None else None, text(bs.find('u:Description', NS)) if bs is not None else None),
                'Carrier': (text(leg.find('u:Carrier/u:OrganizationCode', NS)), text(leg.find('u:Carrier/u:CompanyName', NS)), text(leg.find('u:Carrier/u:Country/u:Code', NS)), text(leg.find('u:Carrier/u:Port/u:Code', NS))),
                'Creditor': (text(leg.find('u:Creditor/u:OrganizationCode', NS)), text(leg.find('u:Creditor/u:CompanyName', NS)), text(leg.find('u:Creditor/u:Country/u:Code', NS)), text(leg.find('u:Creditor/u:Port/u:Code', NS))),
                'ActualArrival': text(leg.find('u:ActualArrival', NS)) or None,
                'ActualDeparture': text(leg.find('u:ActualDeparture', NS)) or None,
                'EstimatedArrival': text(leg.find('u:EstimatedArrival', NS)) or None,
                'EstimatedDeparture': text(leg.find('u:EstimatedDeparture', NS)) or None,
                'ScheduledArrival': text(leg.find('u:ScheduledArrival', NS)) or None,
                'ScheduledDeparture': text(leg.find('u:ScheduledDeparture', NS)) or None,
            })

    # SubShipmentCollection with nested TransportLegs, ChargeLines, PackingLines and Containers
    sub_shipments = []
    sub_coll = root.find('.//u:SubShipmentCollection', NS)
    if sub_coll is not None:
        for sub in sub_coll.findall('u:SubShipment', NS):
            def c(path_: str) -> Optional[etree._Element]:
                return sub.find(path_, NS)
            sub_obj: Dict[str, object] = {}
            # SubShipment DateCollection (parsed per sub)
            sub_dates = []
            sdc = sub.find('u:DateCollection', NS)
            if sdc is not None:
                for di in sdc.findall('u:Date', NS):
                    dtype = di.find('u:Type', NS)
                    d_code = None
                    d_desc = None
                    if dtype is not None:
                        d_code_candidate = text(dtype.find('u:Code', NS)) if dtype is not None else ""
                        d_desc_candidate = text(dtype.find('u:Description', NS)) if dtype is not None else ""
                        if d_code_candidate or d_desc_candidate:
                            d_code = d_code_candidate or None
                            d_desc = d_desc_candidate or None
                        else:
                            d_code = text(dtype) or None
                            d_desc = None
                    dtxt = text(di.find('u:DateTime', NS)) or None
                    is_est_txt = text(di.find('u:IsEstimate', NS)) or ""
                    is_est = 1 if is_est_txt.lower() in ("true", "1", "y", "yes") else (0 if is_est_txt.lower() in ("false", "0", "n", "no") else None)
                    dval = text(di.find('u:Value', NS)) or None
                    tz = text(di.find('u:TimeZone', NS)) or None
                    eff_dt = dtxt or (dval if dval and re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", dval) else None)
                    sub_dates.append({
                        'Source': 'CSL',
                        'DateTypeCode': d_code,
                        'DateTypeDescription': d_desc,
                        'DateKey': _datekey_from_iso(eff_dt),
                        'Time': _time_from_iso(eff_dt),
                        'DateTimeText': dtxt,
                        'IsEstimate': is_est,
                        'Value': dval,
                        'TimeZone': tz,
                    })
            sub_obj['DateCollection'] = sub_dates
            # Ports and dims
            def simple_cd(parent: Optional[etree._Element]) -> Tuple[str, str]:
                return (text(parent.find('u:Code', NS)) if parent is not None else '', text(parent.find('u:Description', NS)) if parent is not None else '')
            sub_obj['Ports'] = {
                'PortOfLoading': (text(c('u:PortOfLoading/u:Code')), text(c('u:PortOfLoading/u:Name'))),
                'PortOfDischarge': (text(c('u:PortOfDischarge/u:Code')), text(c('u:PortOfDischarge/u:Name'))),
                'PortOfFirstArrival': (text(c('u:PortOfFirstArrival/u:Code')), text(c('u:PortOfFirstArrival/u:Name'))),
                'PortOfDestination': (text(c('u:PortOfDestination/u:Code')), text(c('u:PortOfDestination/u:Name'))),
                'PortOfOrigin': (text(c('u:PortOfOrigin/u:Code')), text(c('u:PortOfOrigin/u:Name'))),
                'EventBranchHomePort': (text(c('u:EventBranchHomePort/u:Code')), text(c('u:EventBranchHomePort/u:Name'))),
            }
            sub_obj['Dims'] = {
                'ServiceLevel': simple_cd(c('u:ServiceLevel')),
                'ShipmentType': simple_cd(c('u:ShipmentType')),
                'ReleaseType': simple_cd(c('u:ReleaseType')),
                'ContainerMode': simple_cd(c('u:ContainerMode')),
                'FreightRateCurrency': simple_cd(c('u:FreightRateCurrency')),
                'GoodsValueCurrency': simple_cd(c('u:GoodsValueCurrency')),
                'InsuranceValueCurrency': simple_cd(c('u:InsuranceValueCurrency')),
                'TotalVolumeUnit': simple_cd(c('u:TotalVolumeUnit')),
                'TotalWeightUnit': simple_cd(c('u:TotalWeightUnit')),
                'PacksUnit': simple_cd(c('u:TotalNoOfPacksPackageType')),
                'CO2eUnit': simple_cd(c('u:GreenhouseGasEmission/u:CO2eUnit')),
            }
            # Measures/flags/attrs
            def dec_child(tag: str) -> Optional[str]:
                val = text(c(tag))
                return val if val != '' else None
            sub_obj['Attrs'] = {
                'WayBillNumber': dec_child('u:WayBillNumber'),
                'WayBillType': (text(c('u:WayBillType/u:Code')) or None, text(c('u:WayBillType/u:Description')) or None),
                'VesselName': dec_child('u:VesselName'),
                'VoyageFlightNo': dec_child('u:VoyageFlightNo'),
                'LloydsIMO': dec_child('u:LloydsIMO'),
                'TransportMode': dec_child('u:TransportMode/u:Description') or dec_child('u:TransportMode'),
                'ContainerCount': text(c('u:ContainerCount')),
                'ActualChargeable': dec_child('u:ActualChargeable'),
                'DocumentedChargeable': dec_child('u:DocumentedChargeable'),
                'DocumentedVolume': dec_child('u:DocumentedVolume'),
                'DocumentedWeight': dec_child('u:DocumentedWeight'),
                'GoodsValue': dec_child('u:GoodsValue'),
                'InsuranceValue': dec_child('u:InsuranceValue'),
                'FreightRate': dec_child('u:FreightRate'),
                'TotalVolume': dec_child('u:TotalVolume'),
                'TotalWeight': dec_child('u:TotalWeight'),
                'TotalNoOfPacks': text(c('u:TotalNoOfPacks')),
                'OuterPacks': text(c('u:OuterPacks')),
                'GreenhouseGasEmissionCO2e': text(c('u:GreenhouseGasEmission/u:CO2e')) or None,
                'IsBooking': text(c('u:IsBooking')),
                'IsCancelled': text(c('u:IsCancelled')),
                'IsCFSRegistered': text(c('u:IsCFSRegistered')),
                'IsDirectBooking': text(c('u:IsDirectBooking')),
                'IsForwardRegistered': text(c('u:IsForwardRegistered')),
                'IsHighRisk': text(c('u:IsHighRisk')),
                'IsNeutralMaster': text(c('u:IsNeutralMaster')),
                'IsShipping': text(c('u:IsShipping')),
                'IsSplitShipment': text(c('u:IsSplitShipment')),
            }
            # Nested TransportLegs
            tlegs = []
            tlc2 = sub.find('u:TransportLegCollection', NS)
            if tlc2 is not None:
                for leg in tlc2.findall('u:TransportLeg', NS):
                    order_txt = text(leg.find('u:LegOrder', NS))
                    bs = leg.find('u:BookingStatus', NS)
                    tlegs.append({
                        'PortOfLoading': (text(leg.find('u:PortOfLoading/u:Code', NS)), text(leg.find('u:PortOfLoading/u:Name', NS))),
                        'PortOfDischarge': (text(leg.find('u:PortOfDischarge/u:Code', NS)), text(leg.find('u:PortOfDischarge/u:Name', NS))),
                        'Order': int(order_txt) if order_txt.isdigit() else None,
                        'TransportMode': text(leg.find('u:TransportMode', NS)) or None,
                        'VesselName': text(leg.find('u:VesselName', NS)) or None,
                        'VesselLloydsIMO': text(leg.find('u:VesselLloydsIMO', NS)) or None,
                        'VoyageFlightNo': text(leg.find('u:VoyageFlightNo', NS)) or None,
                        'CarrierBookingReference': text(leg.find('u:CarrierBookingReference', NS)) or None,
                        'BookingStatus': (text(bs.find('u:Code', NS)) if bs is not None else None, text(bs.find('u:Description', NS)) if bs is not None else None),
                        'Carrier': (text(leg.find('u:Carrier/u:OrganizationCode', NS)), text(leg.find('u:Carrier/u:CompanyName', NS)), text(leg.find('u:Carrier/u:Country/u:Code', NS)), text(leg.find('u:Carrier/u:Port/u:Code', NS))),
                        'Creditor': (text(leg.find('u:Creditor/u:OrganizationCode', NS)), text(leg.find('u:Creditor/u:CompanyName', NS)), text(leg.find('u:Creditor/u:Country/u:Code', NS)), text(leg.find('u:Creditor/u:Port/u:Code', NS))),
                        'ActualArrival': text(leg.find('u:ActualArrival', NS)) or None,
                        'ActualDeparture': text(leg.find('u:ActualDeparture', NS)) or None,
                        'EstimatedArrival': text(leg.find('u:EstimatedArrival', NS)) or None,
                        'EstimatedDeparture': text(leg.find('u:EstimatedDeparture', NS)) or None,
                        'ScheduledArrival': text(leg.find('u:ScheduledArrival', NS)) or None,
                        'ScheduledDeparture': text(leg.find('u:ScheduledDeparture', NS)) or None,
                    })
            sub_obj['TransportLegs'] = tlegs
            # Nested ChargeLines and JobCosting header (JobCosting)
            charge_lines = []
            jc = sub.find('u:JobCosting', NS)
            if jc is not None:
                # JobCosting header (for FactJobCosting)
                sub_obj['JobCostingHeader'] = {
                    'Branch': (text(jc.find('u:Branch/u:Code', NS)) or None, text(jc.find('u:Branch/u:Name', NS)) or None),
                    'Department': (text(jc.find('u:Department/u:Code', NS)) or None, text(jc.find('u:Department/u:Name', NS)) or None),
                    'HomeBranch': (text(jc.find('u:HomeBranch/u:Code', NS)) or None, text(jc.find('u:HomeBranch/u:Name', NS)) or None),
                    'Currency': (text(jc.find('u:Currency/u:Code', NS)) or None, text(jc.find('u:Currency/u:Description', NS)) or None),
                    'OperationsStaff': (text(jc.find('u:OperationsStaff/u:Code', NS)) or None, text(jc.find('u:OperationsStaff/u:Name', NS)) or None),
                    'ClientContractNumber': text(jc.find('u:ClientContractNumber', NS)) or None,
                    'AccrualNotRecognized': text(jc.find('u:AccrualNotRecognized', NS)) or None,
                    'AccrualRecognized': text(jc.find('u:AccrualRecognized', NS)) or None,
                    'AgentRevenue': text(jc.find('u:AgentRevenue', NS)) or None,
                    'LocalClientRevenue': text(jc.find('u:LocalClientRevenue', NS)) or None,
                    'OtherDebtorRevenue': text(jc.find('u:OtherDebtorRevenue', NS)) or None,
                    'TotalAccrual': text(jc.find('u:TotalAccrual', NS)) or None,
                    'TotalCost': text(jc.find('u:TotalCost', NS)) or None,
                    'TotalJobProfit': text(jc.find('u:TotalJobProfit', NS)) or None,
                    'TotalRevenue': text(jc.find('u:TotalRevenue', NS)) or None,
                    'TotalWIP': text(jc.find('u:TotalWIP', NS)) or None,
                    'WIPNotRecognized': text(jc.find('u:WIPNotRecognized', NS)) or None,
                    'WIPRecognized': text(jc.find('u:WIPRecognized', NS)) or None,
                }
                clc = jc.find('u:ChargeLineCollection', NS)
                if clc is not None:
                    for cl in clc.findall('u:ChargeLine', NS):
                        charge_lines.append({
                            'Branch': (text(cl.find('u:Branch/u:Code', NS)), text(cl.find('u:Branch/u:Name', NS))),
                            'Department': (text(cl.find('u:Department/u:Code', NS)), text(cl.find('u:Department/u:Name', NS))),
                            'ChargeCode': (text(cl.find('u:ChargeCode/u:Code', NS)), text(cl.find('u:ChargeCode/u:Description', NS))),
                            'ChargeGroup': (text(cl.find('u:ChargeCodeGroup/u:Code', NS)), text(cl.find('u:ChargeCodeGroup/u:Description', NS))),
                            'Description': text(cl.find('u:Description', NS)) or None,
                            'DisplaySequence': text(cl.find('u:DisplaySequence', NS)) or '',
                            'CreditorKey': text(cl.find('u:Creditor/u:Key', NS)) or '',
                            'DebtorKey': text(cl.find('u:Debtor/u:Key', NS)) or '',
                            'Cost': {
                                'APInvoiceNumber': text(cl.find('u:CostAPInvoiceNumber', NS)) or None,
                                'DueDate': text(cl.find('u:CostDueDate', NS)) or None,
                                'ExchangeRate': text(cl.find('u:CostExchangeRate', NS)) or None,
                                'InvoiceDate': text(cl.find('u:CostInvoiceDate', NS)) or None,
                                'IsPosted': text(cl.find('u:CostIsPosted', NS)) or None,
                                'LocalAmount': text(cl.find('u:CostLocalAmount', NS)) or None,
                                'OSAmount': text(cl.find('u:CostOSAmount', NS)) or None,
                                'OSCurrency': (text(cl.find('u:CostOSCurrency/u:Code', NS)), text(cl.find('u:CostOSCurrency/u:Description', NS))),
                                'OSGSTVATAmount': text(cl.find('u:CostOSGSTVATAmount', NS)) or None,
                            },
                            'Sell': {
                                'ExchangeRate': text(cl.find('u:SellExchangeRate', NS)) or None,
                                'GSTVAT': (text(cl.find('u:SellGSTVATID/u:TaxCode', NS)) or None, text(cl.find('u:SellGSTVATID/u:Description', NS)) or None),
                                'InvoiceType': text(cl.find('u:SellInvoiceType', NS)) or None,
                                'IsPosted': text(cl.find('u:SellIsPosted', NS)) or None,
                                'LocalAmount': text(cl.find('u:SellLocalAmount', NS)) or None,
                                'OSAmount': text(cl.find('u:SellOSAmount', NS)) or None,
                                'OSCurrency': (text(cl.find('u:SellOSCurrency/u:Code', NS)), text(cl.find('u:SellOSCurrency/u:Description', NS))),
                                'OSGSTVATAmount': text(cl.find('u:SellOSGSTVATAmount', NS)) or None,
                                'PostedTransaction': (
                                    text(cl.find('u:SellPostedTransaction/u:Number', NS)) or None,
                                    text(cl.find('u:SellPostedTransaction/u:TransactionType', NS)) or None,
                                    text(cl.find('u:SellPostedTransaction/u:TransactionDate', NS)) or None,
                                    text(cl.find('u:SellPostedTransaction/u:DueDate', NS)) or None,
                                    text(cl.find('u:SellPostedTransaction/u:FullyPaidDate', NS)) or None,
                                    text(cl.find('u:SellPostedTransaction/u:OutstandingAmount', NS)) or None,
                                ),
                            },
                            'SupplierReference': text(cl.find('u:SupplierReference', NS)) or None,
                            'SellReference': text(cl.find('u:SellReference', NS)) or None,
                        })
            sub_obj['ChargeLines'] = charge_lines
            # Nested PackingLineCollection
            packs = []
            plc = sub.find('u:PackingLineCollection', NS)
            if plc is not None:
                for pl in plc.findall('u:PackingLine', NS):
                    packs.append({
                        'CommodityCode': text(pl.find('u:Commodity/u:Code', NS)) or None,
                        'CommodityDescription': text(pl.find('u:Commodity/u:Description', NS)) or None,
                        'ContainerLink': (int(text(pl.find('u:ContainerLink', NS))) if text(pl.find('u:ContainerLink', NS)).isdigit() else None),
                        'ContainerNumber': text(pl.find('u:ContainerNumber', NS)) or None,
                        'ContainerPackingOrder': (int(text(pl.find('u:ContainerPackingOrder', NS))) if text(pl.find('u:ContainerPackingOrder', NS)).isdigit() else None),
                        'CountryOfOriginCode': text(pl.find('u:CountryOfOrigin/u:Code', NS)) or None,
                        'DetailedDescription': text(pl.find('u:DetailedDescription', NS)) or None,
                        'EndItemNo': (int(text(pl.find('u:EndItemNo', NS))) if text(pl.find('u:EndItemNo', NS)).isdigit() else None),
                        'ExportReferenceNumber': text(pl.find('u:ExportReferenceNumber', NS)) or None,
                        'GoodsDescription': text(pl.find('u:GoodsDescription', NS)) or None,
                        'HarmonisedCode': text(pl.find('u:HarmonisedCode', NS)) or None,
                        'Height': text(pl.find('u:Height', NS)) or None,
                        'ImportReferenceNumber': text(pl.find('u:ImportReferenceNumber', NS)) or None,
                        'ItemNo': (int(text(pl.find('u:ItemNo', NS))) if text(pl.find('u:ItemNo', NS)).isdigit() else None),
                        'LastKnownCFSStatusCode': text(pl.find('u:LastKnownCFSStatus/u:Code', NS)) or None,
                        'LastKnownCFSStatusDate': text(pl.find('u:LastKnownCFSStatusDate', NS)) or None,
                        'Length': text(pl.find('u:Length', NS)) or None,
                        'LengthUnit': (text(pl.find('u:LengthUnit/u:Code', NS)) or None, text(pl.find('u:LengthUnit/u:Description', NS)) or None),
                        'LinePrice': text(pl.find('u:LinePrice', NS)) or None,
                        'Link': (int(text(pl.find('u:Link', NS))) if text(pl.find('u:Link', NS)).isdigit() else None),
                        'LoadingMeters': text(pl.find('u:LoadingMeters', NS)) or None,
                        'MarksAndNos': text(pl.find('u:MarksAndNos', NS)) or None,
                        'OutturnComment': text(pl.find('u:OutturnComment', NS)) or None,
                        'OutturnDamagedQty': (int(text(pl.find('u:OutturnDamagedQty', NS))) if text(pl.find('u:OutturnDamagedQty', NS)).isdigit() else None),
                        'OutturnedHeight': text(pl.find('u:OutturnedHeight', NS)) or None,
                        'OutturnedLength': text(pl.find('u:OutturnedLength', NS)) or None,
                        'OutturnedVolume': text(pl.find('u:OutturnedVolume', NS)) or None,
                        'OutturnedWeight': text(pl.find('u:OutturnedWeight', NS)) or None,
                        'OutturnedWidth': text(pl.find('u:OutturnedWidth', NS)) or None,
                        'OutturnPillagedQty': (int(text(pl.find('u:OutturnPillagedQty', NS))) if text(pl.find('u:OutturnPillagedQty', NS)).isdigit() else None),
                        'OutturnQty': (int(text(pl.find('u:OutturnQty', NS))) if text(pl.find('u:OutturnQty', NS)).isdigit() else None),
                        'PackingLineID': text(pl.find('u:PackingLineID', NS)) or None,
                        'PackQty': (int(text(pl.find('u:PackQty', NS))) if text(pl.find('u:PackQty', NS)).isdigit() else None),
                        'PackType': (text(pl.find('u:PackType/u:Code', NS)) or None, text(pl.find('u:PackType/u:Description', NS)) or None),
                        'ReferenceNumber': text(pl.find('u:ReferenceNumber', NS)) or None,
                        'RequiresTemperatureControl': ((text(pl.find('u:RequiresTemperatureControl', NS)).lower() == 'true') if pl.find('u:RequiresTemperatureControl', NS) is not None else None),
                        'Volume': text(pl.find('u:Volume', NS)) or None,
                        'VolumeUnit': (text(pl.find('u:VolumeUnit/u:Code', NS)) or None, text(pl.find('u:VolumeUnit/u:Description', NS)) or None),
                        'Weight': text(pl.find('u:Weight', NS)) or None,
                        'WeightUnit': (text(pl.find('u:WeightUnit/u:Code', NS)) or None, text(pl.find('u:WeightUnit/u:Description', NS)) or None),
                        'Width': text(pl.find('u:Width', NS)) or None,
                    })
            sub_obj['PackingLines'] = packs
            # Nested ContainerCollection
            conts = []
            cc = sub.find('u:ContainerCollection', NS)
            if cc is not None:
                for ctn in cc.findall('u:Container', NS):
                    def dec(tag: str) -> Optional[str]:
                        return text(ctn.find(tag, NS)) or None
                    def bool_opt(tag: str) -> Optional[bool]:
                        el = ctn.find(tag, NS)
                        return (text(el).lower() == 'true') if el is not None and text(el) != '' else None
                    conts.append({
                        'ContainerJobID': dec('u:ContainerJobID'),
                        'ContainerNumber': dec('u:ContainerNumber'),
                        'Link': (int(text(ctn.find('u:Link', NS))) if text(ctn.find('u:Link', NS)).isdigit() else None),
                        'ContainerType': (
                            text(ctn.find('u:ContainerType/u:Code', NS)) or None,
                            text(ctn.find('u:ContainerType/u:Description', NS)) or None,
                            text(ctn.find('u:ContainerType/u:ISOCode', NS)) or None,
                            text(ctn.find('u:ContainerType/u:Category/u:Code', NS)) or None,
                            text(ctn.find('u:ContainerType/u:Category/u:Description', NS)) or None,
                        ),
                        'DeliveryMode': dec('u:DeliveryMode'),
                        'FCL_LCL_AIR': (
                            text(ctn.find('u:FCL_LCL_AIR/u:Code', NS)) or None,
                            text(ctn.find('u:FCL_LCL_AIR/u:Description', NS)) or None,
                        ),
                        'ContainerCount': (int(text(ctn.find('u:ContainerCount', NS))) if text(ctn.find('u:ContainerCount', NS)).isdigit() else None),
                        'Seal': dec('u:Seal'),
                        'SealPartyTypeCode': text(ctn.find('u:SealPartyType/u:Code', NS)) or None,
                        'SecondSeal': dec('u:SecondSeal'),
                        'SecondSealPartyTypeCode': text(ctn.find('u:SecondSealPartyType/u:Code', NS)) or None,
                        'ThirdSeal': dec('u:ThirdSeal'),
                        'ThirdSealPartyTypeCode': text(ctn.find('u:ThirdSealPartyType/u:Code', NS)) or None,
                        'StowagePosition': dec('u:StowagePosition'),
                        'LengthUnit': (text(ctn.find('u:LengthUnit/u:Code', NS)) or None, text(ctn.find('u:LengthUnit/u:Description', NS)) or None),
                        'VolumeUnit': (text(ctn.find('u:VolumeUnit/u:Code', NS)) or None, text(ctn.find('u:VolumeUnit/u:Description', NS)) or None),
                        'WeightUnit': (text(ctn.find('u:WeightUnit/u:Code', NS)) or None, text(ctn.find('u:WeightUnit/u:Description', NS)) or None),
                        'TotalHeight': dec('u:TotalHeight'),
                        'TotalLength': dec('u:TotalLength'),
                        'TotalWidth': dec('u:TotalWidth'),
                        'TareWeight': dec('u:TareWeight'),
                        'GrossWeight': dec('u:GrossWeight'),
                        'GoodsWeight': dec('u:GoodsWeight'),
                        'VolumeCapacity': dec('u:VolumeCapacity'),
                        'WeightCapacity': dec('u:WeightCapacity'),
                        'DunnageWeight': dec('u:DunnageWeight'),
                        'OverhangBack': dec('u:OverhangBack'),
                        'OverhangFront': dec('u:OverhangFront'),
                        'OverhangHeight': dec('u:OverhangHeight'),
                        'OverhangLeft': dec('u:OverhangLeft'),
                        'OverhangRight': dec('u:OverhangRight'),
                        'HumidityPercent': (int(text(ctn.find('u:HumidityPercent', NS))) if text(ctn.find('u:HumidityPercent', NS)).isdigit() else None),
                        'AirVentFlow': dec('u:AirVentFlow'),
                        'AirVentFlowRateUnitCode': text(ctn.find('u:AirVentFlowRateUnit/u:Code', NS)) or None,
                        'NonOperatingReefer': bool_opt('u:NonOperatingReefer'),
                        'IsCFSRegistered': bool_opt('u:IsCFSRegistered'),
                        'IsControlledAtmosphere': bool_opt('u:IsControlledAtmosphere'),
                        'IsDamaged': bool_opt('u:IsDamaged'),
                        'IsEmptyContainer': bool_opt('u:IsEmptyContainer'),
                        'IsSealOk': bool_opt('u:IsSealOk'),
                        'IsShipperOwned': bool_opt('u:IsShipperOwned'),
                        'ArrivalPickupByRail': bool_opt('u:ArrivalPickupByRail'),
                        'DepartureDeliveryByRail': bool_opt('u:DepartureDeliveryByRail'),
                        'ArrivalSlotDateTime': dec('u:ArrivalSlotDateTime'),
                        'DepartureSlotDateTime': dec('u:DepartureSlotDateTime'),
                        'EmptyReadyForReturn': dec('u:EmptyReadyForReturn'),
                        'FCLWharfGateIn': dec('u:FCLWharfGateIn'),
                        'FCLWharfGateOut': dec('u:FCLWharfGateOut'),
                        'FCLStorageCommences': dec('u:FCLStorageCommences'),
                        'LCLUnpack': dec('u:LCLUnpack'),
                        'PackDate': dec('u:PackDate'),
                    })
            sub_obj['Containers'] = conts
            # SubShipment AdditionalReferenceCollection
            add_refs = []
            arc2 = sub.find('u:AdditionalReferenceCollection', NS)
            if arc2 is not None:
                for ar_ in arc2.findall('u:AdditionalReference', NS):
                    t = ar_.find('u:Type', NS)
                    coi = ar_.find('u:CountryOfIssue', NS)
                    issue = text(ar_.find('u:IssueDate', NS)) or None
                    add_refs.append({
                        'Source': 'CSL',
                        'TypeCode': text(t.find('u:Code', NS)) if t is not None else None,
                        'TypeDescription': text(t.find('u:Description', NS)) if t is not None else None,
                        'ReferenceNumber': text(ar_.find('u:ReferenceNumber', NS)) or None,
                        'ContextInformation': text(ar_.find('u:ContextInformation', NS)) or None,
                        'CountryOfIssue': (text(coi.find('u:Code', NS)) if coi is not None else None, text(coi.find('u:Name', NS)) if coi is not None else None),
                        'IssueDate': issue,
                    })
            sub_obj['AdditionalReferences'] = add_refs
            # SubShipment MilestoneCollection
            ms_list = []
            msc_sub = sub.find('u:MilestoneCollection', NS)
            if msc_sub is not None:
                for ms in msc_sub.findall('u:Milestone', NS):
                    desc = text(ms.find('u:Description', NS)) or None
                    code = text(ms.find('u:EventCode', NS)) or None
                    seq = text(ms.find('u:Sequence', NS))
                    ad = text(ms.find('u:ActualDate', NS)) or None
                    ed = text(ms.find('u:EstimatedDate', NS)) or None
                    cr = text(ms.find('u:ConditionReference', NS)) or None
                    ct = text(ms.find('u:ConditionType', NS)) or None
                    ms_list.append({
                        'Source': 'CSL',
                        'Description': desc,
                        'EventCode': code,
                        'Sequence': int(seq) if seq.isdigit() else None,
                        'ActualDateKey': _datekey_from_iso(ad),
                        'ActualTime': _time_from_iso(ad),
                        'EstimatedDateKey': _datekey_from_iso(ed),
                        'EstimatedTime': _time_from_iso(ed),
                        'ConditionReference': cr,
                        'ConditionType': ct,
                    })
            sub_obj['Milestones'] = ms_list
            sub_shipments.append(sub_obj)

    dims = {
        "Country": (comp_country_code, comp_country_name),
        "Company": (company_code, company_name, comp_country_code),
        "Branch": (evb_code, evb_name),
        "Department": (dept_code, dept_name),
        "EventType": (et_code, et_desc),
        "ActionPurpose": (ap_code, ap_desc),
        "User": (usr_code, usr_name),
        "Enterprise": (ent_id,),
        "Server": (srv_id,),
        "DataProvider": (provider,),
        "Organizations": org_list,
    }
    fact = {
        "TriggerDateKey": trigger_datekey,
        "ConsolJob": ("ForwardingConsol", consol_job_key),
        "ShipmentJob": ("ForwardingShipment", shipment_job_key),
        "Ports": ports,
        "AWB": awb,
        "Gateway": gateway,
        "ShipmentType": shipment_type,
        "ReleaseType": release_type,
        "ScreeningStatus": screening_status,
        "PaymentMethod": payment_method,
        "Currency": currency,
        "ContainerMode": container_mode,
        "CO2eStatus": co2e_status,
        "CO2eUnit": co2e_unit,
        "TotalVolumeUnit": total_volume_unit,
        "TotalWeightUnit": total_weight_unit,
        "PacksUnit": packs_unit,
        "Measures": measures,
    "Flags": flags,
    "MessageNumbers": message_numbers,
    "Exceptions": exceptions,
    "DateCollection": date_items,
    "Milestones": milestones,
    "Notes": notes,
    "TransportLegsShipment": transport_legs_sh,
    "SubShipments": sub_shipments,
    }
    # Shipment-level PackingLineCollection
    packs_sh = []
    plc_sh = root.find('.//u:Shipment/u:PackingLineCollection', NS)
    if plc_sh is not None:
        for pl in plc_sh.findall('u:PackingLine', NS):
            packs_sh.append({
                'CommodityCode': text(pl.find('u:Commodity/u:Code', NS)) or None,
                'CommodityDescription': text(pl.find('u:Commodity/u:Description', NS)) or None,
                'ContainerLink': (int(text(pl.find('u:ContainerLink', NS))) if text(pl.find('u:ContainerLink', NS)).isdigit() else None),
                'ContainerNumber': text(pl.find('u:ContainerNumber', NS)) or None,
                'ContainerPackingOrder': (int(text(pl.find('u:ContainerPackingOrder', NS))) if text(pl.find('u:ContainerPackingOrder', NS)).isdigit() else None),
                'CountryOfOriginCode': text(pl.find('u:CountryOfOrigin/u:Code', NS)) or None,
                'DetailedDescription': text(pl.find('u:DetailedDescription', NS)) or None,
                'EndItemNo': (int(text(pl.find('u:EndItemNo', NS))) if text(pl.find('u:EndItemNo', NS)).isdigit() else None),
                'ExportReferenceNumber': text(pl.find('u:ExportReferenceNumber', NS)) or None,
                'GoodsDescription': text(pl.find('u:GoodsDescription', NS)) or None,
                'HarmonisedCode': text(pl.find('u:HarmonisedCode', NS)) or None,
                'Height': text(pl.find('u:Height', NS)) or None,
                'ImportReferenceNumber': text(pl.find('u:ImportReferenceNumber', NS)) or None,
                'ItemNo': (int(text(pl.find('u:ItemNo', NS))) if text(pl.find('u:ItemNo', NS)).isdigit() else None),
                'LastKnownCFSStatusCode': text(pl.find('u:LastKnownCFSStatus/u:Code', NS)) or None,
                'LastKnownCFSStatusDate': text(pl.find('u:LastKnownCFSStatusDate', NS)) or None,
                'Length': text(pl.find('u:Length', NS)) or None,
                'LengthUnit': (text(pl.find('u:LengthUnit/u:Code', NS)) or None, text(pl.find('u:LengthUnit/u:Description', NS)) or None),
                'LinePrice': text(pl.find('u:LinePrice', NS)) or None,
                'Link': (int(text(pl.find('u:Link', NS))) if text(pl.find('u:Link', NS)).isdigit() else None),
                'LoadingMeters': text(pl.find('u:LoadingMeters', NS)) or None,
                'MarksAndNos': text(pl.find('u:MarksAndNos', NS)) or None,
                'OutturnComment': text(pl.find('u:OutturnComment', NS)) or None,
                'OutturnDamagedQty': (int(text(pl.find('u:OutturnDamagedQty', NS))) if text(pl.find('u:OutturnDamagedQty', NS)).isdigit() else None),
                'OutturnedHeight': text(pl.find('u:OutturnedHeight', NS)) or None,
                'OutturnedLength': text(pl.find('u:OutturnedLength', NS)) or None,
                'OutturnedVolume': text(pl.find('u:OutturnedVolume', NS)) or None,
                'OutturnedWeight': text(pl.find('u:OutturnedWeight', NS)) or None,
                'OutturnedWidth': text(pl.find('u:OutturnedWidth', NS)) or None,
                'OutturnPillagedQty': (int(text(pl.find('u:OutturnPillagedQty', NS))) if text(pl.find('u:OutturnPillagedQty', NS)).isdigit() else None),
                'OutturnQty': (int(text(pl.find('u:OutturnQty', NS))) if text(pl.find('u:OutturnQty', NS)).isdigit() else None),
                'PackingLineID': text(pl.find('u:PackingLineID', NS)) or None,
                'PackQty': (int(text(pl.find('u:PackQty', NS))) if text(pl.find('u:PackQty', NS)).isdigit() else None),
                'PackType': (text(pl.find('u:PackType/u:Code', NS)) or None, text(pl.find('u:PackType/u:Description', NS)) or None),
                'ReferenceNumber': text(pl.find('u:ReferenceNumber', NS)) or None,
                'RequiresTemperatureControl': ((text(pl.find('u:RequiresTemperatureControl', NS)).lower() == 'true') if pl.find('u:RequiresTemperatureControl', NS) is not None else None),
                'Volume': text(pl.find('u:Volume', NS)) or None,
                'VolumeUnit': (text(pl.find('u:VolumeUnit/u:Code', NS)) or None, text(pl.find('u:VolumeUnit/u:Description', NS)) or None),
                'Weight': text(pl.find('u:Weight', NS)) or None,
                'WeightUnit': (text(pl.find('u:WeightUnit/u:Code', NS)) or None, text(pl.find('u:WeightUnit/u:Description', NS)) or None),
                'Width': text(pl.find('u:Width', NS)) or None,
            })
    fact['PackingLinesShipment'] = packs_sh
    # Shipment-level ContainerCollection
    conts_sh = []
    cc_sh = root.find('.//u:Shipment/u:ContainerCollection', NS)
    if cc_sh is not None:
        for ctn in cc_sh.findall('u:Container', NS):
            def dec(tag: str) -> Optional[str]:
                return text(ctn.find(tag, NS)) or None
            def bool_opt(tag: str) -> Optional[bool]:
                el = ctn.find(tag, NS)
                return (text(el).lower() == 'true') if el is not None and text(el) != '' else None
            conts_sh.append({
                'ContainerJobID': dec('u:ContainerJobID'),
                'ContainerNumber': dec('u:ContainerNumber'),
                'Link': (int(text(ctn.find('u:Link', NS))) if text(ctn.find('u:Link', NS)).isdigit() else None),
                'ContainerType': (
                    text(ctn.find('u:ContainerType/u:Code', NS)) or None,
                    text(ctn.find('u:ContainerType/u:Description', NS)) or None,
                    text(ctn.find('u:ContainerType/u:ISOCode', NS)) or None,
                    text(ctn.find('u:ContainerType/u:Category/u:Code', NS)) or None,
                    text(ctn.find('u:ContainerType/u:Category/u:Description', NS)) or None,
                ),
                'DeliveryMode': dec('u:DeliveryMode'),
                'FCL_LCL_AIR': (
                    text(ctn.find('u:FCL_LCL_AIR/u:Code', NS)) or None,
                    text(ctn.find('u:FCL_LCL_AIR/u:Description', NS)) or None,
                ),
                'ContainerCount': (int(text(ctn.find('u:ContainerCount', NS))) if text(ctn.find('u:ContainerCount', NS)).isdigit() else None),
                'Seal': dec('u:Seal'),
                'SealPartyTypeCode': text(ctn.find('u:SealPartyType/u:Code', NS)) or None,
                'SecondSeal': dec('u:SecondSeal'),
                'SecondSealPartyTypeCode': text(ctn.find('u:SecondSealPartyType/u:Code', NS)) or None,
                'ThirdSeal': dec('u:ThirdSeal'),
                'ThirdSealPartyTypeCode': text(ctn.find('u:ThirdSealPartyType/u:Code', NS)) or None,
                'StowagePosition': dec('u:StowagePosition'),
                'LengthUnit': (text(ctn.find('u:LengthUnit/u:Code', NS)) or None, text(ctn.find('u:LengthUnit/u:Description', NS)) or None),
                'VolumeUnit': (text(ctn.find('u:VolumeUnit/u:Code', NS)) or None, text(ctn.find('u:VolumeUnit/u:Description', NS)) or None),
                'WeightUnit': (text(ctn.find('u:WeightUnit/u:Code', NS)) or None, text(ctn.find('u:WeightUnit/u:Description', NS)) or None),
                'TotalHeight': dec('u:TotalHeight'),
                'TotalLength': dec('u:TotalLength'),
                'TotalWidth': dec('u:TotalWidth'),
                'TareWeight': dec('u:TareWeight'),
                'GrossWeight': dec('u:GrossWeight'),
                'GoodsWeight': dec('u:GoodsWeight'),
                'VolumeCapacity': dec('u:VolumeCapacity'),
                'WeightCapacity': dec('u:WeightCapacity'),
                'DunnageWeight': dec('u:DunnageWeight'),
                'OverhangBack': dec('u:OverhangBack'),
                'OverhangFront': dec('u:OverhangFront'),
                'OverhangHeight': dec('u:OverhangHeight'),
                'OverhangLeft': dec('u:OverhangLeft'),
                'OverhangRight': dec('u:OverhangRight'),
                'HumidityPercent': (int(text(ctn.find('u:HumidityPercent', NS))) if text(ctn.find('u:HumidityPercent', NS)).isdigit() else None),
                'AirVentFlow': dec('u:AirVentFlow'),
                'AirVentFlowRateUnitCode': text(ctn.find('u:AirVentFlowRateUnit/u:Code', NS)) or None,
                'NonOperatingReefer': bool_opt('u:NonOperatingReefer'),
                'IsCFSRegistered': bool_opt('u:IsCFSRegistered'),
                'IsControlledAtmosphere': bool_opt('u:IsControlledAtmosphere'),
                'IsDamaged': bool_opt('u:IsDamaged'),
                'IsEmptyContainer': bool_opt('u:IsEmptyContainer'),
                'IsSealOk': bool_opt('u:IsSealOk'),
                'IsShipperOwned': bool_opt('u:IsShipperOwned'),
                'ArrivalPickupByRail': bool_opt('u:ArrivalPickupByRail'),
                'DepartureDeliveryByRail': bool_opt('u:DepartureDeliveryByRail'),
                'ArrivalSlotDateTime': dec('u:ArrivalSlotDateTime'),
                'DepartureSlotDateTime': dec('u:DepartureSlotDateTime'),
                'EmptyReadyForReturn': dec('u:EmptyReadyForReturn'),
                'FCLWharfGateIn': dec('u:FCLWharfGateIn'),
                'FCLWharfGateOut': dec('u:FCLWharfGateOut'),
                'FCLStorageCommences': dec('u:FCLStorageCommences'),
                'LCLUnpack': dec('u:LCLUnpack'),
                'PackDate': dec('u:PackDate'),
            })
    fact['ContainersShipment'] = conts_sh
    # Header-level AdditionalReferenceCollection (Shipment)
    add_refs_sh = []
    sh_add = root.find('.//u:Shipment/u:AdditionalReferenceCollection', NS)
    if sh_add is not None:
        for ar_ in sh_add.findall('u:AdditionalReference', NS):
            t = ar_.find('u:Type', NS)
            coi = ar_.find('u:CountryOfIssue', NS)
            issue = text(ar_.find('u:IssueDate', NS)) or None
            add_refs_sh.append({
                'TypeCode': text(t.find('u:Code', NS)) if t is not None else None,
                'TypeDescription': text(t.find('u:Description', NS)) if t is not None else None,
                'ReferenceNumber': text(ar_.find('u:ReferenceNumber', NS)) or None,
                'ContextInformation': text(ar_.find('u:ContextInformation', NS)) or None,
                'CountryOfIssue': (text(coi.find('u:Code', NS)) if coi is not None else None, text(coi.find('u:Name', NS)) if coi is not None else None),
                'IssueDate': issue,
            })
    fact["AdditionalReferencesShipment"] = add_refs_sh
    # Shipment-level JobCosting header (CSL)
    jc_sh = root.find('.//u:Shipment/u:JobCosting', NS)
    if jc_sh is not None:
        fact['JobCostingHeaderShipment'] = {
            'Branch': (text(jc_sh.find('u:Branch/u:Code', NS)) or None, text(jc_sh.find('u:Branch/u:Name', NS)) or None),
            'Department': (text(jc_sh.find('u:Department/u:Code', NS)) or None, text(jc_sh.find('u:Department/u:Name', NS)) or None),
            'HomeBranch': (text(jc_sh.find('u:HomeBranch/u:Code', NS)) or None, text(jc_sh.find('u:HomeBranch/u:Name', NS)) or None),
            'Currency': (text(jc_sh.find('u:Currency/u:Code', NS)) or None, text(jc_sh.find('u:Currency/u:Description', NS)) or None),
            'OperationsStaff': (text(jc_sh.find('u:OperationsStaff/u:Code', NS)) or None, text(jc_sh.find('u:OperationsStaff/u:Name', NS)) or None),
            'ClientContractNumber': text(jc_sh.find('u:ClientContractNumber', NS)) or None,
            'AccrualNotRecognized': text(jc_sh.find('u:AccrualNotRecognized', NS)) or None,
            'AccrualRecognized': text(jc_sh.find('u:AccrualRecognized', NS)) or None,
            'AgentRevenue': text(jc_sh.find('u:AgentRevenue', NS)) or None,
            'LocalClientRevenue': text(jc_sh.find('u:LocalClientRevenue', NS)) or None,
            'OtherDebtorRevenue': text(jc_sh.find('u:OtherDebtorRevenue', NS)) or None,
            'TotalAccrual': text(jc_sh.find('u:TotalAccrual', NS)) or None,
            'TotalCost': text(jc_sh.find('u:TotalCost', NS)) or None,
            'TotalJobProfit': text(jc_sh.find('u:TotalJobProfit', NS)) or None,
            'TotalRevenue': text(jc_sh.find('u:TotalRevenue', NS)) or None,
            'TotalWIP': text(jc_sh.find('u:TotalWIP', NS)) or None,
            'WIPNotRecognized': text(jc_sh.find('u:WIPNotRecognized', NS)) or None,
            'WIPRecognized': text(jc_sh.find('u:WIPRecognized', NS)) or None,
        }
        # Shipment-level ChargeLineCollection under JobCosting
        clc_sh = jc_sh.find('u:ChargeLineCollection', NS)
        charge_lines_sh = []
        if clc_sh is not None:
            for cl in clc_sh.findall('u:ChargeLine', NS):
                charge_lines_sh.append({
                    'Branch': (text(cl.find('u:Branch/u:Code', NS)), text(cl.find('u:Branch/u:Name', NS))),
                    'Department': (text(cl.find('u:Department/u:Code', NS)), text(cl.find('u:Department/u:Name', NS))),
                    'ChargeCode': (text(cl.find('u:ChargeCode/u:Code', NS)), text(cl.find('u:ChargeCode/u:Description', NS))),
                    'ChargeGroup': (text(cl.find('u:ChargeCodeGroup/u:Code', NS)), text(cl.find('u:ChargeCodeGroup/u:Description', NS))),
                    'Description': text(cl.find('u:Description', NS)) or None,
                    'DisplaySequence': text(cl.find('u:DisplaySequence', NS)) or '',
                    'CreditorKey': text(cl.find('u:Creditor/u:Key', NS)) or '',
                    'DebtorKey': text(cl.find('u:Debtor/u:Key', NS)) or '',
                    'Cost': {
                        'APInvoiceNumber': text(cl.find('u:CostAPInvoiceNumber', NS)) or None,
                        'DueDate': text(cl.find('u:CostDueDate', NS)) or None,
                        'ExchangeRate': text(cl.find('u:CostExchangeRate', NS)) or None,
                        'InvoiceDate': text(cl.find('u:CostInvoiceDate', NS)) or None,
                        'IsPosted': text(cl.find('u:CostIsPosted', NS)) or None,
                        'LocalAmount': text(cl.find('u:CostLocalAmount', NS)) or None,
                        'OSAmount': text(cl.find('u:CostOSAmount', NS)) or None,
                        'OSCurrency': (text(cl.find('u:CostOSCurrency/u:Code', NS)), text(cl.find('u:CostOSCurrency/u:Description', NS))),
                        'OSGSTVATAmount': text(cl.find('u:CostOSGSTVATAmount', NS)) or None,
                    },
                    'Sell': {
                        'ExchangeRate': text(cl.find('u:SellExchangeRate', NS)) or None,
                        'GSTVAT': (text(cl.find('u:SellGSTVATID/u:TaxCode', NS)) or None, text(cl.find('u:SellGSTVATID/u:Description', NS)) or None),
                        'InvoiceType': text(cl.find('u:SellInvoiceType', NS)) or None,
                        'IsPosted': text(cl.find('u:SellIsPosted', NS)) or None,
                        'LocalAmount': text(cl.find('u:SellLocalAmount', NS)) or None,
                        'OSAmount': text(cl.find('u:SellOSAmount', NS)) or None,
                        'OSCurrency': (text(cl.find('u:SellOSCurrency/u:Code', NS)), text(cl.find('u:SellOSCurrency/u:Description', NS))),
                        'OSGSTVATAmount': text(cl.find('u:SellOSGSTVATAmount', NS)) or None,
                        'PostedTransaction': (
                            text(cl.find('u:SellPostedTransaction/u:Number', NS)) or None,
                            text(cl.find('u:SellPostedTransaction/u:TransactionType', NS)) or None,
                            text(cl.find('u:SellPostedTransaction/u:TransactionDate', NS)) or None,
                            text(cl.find('u:SellPostedTransaction/u:DueDate', NS)) or None,
                            text(cl.find('u:SellPostedTransaction/u:FullyPaidDate', NS)) or None,
                            text(cl.find('u:SellPostedTransaction/u:OutstandingAmount', NS)) or None,
                        ),
                    },
                    'SupplierReference': text(cl.find('u:SupplierReference', NS)) or None,
                    'SellReference': text(cl.find('u:SellReference', NS)) or None,
                })
        fact['ChargeLinesShipment'] = charge_lines_sh
    return dims, fact


def upsert_csl(cur: pyodbc.Cursor, dims: Dict, fact: Dict, counters: Optional[TableCounter] = None) -> None:
    # Country and Company
    (country_code, country_name) = dims["Country"]
    if country_code:
        ensure_country(cur, country_code, country_name)
    (company_code, company_name, comp_country_code) = dims["Company"]
    company_key = ensure_company(cur, company_code, company_name, comp_country_code)

    # Branch
    b_code, b_name = dims.get("Branch", ("", ""))
    branch_key = ensure_branch(cur, b_code, b_name)

    # Simple dims
    simple_keys = ensure_simple_dims(cur, dims)

    # Jobs
    consol_job_key = None
    shipment_job_key = None
    jtype, jkey = fact.get("ConsolJob", (None, None))
    if jtype and jkey:
        consol_job_key = ensure_job(cur, jtype, jkey)
    jtype, jkey = fact.get("ShipmentJob", (None, None))
    if jtype and jkey:
        shipment_job_key = ensure_job(cur, jtype, jkey)

    # Ports
    port_keys = {}
    for pname, (pcode, pname_) in fact["Ports"].items():
        port_keys[pname] = ensure_port(cur, pcode, pname_)

    # Dims: service levels, payment, currency, units, container, screening, co2e status
    awb_code, awb_desc = fact["AWB"]
    gateway_code, gateway_desc = fact["Gateway"]
    st_code, st_desc = fact["ShipmentType"]
    rt_code, rt_desc = fact["ReleaseType"]
    scr_code, scr_desc = fact["ScreeningStatus"]
    pm_code, pm_desc = fact["PaymentMethod"]
    cur_code, cur_desc = fact["Currency"]
    cm_code, cm_desc = fact["ContainerMode"]
    co2s_code, co2s_desc = fact["CO2eStatus"]
    co2u_code, co2u_desc = fact["CO2eUnit"]
    tvu_code, tvu_desc = fact["TotalVolumeUnit"]
    twu_code, twu_desc = fact["TotalWeightUnit"]
    pu_code, pu_desc = fact["PacksUnit"]

    awb_sl = ensure_service_level(cur, awb_code, awb_desc, "AWB")
    gateway_sl = ensure_service_level(cur, gateway_code, gateway_desc, "Gateway")
    shipment_type_sl = ensure_service_level(cur, st_code, st_desc, "ShipmentType")
    release_type_sl = ensure_service_level(cur, rt_code, rt_desc, "ReleaseType")
    screening_key = ensure_screening_status(cur, scr_code, scr_desc)
    payment_key = ensure_payment_method(cur, pm_code, pm_desc)
    currency_key = ensure_currency(cur, cur_code, cur_desc)
    container_mode_key = ensure_container_mode(cur, cm_code, cm_desc)
    co2e_status_key = ensure_co2e_status(cur, co2s_code, co2s_desc)
    co2e_unit_key = ensure_unit(cur, co2u_code, co2u_desc, "CO2e")
    total_volume_unit_key = ensure_unit(cur, tvu_code, tvu_desc, "Volume")
    total_weight_unit_key = ensure_unit(cur, twu_code, twu_desc, "Weight")
    packs_unit_key = ensure_unit(cur, pu_code, pu_desc, "Packs")

    # Compose insert/update; use ShipmentJobKey as business key for upsert if available
    measures = fact["Measures"]
    flags = fact["Flags"]
    cols = {
        "CompanyKey": company_key,
    "BranchKey": branch_key,
        "DepartmentKey": simple_keys.get("DepartmentKey"),
        "EventTypeKey": simple_keys.get("EventTypeKey"),
        "ActionPurposeKey": simple_keys.get("ActionPurposeKey"),
        "UserKey": simple_keys.get("UserKey"),
        "EnterpriseKey": simple_keys.get("EnterpriseKey"),
        "ServerKey": simple_keys.get("ServerKey"),
        "DataProviderKey": simple_keys.get("DataProviderKey"),
        "TriggerDateKey": fact.get("TriggerDateKey"),
        "ConsolJobKey": consol_job_key,
        "ShipmentJobKey": shipment_job_key,
        "PlaceOfDeliveryKey": port_keys.get("PlaceOfDelivery"),
        "PlaceOfIssueKey": port_keys.get("PlaceOfIssue"),
        "PlaceOfReceiptKey": port_keys.get("PlaceOfReceipt"),
        "PortFirstForeignKey": port_keys.get("PortFirstForeign"),
        "PortLastForeignKey": port_keys.get("PortLastForeign"),
        "PortOfDischargeKey": port_keys.get("PortOfDischarge"),
        "PortOfFirstArrivalKey": port_keys.get("PortOfFirstArrival"),
        "PortOfLoadingKey": port_keys.get("PortOfLoading"),
        "EventBranchHomePortKey": port_keys.get("EventBranchHomePort"),
        "AWBServiceLevelKey": awb_sl,
        "GatewayServiceLevelKey": gateway_sl,
        "ShipmentTypeKey": shipment_type_sl,
        "ReleaseTypeKey": release_type_sl,
        "ScreeningStatusKey": screening_key,
        "PaymentMethodKey": payment_key,
        "FreightRateCurrencyKey": currency_key,
        "TotalVolumeUnitKey": total_volume_unit_key,
        "TotalWeightUnitKey": total_weight_unit_key,
        "CO2eUnitKey": co2e_unit_key,
        "PacksUnitKey": packs_unit_key,
        "ContainerModeKey": container_mode_key,
        "Co2eStatusKey": co2e_status_key,
        # measures
        "ContainerCount": measures.get("ContainerCount"),
        "ChargeableRate": measures.get("ChargeableRate"),
        "DocumentedChargeable": measures.get("DocumentedChargeable"),
        "DocumentedVolume": measures.get("DocumentedVolume"),
        "DocumentedWeight": measures.get("DocumentedWeight"),
        "FreightRate": measures.get("FreightRate"),
        "GreenhouseGasEmissionCO2e": measures.get("GreenhouseGasEmissionCO2e"),
        "ManifestedChargeable": measures.get("ManifestedChargeable"),
        "ManifestedVolume": measures.get("ManifestedVolume"),
        "ManifestedWeight": measures.get("ManifestedWeight"),
        "MaximumAllowablePackageHeight": measures.get("MaximumAllowablePackageHeight"),
        "MaximumAllowablePackageLength": measures.get("MaximumAllowablePackageLength"),
        "MaximumAllowablePackageWidth": measures.get("MaximumAllowablePackageWidth"),
        "NoCopyBills": measures.get("NoCopyBills"),
        "NoOriginalBills": measures.get("NoOriginalBills"),
        "OuterPacks": measures.get("OuterPacks"),
        "TotalNoOfPacks": measures.get("TotalNoOfPacks"),
        "TotalPreallocatedChargeable": measures.get("TotalPreallocatedChargeable"),
        "TotalPreallocatedVolume": measures.get("TotalPreallocatedVolume"),
        "TotalPreallocatedWeight": measures.get("TotalPreallocatedWeight"),
        "TotalVolume": measures.get("TotalVolume"),
        "TotalWeight": measures.get("TotalWeight"),
        # flags
        "IsCFSRegistered": flags.get("IsCFSRegistered"),
        "IsDirectBooking": flags.get("IsDirectBooking"),
        "IsForwardRegistered": flags.get("IsForwardRegistered"),
        "IsHazardous": flags.get("IsHazardous"),
        "IsNeutralMaster": flags.get("IsNeutralMaster"),
        "RequiresTemperatureControl": flags.get("RequiresTemperatureControl"),
    }

    fact_ship_key = None
    if shipment_job_key:
        # Try find existing row to update
        cur.execute("SELECT FactShipmentKey FROM Dwh2.FactShipment WHERE ShipmentJobKey = ?", shipment_job_key)
        r = cur.fetchone()
        if r:
            fact_ship_key = int(r[0])
            set_clause = ", ".join([f"[{k}] = ?" for k in cols.keys()] + ["UpdatedAt = SYSUTCDATETIME()"])
            params = list(cols.values()) + [fact_ship_key]
            cur.execute(f"UPDATE Dwh2.FactShipment SET {set_clause} WHERE FactShipmentKey = ?", *params)
            if counters:
                counters.add('Dwh2.FactShipment', updated=1)
        else:
            # Insert new
            col_names = list(cols.keys())
            placeholders = ",".join(["?"] * len(col_names))
            cur.execute("INSERT INTO Dwh2.FactShipment ([" + "],[".join(col_names) + "]) VALUES (" + placeholders + ")", *list(cols.values()))
            cur.execute("SELECT TOP 1 FactShipmentKey FROM Dwh2.FactShipment WHERE ShipmentJobKey IS NOT NULL AND ShipmentJobKey = ? ORDER BY FactShipmentKey DESC", shipment_job_key)
            r = cur.fetchone()
            fact_ship_key = int(r[0]) if r else None
            if counters and fact_ship_key:
                counters.add('Dwh2.FactShipment', added=1)
    else:
        # No business key => always insert
        col_names = list(cols.keys())
        placeholders = ",".join(["?"] * len(col_names))
        cur.execute("INSERT INTO Dwh2.FactShipment ([" + "],[".join(col_names) + "]) VALUES (" + placeholders + ")", *list(cols.values()))
        cur.execute("SELECT TOP 1 FactShipmentKey FROM Dwh2.FactShipment ORDER BY FactShipmentKey DESC")
        r = cur.fetchone()
        fact_ship_key = int(r[0]) if r else None
        if counters and fact_ship_key:
            counters.add('Dwh2.FactShipment', added=1)

    # Bridge rows for organizations (CSL)
    orgs = dims.get("Organizations") or []
    if orgs and fact_ship_key:
        for org in orgs:
            c_code, c_name = org.get("Country", ("", ""))
            p_code, p_name = org.get("Port", ("", ""))
            ckey = ensure_country(cur, c_code, c_name) if c_code else None
            pkey = ensure_port(cur, p_code, p_name) if p_code else None
            extra = {}
            if ckey is not None:
                extra["CountryKey"] = ckey
            if pkey is not None:
                extra["PortKey"] = pkey
            # upsert organization
            try:
                org_key = _upsert_scalar_dim(
                    cur,
                    "Dwh2.DimOrganization",
                    "OrganizationCode",
                    org.get("OrganizationCode") or "",
                    "CompanyName",
                    org.get("CompanyName"),
                    extra_cols=extra if extra else None,
                    key_col="OrganizationKey",
                )
            except Exception:
                code = org.get("OrganizationCode") or ""
                name = org.get("CompanyName") or code
                if extra:
                    cur.execute(
                        "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimOrganization] WHERE [OrganizationCode]=?) "
                        "INSERT INTO [Dwh2].[DimOrganization] ([OrganizationCode],[CompanyName],[CountryKey],[PortKey]) VALUES (?,?,?,?); "
                        "ELSE UPDATE [Dwh2].[DimOrganization] SET [CompanyName]=?,[CountryKey]=?,[PortKey]=?, UpdatedAt=SYSUTCDATETIME() WHERE [OrganizationCode]=?;",
                        code, code, name, extra.get("CountryKey"), extra.get("PortKey"), name, extra.get("CountryKey"), extra.get("PortKey"), code
                    )
                else:
                    cur.execute(
                        "IF NOT EXISTS (SELECT 1 FROM [Dwh2].[DimOrganization] WHERE [OrganizationCode]=?) "
                        "INSERT INTO [Dwh2].[DimOrganization] ([OrganizationCode],[CompanyName]) VALUES (?,?); "
                        "ELSE UPDATE [Dwh2].[DimOrganization] SET [CompanyName]=?, UpdatedAt=SYSUTCDATETIME() WHERE [OrganizationCode]=?;",
                        code, code, name, name, code
                    )
                cur.execute("SELECT [OrganizationKey] FROM [Dwh2].[DimOrganization] WHERE [OrganizationCode]=?", code)
                r = cur.fetchone()
                org_key = int(r[0]) if r else None
            if org_key is not None:
                try:
                    cur.execute(
                        "SELECT 1 FROM Dwh2.BridgeFactShipmentOrganization WHERE FactShipmentKey=? AND OrganizationKey=? AND AddressType=?",
                        fact_ship_key, org_key, org.get("AddressType") or ""
                    )
                    exists = cur.fetchone()
                    if not exists:
                        cur.execute(
                            "INSERT INTO Dwh2.BridgeFactShipmentOrganization (FactShipmentKey, OrganizationKey, AddressType) VALUES (?,?,?)",
                            fact_ship_key, org_key, org.get("AddressType") or ""
                        )
                        if counters:
                            counters.add('Dwh2.BridgeFactShipmentOrganization', added=1)
                except Exception:
                    pass

    # Shipment-level TransportLegCollection rows
    if fact_ship_key and fact.get('TransportLegsShipment'):
        rows = []
        for leg in fact['TransportLegsShipment']:
            pol_code, pol_name = leg['PortOfLoading']
            pod_code, pod_name = leg['PortOfDischarge']
            polk = ensure_port(cur, pol_code, pol_name) if pol_code else None
            podk = ensure_port(cur, pod_code, pod_name) if pod_code else None
            bsc, bsd = leg['BookingStatus']
            carr_code, carr_name, carr_country_code, carr_port_code = leg['Carrier']
            cred_code, cred_name, cred_country_code, cred_port_code = leg['Creditor']
            carr_key = ensure_organization_min(cur, carr_code, carr_name, carr_country_code, carr_port_code) if carr_code else None
            cred_key = ensure_organization_min(cur, cred_code, cred_name, cred_country_code, cred_port_code) if cred_code else None
            rows.append((
                fact_ship_key, None, None,
                polk, podk, leg.get('Order'), leg.get('TransportMode'),
                leg.get('VesselName'), leg.get('VesselLloydsIMO'), leg.get('VoyageFlightNo'),
                leg.get('CarrierBookingReference'), bsc, bsd,
                carr_key, cred_key,
                _datekey_from_iso(leg.get('ActualArrival')), _time_from_iso(leg.get('ActualArrival')),
                _datekey_from_iso(leg.get('ActualDeparture')), _time_from_iso(leg.get('ActualDeparture')),
                _datekey_from_iso(leg.get('EstimatedArrival')), _time_from_iso(leg.get('EstimatedArrival')),
                _datekey_from_iso(leg.get('EstimatedDeparture')), _time_from_iso(leg.get('EstimatedDeparture')),
                _datekey_from_iso(leg.get('ScheduledArrival')), _time_from_iso(leg.get('ScheduledArrival')),
                _datekey_from_iso(leg.get('ScheduledDeparture')), _time_from_iso(leg.get('ScheduledDeparture')),
                company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
            ))
        if rows:
            try:
                if ENABLE_BATCH:
                    cur.fast_executemany = True
            except Exception:
                pass
            cur.executemany(
                "INSERT INTO Dwh2.FactTransportLeg (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, PortOfLoadingKey, PortOfDischargeKey, [Order], TransportMode, VesselName, VesselLloydsIMO, VoyageFlightNo, CarrierBookingReference, BookingStatusCode, BookingStatusDescription, CarrierOrganizationKey, CreditorOrganizationKey, ActualArrivalDateKey, ActualArrivalTime, ActualDepartureDateKey, ActualDepartureTime, EstimatedArrivalDateKey, EstimatedArrivalTime, EstimatedDepartureDateKey, EstimatedDepartureTime, ScheduledArrivalDateKey, ScheduledArrivalTime, ScheduledDepartureDateKey, ScheduledDepartureTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows
            )
            if counters:
                counters.add('Dwh2.FactTransportLeg', added=len(rows))

    # Shipment-level ContainerCollection (CSL)
    if fact_ship_key and fact.get('ContainersShipment'):
        conts = fact.get('ContainersShipment') or []
        if conts:
            try:
                cur.execute("DELETE FROM Dwh2.FactContainer WHERE FactShipmentKey = ? AND Source = 'CSL'", fact_ship_key)
            except Exception:
                pass
            rows = []
            for ct in conts:
                len_code, len_desc = ct.get('LengthUnit') or (None, None)
                vol_code, vol_desc = ct.get('VolumeUnit') or (None, None)
                wei_code, wei_desc = ct.get('WeightUnit') or (None, None)
                lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                ct_code, ct_desc, ct_iso, cat_code, cat_desc = ct.get('ContainerType') or (None, None, None, None, None)
                fclc, fcld = ct.get('FCL_LCL_AIR') or (None, None)
                rows.append((
                    fact_ship_key, None, None,
                    'CSL',
                    ct.get('ContainerJobID'), ct.get('ContainerNumber'), ct.get('Link'),
                    ct_code, ct_desc, ct_iso, cat_code, cat_desc,
                    ct.get('DeliveryMode'), fclc, fcld, ct.get('ContainerCount'),
                    ct.get('Seal'), ct.get('SealPartyTypeCode'), ct.get('SecondSeal'), ct.get('SecondSealPartyTypeCode'), ct.get('ThirdSeal'), ct.get('ThirdSealPartyTypeCode'), ct.get('StowagePosition'),
                    lenk, volk, weik,
                    ct.get('TotalHeight'), ct.get('TotalLength'), ct.get('TotalWidth'), ct.get('TareWeight'), ct.get('GrossWeight'), ct.get('GoodsWeight'), ct.get('VolumeCapacity'), ct.get('WeightCapacity'), ct.get('DunnageWeight'), ct.get('OverhangBack'), ct.get('OverhangFront'), ct.get('OverhangHeight'), ct.get('OverhangLeft'), ct.get('OverhangRight'), ct.get('HumidityPercent'), ct.get('AirVentFlow'), ct.get('AirVentFlowRateUnitCode'),
                    (1 if ct.get('NonOperatingReefer') is True else (0 if ct.get('NonOperatingReefer') is False else None)),
                    (1 if ct.get('IsCFSRegistered') is True else (0 if ct.get('IsCFSRegistered') is False else None)),
                    (1 if ct.get('IsControlledAtmosphere') is True else (0 if ct.get('IsControlledAtmosphere') is False else None)),
                    (1 if ct.get('IsDamaged') is True else (0 if ct.get('IsDamaged') is False else None)),
                    (1 if ct.get('IsEmptyContainer') is True else (0 if ct.get('IsEmptyContainer') is False else None)),
                    (1 if ct.get('IsSealOk') is True else (0 if ct.get('IsSealOk') is False else None)),
                    (1 if ct.get('IsShipperOwned') is True else (0 if ct.get('IsShipperOwned') is False else None)),
                    (1 if ct.get('ArrivalPickupByRail') is True else (0 if ct.get('ArrivalPickupByRail') is False else None)),
                    (1 if ct.get('DepartureDeliveryByRail') is True else (0 if ct.get('DepartureDeliveryByRail') is False else None)),
                    _datekey_from_iso(ct.get('ArrivalSlotDateTime')), _time_from_iso(ct.get('ArrivalSlotDateTime')),
                    _datekey_from_iso(ct.get('DepartureSlotDateTime')), _time_from_iso(ct.get('DepartureSlotDateTime')),
                    _datekey_from_iso(ct.get('EmptyReadyForReturn')), _time_from_iso(ct.get('EmptyReadyForReturn')),
                    _datekey_from_iso(ct.get('FCLWharfGateIn')), _time_from_iso(ct.get('FCLWharfGateIn')),
                    _datekey_from_iso(ct.get('FCLWharfGateOut')), _time_from_iso(ct.get('FCLWharfGateOut')),
                    _datekey_from_iso(ct.get('FCLStorageCommences')), _time_from_iso(ct.get('FCLStorageCommences')),
                    _datekey_from_iso(ct.get('LCLUnpack')), _time_from_iso(ct.get('LCLUnpack')),
                    _datekey_from_iso(ct.get('PackDate')), _time_from_iso(ct.get('PackDate')),
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
            if rows:
                try:
                    if ENABLE_BATCH:
                        cur.fast_executemany = True
                except Exception:
                    pass
                insert_placeholders = ",".join(["?"] * len(rows[0]))
                sql = (
                    "INSERT INTO Dwh2.FactContainer (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, ContainerJobID, ContainerNumber, [Link], ContainerTypeCode, ContainerTypeDescription, ContainerTypeISOCode, ContainerCategoryCode, ContainerCategoryDescription, DeliveryMode, FCL_LCL_AIR_Code, FCL_LCL_AIR_Description, ContainerCount, Seal, SealPartyTypeCode, SecondSeal, SecondSealPartyTypeCode, ThirdSeal, ThirdSealPartyTypeCode, StowagePosition, LengthUnitKey, VolumeUnitKey, WeightUnitKey, TotalHeight, TotalLength, TotalWidth, TareWeight, GrossWeight, GoodsWeight, VolumeCapacity, WeightCapacity, DunnageWeight, OverhangBack, OverhangFront, OverhangHeight, OverhangLeft, OverhangRight, HumidityPercent, AirVentFlow, AirVentFlowRateUnitCode, NonOperatingReefer, IsCFSRegistered, IsControlledAtmosphere, IsDamaged, IsEmptyContainer, IsSealOk, IsShipperOwned, ArrivalPickupByRail, DepartureDeliveryByRail, ArrivalSlotDateKey, ArrivalSlotTime, DepartureSlotDateKey, DepartureSlotTime, EmptyReadyForReturnDateKey, EmptyReadyForReturnTime, FCLWharfGateInDateKey, FCLWharfGateInTime, FCLWharfGateOutDateKey, FCLWharfGateOutTime, FCLStorageCommencesDateKey, FCLStorageCommencesTime, LCLUnpackDateKey, LCLUnpackTime, PackDateKey, PackTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (" + insert_placeholders + ")"
                )
                cur.executemany(sql, rows)
                if counters:
                    counters.add('Dwh2.FactContainer', added=len(rows))
    # Shipment-level PackingLineCollection (CSL)
    if fact_ship_key and fact.get('PackingLinesShipment'):
        packs_sh = fact.get('PackingLinesShipment') or []
        if packs_sh:
            try:
                cur.execute("DELETE FROM Dwh2.FactPackingLine WHERE FactShipmentKey = ? AND Source = 'CSL'", fact_ship_key)
            except Exception:
                pass
            rows = []
            for pl in packs_sh:
                len_code, len_desc = pl.get('LengthUnit') or (None, None)
                vol_code, vol_desc = pl.get('VolumeUnit') or (None, None)
                wei_code, wei_desc = pl.get('WeightUnit') or (None, None)
                pack_code, pack_desc = pl.get('PackType') or (None, None)
                lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                packk = ensure_unit(cur, pack_code, pack_desc, 'Packs') if pack_code else None
                rows.append((
                    fact_ship_key, None, None,
                    'CSL',
                    pl.get('CommodityCode'), pl.get('CommodityDescription'), pl.get('ContainerLink'), pl.get('ContainerNumber'), pl.get('ContainerPackingOrder'), pl.get('CountryOfOriginCode'), pl.get('DetailedDescription'), pl.get('EndItemNo'), pl.get('ExportReferenceNumber'), pl.get('GoodsDescription'), pl.get('HarmonisedCode'),
                    pl.get('Height'), pl.get('Length'), pl.get('Width'), lenk,
                    pl.get('ImportReferenceNumber'), pl.get('ItemNo'),
                    pl.get('LastKnownCFSStatusCode'), _datekey_from_iso(pl.get('LastKnownCFSStatusDate')), _time_from_iso(pl.get('LastKnownCFSStatusDate')),
                    pl.get('LinePrice'), pl.get('Link'), pl.get('LoadingMeters'), pl.get('MarksAndNos'), pl.get('OutturnComment'), pl.get('OutturnDamagedQty'), pl.get('OutturnedHeight'), pl.get('OutturnedLength'), pl.get('OutturnedVolume'), pl.get('OutturnedWeight'), pl.get('OutturnedWidth'), pl.get('OutturnPillagedQty'), pl.get('OutturnQty'), pl.get('PackingLineID'), pl.get('PackQty'), packk, pl.get('ReferenceNumber'),
                    (1 if pl.get('RequiresTemperatureControl') is True else (0 if pl.get('RequiresTemperatureControl') is False else None)),
                    pl.get('Volume'), volk, pl.get('Weight'), weik,
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
            if rows:
                try:
                    cur.fast_executemany = True
                except Exception:
                    pass
                cur.executemany(
                    "INSERT INTO Dwh2.FactPackingLine (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, CommodityCode, CommodityDescription, ContainerLink, ContainerNumber, ContainerPackingOrder, CountryOfOriginCode, DetailedDescription, EndItemNo, ExportReferenceNumber, GoodsDescription, HarmonisedCode, Height, Length, Width, LengthUnitKey, ImportReferenceNumber, ItemNo, LastKnownCFSStatusCode, LastKnownCFSStatusDateKey, LastKnownCFSStatusTime, LinePrice, [Link], LoadingMeters, MarksAndNos, OutturnComment, OutturnDamagedQty, OutturnedHeight, OutturnedLength, OutturnedVolume, OutturnedWeight, OutturnedWidth, OutturnPillagedQty, OutturnQty, PackingLineID, PackQty, PackTypeUnitKey, ReferenceNumber, RequiresTemperatureControl, Volume, VolumeUnitKey, Weight, WeightUnitKey, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    rows
                )
                if counters:
                    counters.add('Dwh2.FactPackingLine', added=len(rows))

    # SubShipmentCollection
    if fact_ship_key and fact.get('SubShipments'):
        if counters is not None:
            try:
                print(f"  [debug] CSL SubShipments parsed={len(fact.get('SubShipments') or [])} for FactShipmentKey={fact_ship_key}")
            except Exception:
                pass
        # CSL is authoritative: if there are already sub-shipments (likely from AR fallback), replace them
        try:
            cur.execute("SELECT COUNT(1) FROM Dwh2.FactSubShipment WHERE FactShipmentKey=?", fact_ship_key)
            rcnt = cur.fetchone(); existing_subs = int(rcnt[0]) if rcnt else 0
        except Exception:
            existing_subs = 0
        if existing_subs > 0:
            try:
                # Delete child facts referencing existing sub-shipments for this shipment, then the sub-shipments
                cur.execute("DELETE FROM Dwh2.FactChargeLine WHERE FactSubShipmentKey IN (SELECT FactSubShipmentKey FROM Dwh2.FactSubShipment WHERE FactShipmentKey=?)", fact_ship_key)
            except Exception:
                pass
            try:
                cur.execute("DELETE FROM Dwh2.FactPackingLine WHERE FactSubShipmentKey IN (SELECT FactSubShipmentKey FROM Dwh2.FactSubShipment WHERE FactShipmentKey=?)", fact_ship_key)
            except Exception:
                pass
            try:
                cur.execute("DELETE FROM Dwh2.FactContainer WHERE FactSubShipmentKey IN (SELECT FactSubShipmentKey FROM Dwh2.FactSubShipment WHERE FactShipmentKey=?)", fact_ship_key)
            except Exception:
                pass
            try:
                cur.execute("DELETE FROM Dwh2.FactEventDate WHERE FactSubShipmentKey IN (SELECT FactSubShipmentKey FROM Dwh2.FactSubShipment WHERE FactShipmentKey=?)", fact_ship_key)
            except Exception:
                pass
            try:
                cur.execute("DELETE FROM Dwh2.FactJobCosting WHERE FactSubShipmentKey IN (SELECT FactSubShipmentKey FROM Dwh2.FactSubShipment WHERE FactShipmentKey=?)", fact_ship_key)
            except Exception:
                pass
            try:
                # Also remove transport legs tied to existing sub-shipments to avoid FK violations
                cur.execute("DELETE FROM Dwh2.FactTransportLeg WHERE FactSubShipmentKey IN (SELECT FactSubShipmentKey FROM Dwh2.FactSubShipment WHERE FactShipmentKey=?)", fact_ship_key)
            except Exception:
                pass
            try:
                # And any additional references tied to those sub-shipments
                cur.execute("DELETE FROM Dwh2.FactAdditionalReference WHERE FactSubShipmentKey IN (SELECT FactSubShipmentKey FROM Dwh2.FactSubShipment WHERE FactShipmentKey=?)", fact_ship_key)
            except Exception:
                pass
            try:
                cur.execute("DELETE FROM Dwh2.FactSubShipment WHERE FactShipmentKey=?", fact_ship_key)
            except Exception:
                pass
        sub_iter = fact['SubShipments']
        for sub_obj in sub_iter:
            # Ports inside sub-shipment
            ports = sub_obj['Ports']
            polk = ensure_port(cur, *ports['PortOfLoading']) if ports['PortOfLoading'][0] else None
            podk = ensure_port(cur, *ports['PortOfDischarge']) if ports['PortOfDischarge'][0] else None
            pfak = ensure_port(cur, *ports['PortOfFirstArrival']) if ports['PortOfFirstArrival'][0] else None
            podstk = ensure_port(cur, *ports['PortOfDestination']) if ports['PortOfDestination'][0] else None
            porgk = ensure_port(cur, *ports['PortOfOrigin']) if ports['PortOfOrigin'][0] else None
            ebpk = ensure_port(cur, *ports['EventBranchHomePort']) if ports['EventBranchHomePort'][0] else None
            # Dims
            dims_sub = sub_obj['Dims']
            slk = ensure_service_level(cur, *dims_sub['ServiceLevel'], 'Service') if dims_sub['ServiceLevel'][0] else None
            stk = ensure_service_level(cur, *dims_sub['ShipmentType'], 'ShipmentType') if dims_sub['ShipmentType'][0] else None
            rtk = ensure_service_level(cur, *dims_sub['ReleaseType'], 'ReleaseType') if dims_sub['ReleaseType'][0] else None
            cmk = ensure_container_mode(cur, *dims_sub['ContainerMode']) if dims_sub['ContainerMode'][0] else None
            frck = ensure_currency(cur, *dims_sub['FreightRateCurrency']) if dims_sub['FreightRateCurrency'][0] else None
            gvck = ensure_currency(cur, *dims_sub['GoodsValueCurrency']) if dims_sub['GoodsValueCurrency'][0] else None
            ivck = ensure_currency(cur, *dims_sub['InsuranceValueCurrency']) if dims_sub['InsuranceValueCurrency'][0] else None
            tvuk = ensure_unit(cur, *dims_sub['TotalVolumeUnit'], 'Volume') if dims_sub['TotalVolumeUnit'][0] else None
            twuk = ensure_unit(cur, *dims_sub['TotalWeightUnit'], 'Weight') if dims_sub['TotalWeightUnit'][0] else None
            puk = ensure_unit(cur, *dims_sub['PacksUnit'], 'Packs') if dims_sub['PacksUnit'][0] else None
            co2euk = ensure_unit(cur, *dims_sub['CO2eUnit'], 'CO2e') if dims_sub['CO2eUnit'][0] else None
            # Measures and attributes
            attrs = sub_obj['Attrs']
            def as_int(s: Optional[str]) -> Optional[int]:
                return int(s) if s and s.isdigit() else None
            cols = {
                'FactShipmentKey': fact_ship_key,
                'FactAccountsReceivableTransactionKey': None,
                'EventBranchHomePortKey': ebpk,
                'PortOfLoadingKey': polk,
                'PortOfDischargeKey': podk,
                'PortOfFirstArrivalKey': pfak,
                'PortOfDestinationKey': podstk,
                'PortOfOriginKey': porgk,
                'ServiceLevelKey': slk,
                'ShipmentTypeKey': stk,
                'ReleaseTypeKey': rtk,
                'ContainerModeKey': cmk,
                'FreightRateCurrencyKey': frck,
                'GoodsValueCurrencyKey': gvck,
                'InsuranceValueCurrencyKey': ivck,
                'TotalVolumeUnitKey': tvuk,
                'TotalWeightUnitKey': twuk,
                'PacksUnitKey': puk,
                'CO2eUnitKey': co2euk,
                'WayBillNumber': attrs.get('WayBillNumber'),
                'WayBillTypeCode': (attrs.get('WayBillType') or (None, None))[0],
                'WayBillTypeDescription': (attrs.get('WayBillType') or (None, None))[1],
                'VesselName': attrs.get('VesselName'),
                'VoyageFlightNo': attrs.get('VoyageFlightNo'),
                'LloydsIMO': attrs.get('LloydsIMO'),
                'TransportMode': attrs.get('TransportMode'),
                'ContainerCount': as_int(attrs.get('ContainerCount')),
                'ActualChargeable': attrs.get('ActualChargeable'),
                'DocumentedChargeable': attrs.get('DocumentedChargeable'),
                'DocumentedVolume': attrs.get('DocumentedVolume'),
                'DocumentedWeight': attrs.get('DocumentedWeight'),
                'GoodsValue': attrs.get('GoodsValue'),
                'InsuranceValue': attrs.get('InsuranceValue'),
                'FreightRate': attrs.get('FreightRate'),
                'TotalVolume': attrs.get('TotalVolume'),
                'TotalWeight': attrs.get('TotalWeight'),
                'TotalNoOfPacks': as_int(attrs.get('TotalNoOfPacks')),
                'OuterPacks': as_int(attrs.get('OuterPacks')),
                'GreenhouseGasEmissionCO2e': attrs.get('GreenhouseGasEmissionCO2e'),
                'IsBooking': 1 if (str(attrs.get('IsBooking') or '').lower() == 'true') else (0 if (str(attrs.get('IsBooking') or '').lower() == 'false') else None),
                'IsCancelled': 1 if (str(attrs.get('IsCancelled') or '').lower() == 'true') else (0 if (str(attrs.get('IsCancelled') or '').lower() == 'false') else None),
                'IsCFSRegistered': 1 if (str(attrs.get('IsCFSRegistered') or '').lower() == 'true') else (0 if (str(attrs.get('IsCFSRegistered') or '').lower() == 'false') else None),
                'IsDirectBooking': 1 if (str(attrs.get('IsDirectBooking') or '').lower() == 'true') else (0 if (str(attrs.get('IsDirectBooking') or '').lower() == 'false') else None),
                'IsForwardRegistered': 1 if (str(attrs.get('IsForwardRegistered') or '').lower() == 'true') else (0 if (str(attrs.get('IsForwardRegistered') or '').lower() == 'false') else None),
                'IsHighRisk': 1 if (str(attrs.get('IsHighRisk') or '').lower() == 'true') else (0 if (str(attrs.get('IsHighRisk') or '').lower() == 'false') else None),
                'IsNeutralMaster': 1 if (str(attrs.get('IsNeutralMaster') or '').lower() == 'true') else (0 if (str(attrs.get('IsNeutralMaster') or '').lower() == 'false') else None),
                'IsShipping': 1 if (str(attrs.get('IsShipping') or '').lower() == 'true') else (0 if (str(attrs.get('IsShipping') or '').lower() == 'false') else None),
                'IsSplitShipment': 1 if (str(attrs.get('IsSplitShipment') or '').lower() == 'true') else (0 if (str(attrs.get('IsSplitShipment') or '').lower() == 'false') else None),
            }
            # Insert sub-shipment
            col_names = list(cols.keys()) + ['CompanyKey', 'DepartmentKey', 'EventUserKey', 'DataProviderKey']
            col_vals = list(cols.values()) + [company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')]
            placeholders = ','.join(['?'] * len(col_names))
            cur.execute(
                "INSERT INTO Dwh2.FactSubShipment ([" + "],[".join(col_names) + "]) OUTPUT INSERTED.FactSubShipmentKey VALUES (" + placeholders + ")",
                *col_vals
            )
            rsub = cur.fetchone()
            sub_key = int(rsub[0]) if rsub and rsub[0] is not None else None
            if counters and sub_key:
                counters.add('Dwh2.FactSubShipment', added=1)
            if counters is not None:
                try:
                    print(f"    [debug] Created FactSubShipmentKey={sub_key} (WayBill={cols.get('WayBillNumber')})")
                    print(f"      [debug] Parsed TransportLegs={len(sub_obj.get('TransportLegs') or [])}, ChargeLines={len(sub_obj.get('ChargeLines') or [])}, PackingLines={len(sub_obj.get('PackingLines') or [])}, Containers={len(sub_obj.get('Containers') or [])}")
                except Exception:
                    pass

            # MilestoneCollection under SubShipment (CSL) -> FactMilestone (parent = SubShipment)
            sub_ms = sub_obj.get('Milestones') or []
            if sub_key and sub_ms:
                # Replace existing milestones for this SubShipment to ensure idempotency
                try:
                    cur.execute("DELETE FROM Dwh2.FactMilestone WHERE FactSubShipmentKey = ? AND Source='CSL'", sub_key)
                except Exception:
                    pass
                try:
                    cur.fast_executemany = True
                except Exception:
                    pass
                rows_ms = []
                for r in sub_ms:
                    rows_ms.append((
                        None,          # FactShipmentKey
                        sub_key,       # FactSubShipmentKey
                        r.get('Source'), r.get('EventCode'), r.get('Description'), r.get('Sequence'),
                        r.get('ActualDateKey'), r.get('ActualTime'), r.get('EstimatedDateKey'), r.get('EstimatedTime'), r.get('ConditionReference'), r.get('ConditionType'),
                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                    ))
                cur.executemany(
                    "INSERT INTO Dwh2.FactMilestone (FactShipmentKey, FactSubShipmentKey, Source, EventCode, [Description], [Sequence], ActualDateKey, ActualTime, EstimatedDateKey, EstimatedTime, ConditionReference, ConditionType, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    rows_ms
                )
                if counters:
                    counters.add('Dwh2.FactMilestone', added=len(rows_ms))

            # DateCollection under SubShipment (CSL) -> FactEventDate (parent = SubShipment)
            sub_dates = sub_obj.get('DateCollection') or []
            if sub_key and sub_dates:
                # idempotent: replace existing CSL sub dates
                try:
                    cur.execute("DELETE FROM Dwh2.FactEventDate WHERE FactSubShipmentKey = ? AND Source='CSL'", sub_key)
                except Exception:
                    pass
                try:
                    cur.fast_executemany = True
                except Exception:
                    pass
                rows_ed = []
                for r in sub_dates:
                    rows_ed.append((
                        None, sub_key, None,
                        'CSL', r.get('DateTypeCode'), r.get('DateTypeDescription'),
                        r.get('DateKey'), r.get('Time'), r.get('DateTimeText'), r.get('IsEstimate'), r.get('Value'), r.get('TimeZone'),
                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                    ))
                cur.executemany(
                    "INSERT INTO Dwh2.FactEventDate (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, DateTypeCode, DateTypeDescription, DateKey, [Time], DateTimeText, IsEstimate, [Value], TimeZone, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    rows_ed
                )
                if counters:
                    counters.add('Dwh2.FactEventDate', added=len(rows_ed))

            # JobCosting header under SubShipment (CSL) -> FactJobCosting (parent = SubShipment)
            if sub_key and sub_obj.get('JobCostingHeader'):
                h2 = sub_obj['JobCostingHeader']  # type: ignore[index]
                # Ensure dims
                brk = None
                dpk = None
                hbk = None
                osk = None
                curk2 = None
                br_code, br_name = (h2.get('Branch') or (None, None))
                if br_code:
                    brk = ensure_branch(cur, br_code, br_name)
                dp_code, dp_name = (h2.get('Department') or (None, None))
                if dp_code:
                    try:
                        dpk = _upsert_scalar_dim(cur, 'Dwh2.DimDepartment', 'Code', dp_code, 'Name', dp_name or dp_code, key_col='DepartmentKey')
                    except Exception:
                        cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimDepartment WHERE Code=?) INSERT INTO Dwh2.DimDepartment (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimDepartment SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", dp_code, dp_code, dp_name or dp_code, dp_name or dp_code, dp_code)
                        cur.execute("SELECT DepartmentKey FROM Dwh2.DimDepartment WHERE Code=?", dp_code)
                        r = cur.fetchone(); dpk = int(r[0]) if r else None
                hb_code, hb_name = (h2.get('HomeBranch') or (None, None))
                if hb_code:
                    hbk = ensure_branch(cur, hb_code, hb_name)
                os_code, os_name = (h2.get('OperationsStaff') or (None, None))
                if os_code:
                    try:
                        osk = _upsert_scalar_dim(cur, 'Dwh2.DimUser', 'Code', os_code, 'Name', os_name or os_code, key_col='UserKey')
                    except Exception:
                        cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimUser WHERE Code=?) INSERT INTO Dwh2.DimUser (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimUser SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", os_code, os_code, os_name or os_code, os_name or os_code, os_code)
                        cur.execute("SELECT UserKey FROM Dwh2.DimUser WHERE Code=?", os_code)
                        r = cur.fetchone(); osk = int(r[0]) if r else None
                cur_code2, cur_desc2 = (h2.get('Currency') or (None, None))
                if cur_code2:
                    curk2 = ensure_currency(cur, cur_code2, cur_desc2)
                # Measures as decimals
                def _dec_or_none(v: Optional[str]) -> Optional[float]:
                    try:
                        return float(v) if v is not None and v != '' else None
                    except Exception:
                        return None
                # Detect pre-existence for accurate counters
                existed = False
                try:
                    cur.execute("SELECT 1 FROM Dwh2.FactJobCosting WHERE FactSubShipmentKey=?", sub_key)
                    existed = cur.fetchone() is not None
                except Exception:
                    existed = False
                cols_jc = [
                    ('FactShipmentKey', None),
                    ('FactSubShipmentKey', sub_key),
                    ('FactAccountsReceivableTransactionKey', None),
                    ('Source', 'CSL'),
                    ('BranchKey', brk),
                    ('DepartmentKey', dpk),
                    ('HomeBranchKey', hbk),
                    ('OperationsStaffKey', osk),
                    ('CurrencyKey', curk2),
                    ('ClientContractNumber', h2.get('ClientContractNumber')),
                    ('AccrualNotRecognized', _dec_or_none(h2.get('AccrualNotRecognized'))),
                    ('AccrualRecognized', _dec_or_none(h2.get('AccrualRecognized'))),
                    ('AgentRevenue', _dec_or_none(h2.get('AgentRevenue'))),
                    ('LocalClientRevenue', _dec_or_none(h2.get('LocalClientRevenue'))),
                    ('OtherDebtorRevenue', _dec_or_none(h2.get('OtherDebtorRevenue'))),
                    ('TotalAccrual', _dec_or_none(h2.get('TotalAccrual'))),
                    ('TotalCost', _dec_or_none(h2.get('TotalCost'))),
                    ('TotalJobProfit', _dec_or_none(h2.get('TotalJobProfit'))),
                    ('TotalRevenue', _dec_or_none(h2.get('TotalRevenue'))),
                    ('TotalWIP', _dec_or_none(h2.get('TotalWIP'))),
                    ('WIPNotRecognized', _dec_or_none(h2.get('WIPNotRecognized'))),
                    ('WIPRecognized', _dec_or_none(h2.get('WIPRecognized'))),
                    ('CompanyKey', company_key),
                    ('EventUserKey', simple_keys.get('UserKey')),
                    ('DataProviderKey', simple_keys.get('DataProviderKey')),
                ]
                coln = [c for c,_ in cols_jc]
                vals = [v for _,v in cols_jc]
                placeholders = ','.join(['?']*len(coln))
                # Upsert by parent key (FactSubShipmentKey)
                set_clause = ', '.join([f'[{c}]=?' for c in coln[3:]]) + ', UpdatedAt=SYSUTCDATETIME()'
                set_vals = vals[3:]
                cur.execute(
                    "IF EXISTS (SELECT 1 FROM Dwh2.FactJobCosting WHERE FactSubShipmentKey=?) "
                    "UPDATE Dwh2.FactJobCosting SET " + set_clause + " WHERE FactSubShipmentKey=? "
                    "ELSE INSERT INTO Dwh2.FactJobCosting (" + ','.join('['+c+']' for c in coln) + ") VALUES (" + placeholders + ")",
                    sub_key, *set_vals, sub_key, *vals
                )
                if counters:
                    if existed:
                        counters.add('Dwh2.FactJobCosting', updated=1)
                    else:
                        counters.add('Dwh2.FactJobCosting', added=1)

            # TransportLegCollection under SubShipment
            tlegs = sub_obj.get('TransportLegs') or []
            if sub_key and tlegs:
                rows = []
                for leg in tlegs:
                    pol_code, pol_name = leg['PortOfLoading']
                    pod_code, pod_name = leg['PortOfDischarge']
                    polk = ensure_port(cur, pol_code, pol_name) if pol_code else None
                    podk = ensure_port(cur, pod_code, pod_name) if pod_code else None
                    bsc, bsd = leg['BookingStatus']
                    carr_code, carr_name, carr_country_code, carr_port_code = leg['Carrier']
                    cred_code, cred_name, cred_country_code, cred_port_code = leg['Creditor']
                    carr_key = ensure_organization_min(cur, carr_code, carr_name, carr_country_code, carr_port_code) if carr_code else None
                    cred_key = ensure_organization_min(cur, cred_code, cred_name, cred_country_code, cred_port_code) if cred_code else None
                    rows.append((
                        None, sub_key, None,
                        polk, podk, leg.get('Order'), leg.get('TransportMode'),
                        leg.get('VesselName'), leg.get('VesselLloydsIMO'), leg.get('VoyageFlightNo'),
                        leg.get('CarrierBookingReference'), bsc, bsd,
                        carr_key, cred_key,
                        _datekey_from_iso(leg.get('ActualArrival')), _time_from_iso(leg.get('ActualArrival')),
                        _datekey_from_iso(leg.get('ActualDeparture')), _time_from_iso(leg.get('ActualDeparture')),
                        _datekey_from_iso(leg.get('EstimatedArrival')), _time_from_iso(leg.get('EstimatedArrival')),
                        _datekey_from_iso(leg.get('EstimatedDeparture')), _time_from_iso(leg.get('EstimatedDeparture')),
                        _datekey_from_iso(leg.get('ScheduledArrival')), _time_from_iso(leg.get('ScheduledArrival')),
                        _datekey_from_iso(leg.get('ScheduledDeparture')), _time_from_iso(leg.get('ScheduledDeparture')),
                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                    ))
                if rows:
                    try:
                        cur.fast_executemany = True
                    except Exception:
                        pass
                    cur.executemany(
                        "INSERT INTO Dwh2.FactTransportLeg (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, PortOfLoadingKey, PortOfDischargeKey, [Order], TransportMode, VesselName, VesselLloydsIMO, VoyageFlightNo, CarrierBookingReference, BookingStatusCode, BookingStatusDescription, CarrierOrganizationKey, CreditorOrganizationKey, ActualArrivalDateKey, ActualArrivalTime, ActualDepartureDateKey, ActualDepartureTime, EstimatedArrivalDateKey, EstimatedArrivalTime, EstimatedDepartureDateKey, EstimatedDepartureTime, ScheduledArrivalDateKey, ScheduledArrivalTime, ScheduledDepartureDateKey, ScheduledDepartureTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        rows
                    )
                    if counters:
                        counters.add('Dwh2.FactTransportLeg', added=len(rows))

            # ChargeLineCollection under SubShipment/JobCosting
            charge_lines = sub_obj.get('ChargeLines') or []
            if sub_key and charge_lines:
                # Ensure idempotency for CSL at sub level
                try:
                    cur.execute("DELETE FROM Dwh2.FactChargeLine WHERE FactSubShipmentKey = ? AND Source='CSL'", sub_key)
                except Exception:
                    pass
                if counters is not None:
                    try:
                        print(f"      [debug] Preparing {len(charge_lines)} CSL ChargeLine row(s) for FactSubShipmentKey={sub_key}")
                    except Exception:
                        pass
                rows = []
                for cl in charge_lines:
                        br_code, br_name = cl['Branch']
                        brk = ensure_branch(cur, br_code, br_name) if br_code else None
                        dept_code, dept_name = cl['Department']
                        dept_key = None
                        if dept_code:
                            try:
                                dept_key = _upsert_scalar_dim(cur, 'Dwh2.DimDepartment', 'Code', dept_code, 'Name', dept_name or dept_code, key_col='DepartmentKey')
                            except Exception:
                                cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimDepartment WHERE Code=?) INSERT INTO Dwh2.DimDepartment (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimDepartment SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", dept_code, dept_code, dept_name or dept_code, dept_name or dept_code, dept_code)
                                cur.execute("SELECT DepartmentKey FROM Dwh2.DimDepartment WHERE Code=?", dept_code)
                                r = cur.fetchone(); dept_key = int(r[0]) if r else None
                        chg_code, chg_desc = cl['ChargeCode']
                        chg_group_code, chg_group_desc = cl['ChargeGroup']
                        cred_key = ensure_organization_min(cur, cl.get('CreditorKey') or '') if cl.get('CreditorKey') else None
                        debt_key = ensure_organization_min(cur, cl.get('DebtorKey') or '') if cl.get('DebtorKey') else None
                        sell_pt = cl['Sell'].get('PostedTransaction') if cl.get('Sell') else None
                        rows.append((
                            None, sub_key, None,
                            'CSL',
                            brk, dept_key,
                            chg_code or None,
                            chg_desc or None,
                            chg_group_code or None,
                            chg_group_desc or None,
                            cl.get('Description'),
                            int(cl.get('DisplaySequence') or '0') if str(cl.get('DisplaySequence') or '').isdigit() else 0,
                            cred_key, debt_key,
                            cl['Cost'].get('APInvoiceNumber') if cl.get('Cost') else None,
                            _datekey_from_iso(cl['Cost'].get('DueDate')) if cl.get('Cost') else None, _time_from_iso(cl['Cost'].get('DueDate')) if cl.get('Cost') else None,
                            (cl['Cost'].get('ExchangeRate') if cl.get('Cost') else None),
                            _datekey_from_iso(cl['Cost'].get('InvoiceDate')) if cl.get('Cost') else None, _time_from_iso(cl['Cost'].get('InvoiceDate')) if cl.get('Cost') else None,
                            1 if (str((cl['Cost'].get('IsPosted') if cl.get('Cost') else '') or '').lower() == 'true') else (0 if (str((cl['Cost'].get('IsPosted') if cl.get('Cost') else '') or '').lower() == 'false') else None),
                            (cl['Cost'].get('LocalAmount') if cl.get('Cost') else None),
                            (cl['Cost'].get('OSAmount') if cl.get('Cost') else None),
                            ensure_currency(cur, *(cl['Cost'].get('OSCurrency') or ('',''))) if cl.get('Cost') and cl['Cost'].get('OSCurrency') and cl['Cost'].get('OSCurrency')[0] else None,
                            (cl['Cost'].get('OSGSTVATAmount') if cl.get('Cost') else None),
                            (cl['Sell'].get('ExchangeRate') if cl.get('Sell') else None),
                            (cl['Sell'].get('GSTVAT') or (None,None))[0] if cl.get('Sell') else None,
                            (cl['Sell'].get('GSTVAT') or (None,None))[1] if cl.get('Sell') else None,
                            (cl['Sell'].get('InvoiceType') if cl.get('Sell') else None),
                            1 if (str((cl['Sell'].get('IsPosted') if cl.get('Sell') else '') or '').lower() == 'true') else (0 if (str((cl['Sell'].get('IsPosted') if cl.get('Sell') else '') or '').lower() == 'false') else None),
                            (cl['Sell'].get('LocalAmount') if cl.get('Sell') else None),
                            (cl['Sell'].get('OSAmount') if cl.get('Sell') else None),
                            ensure_currency(cur, *(cl['Sell'].get('OSCurrency') or ('',''))) if cl.get('Sell') and cl['Sell'].get('OSCurrency') and cl['Sell'].get('OSCurrency')[0] else None,
                            (cl['Sell'].get('OSGSTVATAmount') if cl.get('Sell') else None),
                            (sell_pt or (None,None,None,None,None,None))[0] if sell_pt else None,
                            (sell_pt or (None,None,None,None,None,None))[1] if sell_pt else None,
                            _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[2]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[2]) if sell_pt else None,
                            _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[3]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[3]) if sell_pt else None,
                            _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[4]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[4]) if sell_pt else None,
                            (sell_pt or (None,None,None,None,None,None))[5] if sell_pt else None,
                            cl.get('SupplierReference'),
                            cl.get('SellReference'),
                            company_key, simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                        ))
                if rows:
                    try:
                        cur.fast_executemany = True
                    except Exception:
                        pass
                    insert_placeholders = ",".join(["?"] * len(rows[0]))
                    cur.executemany(
                        "INSERT INTO Dwh2.FactChargeLine ("
                        "FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, BranchKey, DepartmentKey, "
                        "ChargeCode, ChargeCodeDescription, ChargeCodeGroup, ChargeCodeGroupDescription, [Description], DisplaySequence, "
                        "CreditorOrganizationKey, DebtorOrganizationKey, CostAPInvoiceNumber, CostDueDateKey, CostDueTime, CostExchangeRate, CostInvoiceDateKey, CostInvoiceTime, "
                        "CostIsPosted, CostLocalAmount, CostOSAmount, CostOSCurrencyKey, CostOSGSTVATAmount, "
                        "SellExchangeRate, SellGSTVATTaxCode, SellGSTVATDescription, SellInvoiceType, SellIsPosted, SellLocalAmount, SellOSAmount, SellOSCurrencyKey, SellOSGSTVATAmount, "
                        "SellPostedTransactionNumber, SellPostedTransactionType, SellTransactionDateKey, SellTransactionTime, SellDueDateKey, SellDueTime, SellFullyPaidDateKey, SellFullyPaidTime, "
                        "SellOutstandingAmount, SupplierReference, SellReference, CompanyKey, EventUserKey, DataProviderKey) VALUES (" + insert_placeholders + ")",
                        rows
                    )
                    if counters is not None:
                        try:
                            print(f"      [debug] Inserted CSL ChargeLines rows={len(rows)} for FactSubShipmentKey={sub_key}")
                        except Exception:
                            pass
                    if counters:
                        counters.add('Dwh2.FactChargeLine', added=len(rows))

            # PackingLineCollection under SubShipment (CSL)
            packs2 = sub_obj.get('PackingLines') or []
            if sub_key and packs2:
                try:
                    cur.execute("DELETE FROM Dwh2.FactPackingLine WHERE FactSubShipmentKey = ? AND Source = 'CSL'", sub_key)
                except Exception:
                    pass
                rows = []
                for pl in packs2:
                    len_code, len_desc = pl.get('LengthUnit') or (None, None)
                    vol_code, vol_desc = pl.get('VolumeUnit') or (None, None)
                    wei_code, wei_desc = pl.get('WeightUnit') or (None, None)
                    pack_code, pack_desc = pl.get('PackType') or (None, None)
                    lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                    volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                    weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                    packk = ensure_unit(cur, pack_code, pack_desc, 'Packs') if pack_code else None
                    rows.append((
                        None, sub_key, None,
                        'CSL',
                        pl.get('CommodityCode'), pl.get('CommodityDescription'), pl.get('ContainerLink'), pl.get('ContainerNumber'), pl.get('ContainerPackingOrder'), pl.get('CountryOfOriginCode'), pl.get('DetailedDescription'), pl.get('EndItemNo'), pl.get('ExportReferenceNumber'), pl.get('GoodsDescription'), pl.get('HarmonisedCode'),
                        pl.get('Height'), pl.get('Length'), pl.get('Width'), lenk,
                        pl.get('ImportReferenceNumber'), pl.get('ItemNo'),
                        pl.get('LastKnownCFSStatusCode'), _datekey_from_iso(pl.get('LastKnownCFSStatusDate')), _time_from_iso(pl.get('LastKnownCFSStatusDate')),
                        pl.get('LinePrice'), pl.get('Link'), pl.get('LoadingMeters'), pl.get('MarksAndNos'), pl.get('OutturnComment'), pl.get('OutturnDamagedQty'), pl.get('OutturnedHeight'), pl.get('OutturnedLength'), pl.get('OutturnedVolume'), pl.get('OutturnedWeight'), pl.get('OutturnedWidth'), pl.get('OutturnPillagedQty'), pl.get('OutturnQty'), pl.get('PackingLineID'), pl.get('PackQty'), packk, pl.get('ReferenceNumber'),
                        (1 if pl.get('RequiresTemperatureControl') is True else (0 if pl.get('RequiresTemperatureControl') is False else None)),
                        pl.get('Volume'), volk, pl.get('Weight'), weik,
                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                    ))
                if rows:
                    try:
                        cur.fast_executemany = True
                    except Exception:
                        pass
                    insert_placeholders = ",".join(["?"] * len(rows[0]))
                    cur.executemany(
                        "INSERT INTO Dwh2.FactPackingLine (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, CommodityCode, CommodityDescription, ContainerLink, ContainerNumber, ContainerPackingOrder, CountryOfOriginCode, DetailedDescription, EndItemNo, ExportReferenceNumber, GoodsDescription, HarmonisedCode, Height, Length, Width, LengthUnitKey, ImportReferenceNumber, ItemNo, LastKnownCFSStatusCode, LastKnownCFSStatusDateKey, LastKnownCFSStatusTime, LinePrice, [Link], LoadingMeters, MarksAndNos, OutturnComment, OutturnDamagedQty, OutturnedHeight, OutturnedLength, OutturnedVolume, OutturnedWeight, OutturnedWidth, OutturnPillagedQty, OutturnQty, PackingLineID, PackQty, PackTypeUnitKey, ReferenceNumber, RequiresTemperatureControl, Volume, VolumeUnitKey, Weight, WeightUnitKey, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (" + insert_placeholders + ")",
                        rows
                    )
                    if counters:
                        counters.add('Dwh2.FactPackingLine', added=len(rows))
            # ContainerCollection under SubShipment (CSL)
            conts2 = sub_obj.get('Containers') or []
            if sub_key and conts2:
                try:
                    cur.execute("DELETE FROM Dwh2.FactContainer WHERE FactSubShipmentKey = ? AND Source = 'CSL'", sub_key)
                except Exception:
                    pass
                rows = []
                for ct in conts2:
                    len_code, len_desc = ct.get('LengthUnit') or (None, None)
                    vol_code, vol_desc = ct.get('VolumeUnit') or (None, None)
                    wei_code, wei_desc = ct.get('WeightUnit') or (None, None)
                    lenk = ensure_unit(cur, len_code, len_desc, 'Length') if len_code else None
                    volk = ensure_unit(cur, vol_code, vol_desc, 'Volume') if vol_code else None
                    weik = ensure_unit(cur, wei_code, wei_desc, 'Weight') if wei_code else None
                    ct_code, ct_desc, ct_iso, cat_code, cat_desc = ct.get('ContainerType') or (None, None, None, None, None)
                    fclc, fcld = ct.get('FCL_LCL_AIR') or (None, None)
                    rows.append((
                        None, sub_key, None,
                        'CSL',
                        ct.get('ContainerJobID'), ct.get('ContainerNumber'), ct.get('Link'),
                        ct_code, ct_desc, ct_iso, cat_code, cat_desc,
                        ct.get('DeliveryMode'), fclc, fcld, ct.get('ContainerCount'),
                        ct.get('Seal'), ct.get('SealPartyTypeCode'), ct.get('SecondSeal'), ct.get('SecondSealPartyTypeCode'), ct.get('ThirdSeal'), ct.get('ThirdSealPartyTypeCode'), ct.get('StowagePosition'),
                        lenk, volk, weik,
                        ct.get('TotalHeight'), ct.get('TotalLength'), ct.get('TotalWidth'), ct.get('TareWeight'), ct.get('GrossWeight'), ct.get('GoodsWeight'), ct.get('VolumeCapacity'), ct.get('WeightCapacity'), ct.get('DunnageWeight'), ct.get('OverhangBack'), ct.get('OverhangFront'), ct.get('OverhangHeight'), ct.get('OverhangLeft'), ct.get('OverhangRight'), ct.get('HumidityPercent'), ct.get('AirVentFlow'), ct.get('AirVentFlowRateUnitCode'),
                        (1 if ct.get('NonOperatingReefer') is True else (0 if ct.get('NonOperatingReefer') is False else None)),
                        (1 if ct.get('IsCFSRegistered') is True else (0 if ct.get('IsCFSRegistered') is False else None)),
                        (1 if ct.get('IsControlledAtmosphere') is True else (0 if ct.get('IsControlledAtmosphere') is False else None)),
                        (1 if ct.get('IsDamaged') is True else (0 if ct.get('IsDamaged') is False else None)),
                        (1 if ct.get('IsEmptyContainer') is True else (0 if ct.get('IsEmptyContainer') is False else None)),
                        (1 if ct.get('IsSealOk') is True else (0 if ct.get('IsSealOk') is False else None)),
                        (1 if ct.get('IsShipperOwned') is True else (0 if ct.get('IsShipperOwned') is False else None)),
                        (1 if ct.get('ArrivalPickupByRail') is True else (0 if ct.get('ArrivalPickupByRail') is False else None)),
                        (1 if ct.get('DepartureDeliveryByRail') is True else (0 if ct.get('DepartureDeliveryByRail') is False else None)),
                        _datekey_from_iso(ct.get('ArrivalSlotDateTime')), _time_from_iso(ct.get('ArrivalSlotDateTime')),
                        _datekey_from_iso(ct.get('DepartureSlotDateTime')), _time_from_iso(ct.get('DepartureSlotDateTime')),
                        _datekey_from_iso(ct.get('EmptyReadyForReturn')), _time_from_iso(ct.get('EmptyReadyForReturn')),
                        _datekey_from_iso(ct.get('FCLWharfGateIn')), _time_from_iso(ct.get('FCLWharfGateIn')),
                        _datekey_from_iso(ct.get('FCLWharfGateOut')), _time_from_iso(ct.get('FCLWharfGateOut')),
                        _datekey_from_iso(ct.get('FCLStorageCommences')), _time_from_iso(ct.get('FCLStorageCommences')),
                        _datekey_from_iso(ct.get('LCLUnpack')), _time_from_iso(ct.get('LCLUnpack')),
                        _datekey_from_iso(ct.get('PackDate')), _time_from_iso(ct.get('PackDate')),
                        company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                    ))
                if rows:
                    try:
                        cur.fast_executemany = True
                    except Exception:
                        pass
                    insert_placeholders = ",".join(["?"] * len(rows[0]))
                    sql = (
                        "INSERT INTO Dwh2.FactContainer (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, ContainerJobID, ContainerNumber, [Link], ContainerTypeCode, ContainerTypeDescription, ContainerTypeISOCode, ContainerCategoryCode, ContainerCategoryDescription, DeliveryMode, FCL_LCL_AIR_Code, FCL_LCL_AIR_Description, ContainerCount, Seal, SealPartyTypeCode, SecondSeal, SecondSealPartyTypeCode, ThirdSeal, ThirdSealPartyTypeCode, StowagePosition, LengthUnitKey, VolumeUnitKey, WeightUnitKey, TotalHeight, TotalLength, TotalWidth, TareWeight, GrossWeight, GoodsWeight, VolumeCapacity, WeightCapacity, DunnageWeight, OverhangBack, OverhangFront, OverhangHeight, OverhangLeft, OverhangRight, HumidityPercent, AirVentFlow, AirVentFlowRateUnitCode, NonOperatingReefer, IsCFSRegistered, IsControlledAtmosphere, IsDamaged, IsEmptyContainer, IsSealOk, IsShipperOwned, ArrivalPickupByRail, DepartureDeliveryByRail, ArrivalSlotDateKey, ArrivalSlotTime, DepartureSlotDateKey, DepartureSlotTime, EmptyReadyForReturnDateKey, EmptyReadyForReturnTime, FCLWharfGateInDateKey, FCLWharfGateInTime, FCLWharfGateOutDateKey, FCLWharfGateOutTime, FCLStorageCommencesDateKey, FCLStorageCommencesTime, LCLUnpackDateKey, LCLUnpackTime, PackDateKey, PackTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (" + insert_placeholders + ")"
                    )
                    cur.executemany(sql, rows)
                    if counters:
                        counters.add('Dwh2.FactContainer', added=len(rows))
                # AdditionalReferenceCollection under SubShipment
                add_refs = sub_obj.get('AdditionalReferences') or []
                if sub_key and add_refs:
                    rows = []
                    for r in add_refs:
                        coi_code, coi_name = r.get('CountryOfIssue') or (None, None)
                        coi_key = ensure_country(cur, coi_code, coi_name) if coi_code else None
                        rows.append((
                            None, sub_key, None,
                            'CSL',
                            r.get('TypeCode'), r.get('TypeDescription'), r.get('ReferenceNumber'), r.get('ContextInformation'),
                            coi_key,
                            _datekey_from_iso(r.get('IssueDate')), _time_from_iso(r.get('IssueDate')),
                            company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                        ))
                    try:
                        cur.fast_executemany = True
                    except Exception:
                        pass
                    cur.executemany(
                        "INSERT INTO Dwh2.FactAdditionalReference (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, TypeCode, TypeDescription, ReferenceNumber, ContextInformation, CountryOfIssueKey, IssueDateKey, IssueTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        rows
                    )
                    if counters:
                        counters.add('Dwh2.FactAdditionalReference', added=len(rows))

    # DateCollection for CSL: Shipment-level
    if fact_ship_key and fact.get("DateCollection"):
        try:
            cur.fast_executemany = True
        except Exception:
            pass
        rows = fact["DateCollection"]
        if rows:
            try:
                cur.execute("DELETE FROM Dwh2.FactEventDate WHERE FactShipmentKey = ? AND Source = 'CSL'", fact_ship_key)
            except Exception:
                pass
            params = []
            for r in rows:
                params.append((
                    fact_ship_key, None, None,
                    'CSL', r.get('DateTypeCode'), r.get('DateTypeDescription'),
                    r.get('DateKey'), r.get('Time'), r.get('DateTimeText'), r.get('IsEstimate'), r.get('Value'), r.get('TimeZone'),
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
            cur.executemany(
                "INSERT INTO Dwh2.FactEventDate (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, DateTypeCode, DateTypeDescription, DateKey, [Time], DateTimeText, IsEstimate, [Value], TimeZone, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params
            )
            if counters:
                counters.add('Dwh2.FactEventDate', added=len(params))

    # Exceptions for CSL (attach to Shipment)
    if fact_ship_key and fact.get("Exceptions"):
        try:
            cur.fast_executemany = True
        except Exception:
            pass
        rows = fact["Exceptions"]
        if rows:
            # Replace existing exceptions for this Shipment to ensure idempotency
            try:
                cur.execute(
                    "DELETE FROM Dwh2.FactException WHERE FactShipmentKey = ? AND Source = 'CSL'",
                    fact_ship_key,
                )
            except Exception:
                pass
            params = []
            for r in rows:
                params.append((
                    fact_ship_key,  # FactShipmentKey
                    None,           # FactARKey
                    r.get('Source'),
                    r.get('ExceptionId'),
                    r.get('Code'), r.get('Type'), r.get('Severity'), r.get('Status'), r.get('Description'), r.get('IsResolved'),
                    r.get('Actioned'), r.get('ActionedDateKey'), r.get('ActionedTime'), r.get('Category'), r.get('EventDateKey'), r.get('EventTime'), r.get('DurationHours'), r.get('LocationCode'), r.get('LocationName'), r.get('Notes'), r.get('StaffCode'), r.get('StaffName'),
                    r.get('RaisedDateKey'), r.get('RaisedTime'), r.get('ResolvedDateKey'), r.get('ResolvedTime'),
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
            cur.executemany(
                "INSERT INTO Dwh2.FactException (FactShipmentKey, FactAccountsReceivableTransactionKey, Source, ExceptionId, Code, [Type], Severity, [Status], [Description], IsResolved, Actioned, ActionedDateKey, ActionedTime, Category, EventDateKey, EventTime, DurationHours, LocationCode, LocationName, Notes, StaffCode, StaffName, RaisedDateKey, RaisedTime, ResolvedDateKey, ResolvedTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params
            )
            if counters:
                counters.add('Dwh2.FactException', added=len(params))

    
    # Milestones for CSL
    if fact_ship_key and fact.get('Milestones'):
        # Replace existing milestones for this Shipment to ensure idempotency
        try:
            cur.execute(
                "DELETE FROM Dwh2.FactMilestone WHERE FactShipmentKey = ? AND Source = 'CSL'",
                fact_ship_key,
            )
        except Exception:
            pass
        rows = []
        for r in fact['Milestones']:
            rows.append((
                fact_ship_key,
                None,
                r.get('Source'), r.get('EventCode'), r.get('Description'), r.get('Sequence'),
                r.get('ActualDateKey'), r.get('ActualTime'), r.get('EstimatedDateKey'), r.get('EstimatedTime'), r.get('ConditionReference'), r.get('ConditionType'),
                company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
            ))
        if rows:
            try:
                cur.fast_executemany = True
            except Exception:
                pass
            cur.executemany(
                "INSERT INTO Dwh2.FactMilestone (FactShipmentKey, FactAccountsReceivableTransactionKey, Source, EventCode, [Description], [Sequence], ActualDateKey, ActualTime, EstimatedDateKey, EstimatedTime, ConditionReference, ConditionType, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows
            )
            if counters:
                counters.add('Dwh2.FactMilestone', added=len(rows))
    # Notes for CSL
    if fact_ship_key and fact.get('Notes'):
        try:
            cur.fast_executemany = True
        except Exception:
            pass
        rows = fact['Notes']
        if rows:
            # Replace existing notes for this Shipment to ensure idempotency
            try:
                cur.execute(
                    "DELETE FROM Dwh2.FactNote WHERE FactShipmentKey = ? AND Source = 'CSL'",
                    fact_ship_key,
                )
            except Exception:
                pass
            params = []
            for r in rows:
                params.append((
                    fact_ship_key,
                    None,
                    r.get('Source'),
                    r.get('Description'), r.get('IsCustomDescription'), r.get('NoteText'),
                    r.get('NoteContextCode'), r.get('NoteContextDescription'),
                    r.get('VisibilityCode'), r.get('VisibilityDescription'),
                    r.get('Content'),
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
            cur.executemany(
                "INSERT INTO Dwh2.FactNote (FactShipmentKey, FactAccountsReceivableTransactionKey, Source, [Description], IsCustomDescription, NoteText, NoteContextCode, NoteContextDescription, VisibilityCode, VisibilityDescription, Content, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params
            )
            if counters:
                counters.add('Dwh2.FactNote', added=len(params))
    # Message numbers (CSL) with idempotent insert
    if fact_ship_key and fact.get("MessageNumbers"):
        try:
            cur.fast_executemany = True
        except Exception:
            pass
        rows = []
        for mtype, mval in fact.get("MessageNumbers"):
            rows.append((
                fact_ship_key, mval, (mtype or None),
                fact_ship_key,   # FactShipmentKey
                None,            # FactARKey
                'CSL',           # Source
                mval, (mtype or None),
                company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
            ))
        if rows:
            cur.executemany(
                "IF NOT EXISTS (SELECT 1 FROM Dwh2.FactMessageNumber WHERE FactShipmentKey=? AND [Value]=? AND ISNULL([Type],'') = ISNULL(?,'')) "
                "INSERT INTO Dwh2.FactMessageNumber (FactShipmentKey, FactAccountsReceivableTransactionKey, Source, [Value], [Type], CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?)",
                rows
            )
            if counters:
                counters.add('Dwh2.FactMessageNumber', added=len(rows))

    # Shipment-level AdditionalReferenceCollection (header)
    if fact_ship_key and fact.get('AdditionalReferencesShipment'):
        add_refs = fact.get('AdditionalReferencesShipment') or []
        if add_refs:
            # Ensure idempotency: replace existing shipment-level CSL additional references
            try:
                cur.execute("DELETE FROM Dwh2.FactAdditionalReference WHERE FactShipmentKey = ? AND FactSubShipmentKey IS NULL AND Source = 'CSL'", fact_ship_key)
            except Exception:
                pass
            rows = []
            for r in add_refs:
                coi_code, coi_name = r.get('CountryOfIssue') or (None, None)
                coi_key = ensure_country(cur, coi_code, coi_name) if coi_code else None
                rows.append((
                    fact_ship_key, None, None,
                    'CSL',
                    r.get('TypeCode'), r.get('TypeDescription'), r.get('ReferenceNumber'), r.get('ContextInformation'),
                    coi_key,
                    _datekey_from_iso(r.get('IssueDate')), _time_from_iso(r.get('IssueDate')),
                    company_key, simple_keys.get('DepartmentKey'), simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
            try:
                cur.fast_executemany = True
            except Exception:
                pass
            cur.executemany(
                "INSERT INTO Dwh2.FactAdditionalReference (FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, TypeCode, TypeDescription, ReferenceNumber, ContextInformation, CountryOfIssueKey, IssueDateKey, IssueTime, CompanyKey, DepartmentKey, EventUserKey, DataProviderKey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows
            )
            if counters:
                counters.add('Dwh2.FactAdditionalReference', added=len(rows))

    # Shipment-level ChargeLineCollection (under JobCosting)
    if fact_ship_key and fact.get('ChargeLinesShipment'):
        cls = fact.get('ChargeLinesShipment') or []
        if cls:
            try:
                # Replace existing shipment-level charge lines from CSL for idempotency
                cur.execute("DELETE FROM Dwh2.FactChargeLine WHERE FactShipmentKey = ? AND FactSubShipmentKey IS NULL AND Source='CSL'", fact_ship_key)
            except Exception:
                pass
            if counters is not None:
                try:
                    print(f"  [debug] Preparing {len(cls)} CSL Shipment-level ChargeLine row(s) for FactShipmentKey={fact_ship_key}")
                except Exception:
                    pass
            rows = []
            for cl in cls:
                br_code, br_name = cl['Branch']
                brk = ensure_branch(cur, br_code, br_name) if br_code else None
                dept_code, dept_name = cl['Department']
                dept_key = None
                if dept_code:
                    try:
                        dept_key = _upsert_scalar_dim(cur, 'Dwh2.DimDepartment', 'Code', dept_code, 'Name', dept_name or dept_code, key_col='DepartmentKey')
                    except Exception:
                        cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimDepartment WHERE Code=?) INSERT INTO Dwh2.DimDepartment (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimDepartment SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", dept_code, dept_code, dept_name or dept_code, dept_name or dept_code, dept_code)
                        cur.execute("SELECT DepartmentKey FROM Dwh2.DimDepartment WHERE Code=?", dept_code)
                        r = cur.fetchone(); dept_key = int(r[0]) if r else None
                chg_code, chg_desc = cl['ChargeCode']
                chg_group_code, chg_group_desc = cl['ChargeGroup']
                cred_key = ensure_organization_min(cur, cl.get('CreditorKey') or '') if cl.get('CreditorKey') else None
                debt_key = ensure_organization_min(cur, cl.get('DebtorKey') or '') if cl.get('DebtorKey') else None
                sell_pt = cl['Sell'].get('PostedTransaction') if cl.get('Sell') else None
                rows.append((
                    fact_ship_key, None, None,
                    'CSL',
                    brk, dept_key,
                    chg_code or None,
                    chg_desc or None,
                    chg_group_code or None,
                    chg_group_desc or None,
                    cl.get('Description'),
                    int(cl.get('DisplaySequence') or '0') if str(cl.get('DisplaySequence') or '').isdigit() else 0,
                    cred_key, debt_key,
                    cl['Cost'].get('APInvoiceNumber') if cl.get('Cost') else None,
                    _datekey_from_iso(cl['Cost'].get('DueDate')) if cl.get('Cost') else None, _time_from_iso(cl['Cost'].get('DueDate')) if cl.get('Cost') else None,
                    (cl['Cost'].get('ExchangeRate') if cl.get('Cost') else None),
                    _datekey_from_iso(cl['Cost'].get('InvoiceDate')) if cl.get('Cost') else None, _time_from_iso(cl['Cost'].get('InvoiceDate')) if cl.get('Cost') else None,
                    1 if (str((cl['Cost'].get('IsPosted') if cl.get('Cost') else '') or '').lower() == 'true') else (0 if (str((cl['Cost'].get('IsPosted') if cl.get('Cost') else '') or '').lower() == 'false') else None),
                    (cl['Cost'].get('LocalAmount') if cl.get('Cost') else None),
                    (cl['Cost'].get('OSAmount') if cl.get('Cost') else None),
                    ensure_currency(cur, *(cl['Cost'].get('OSCurrency') or ('',''))) if cl.get('Cost') and cl['Cost'].get('OSCurrency') and cl['Cost'].get('OSCurrency')[0] else None,
                    (cl['Cost'].get('OSGSTVATAmount') if cl.get('Cost') else None),
                    (cl['Sell'].get('ExchangeRate') if cl.get('Sell') else None),
                    (cl['Sell'].get('GSTVAT') or (None,None))[0] if cl.get('Sell') else None,
                    (cl['Sell'].get('GSTVAT') or (None,None))[1] if cl.get('Sell') else None,
                    (cl['Sell'].get('InvoiceType') if cl.get('Sell') else None),
                    1 if (str((cl['Sell'].get('IsPosted') if cl.get('Sell') else '') or '').lower() == 'true') else (0 if (str((cl['Sell'].get('IsPosted') if cl.get('Sell') else '') or '').lower() == 'false') else None),
                    (cl['Sell'].get('LocalAmount') if cl.get('Sell') else None),
                    (cl['Sell'].get('OSAmount') if cl.get('Sell') else None),
                    ensure_currency(cur, *(cl['Sell'].get('OSCurrency') or ('',''))) if cl.get('Sell') and cl['Sell'].get('OSCurrency') and cl['Sell'].get('OSCurrency')[0] else None,
                    (cl['Sell'].get('OSGSTVATAmount') if cl.get('Sell') else None),
                    (sell_pt or (None,None,None,None,None,None))[0] if sell_pt else None,
                    (sell_pt or (None,None,None,None,None,None))[1] if sell_pt else None,
                    _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[2]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[2]) if sell_pt else None,
                    _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[3]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[3]) if sell_pt else None,
                    _datekey_from_iso((sell_pt or (None,None,None,None,None,None))[4]) if sell_pt else None, _time_from_iso((sell_pt or (None,None,None,None,None,None))[4]) if sell_pt else None,
                    (sell_pt or (None,None,None,None,None,None))[5] if sell_pt else None,
                    cl.get('SupplierReference'),
                    cl.get('SellReference'),
                    company_key, simple_keys.get('UserKey'), simple_keys.get('DataProviderKey')
                ))
            if rows:
                try:
                    cur.fast_executemany = True
                except Exception:
                    pass
                insert_placeholders = ",".join(["?"] * len(rows[0]))
                cur.executemany(
                    "INSERT INTO Dwh2.FactChargeLine ("
                    "FactShipmentKey, FactSubShipmentKey, FactAccountsReceivableTransactionKey, Source, BranchKey, DepartmentKey, "
                    "ChargeCode, ChargeCodeDescription, ChargeCodeGroup, ChargeCodeGroupDescription, [Description], DisplaySequence, "
                    "CreditorOrganizationKey, DebtorOrganizationKey, CostAPInvoiceNumber, CostDueDateKey, CostDueTime, CostExchangeRate, CostInvoiceDateKey, CostInvoiceTime, "
                    "CostIsPosted, CostLocalAmount, CostOSAmount, CostOSCurrencyKey, CostOSGSTVATAmount, "
                    "SellExchangeRate, SellGSTVATTaxCode, SellGSTVATDescription, SellInvoiceType, SellIsPosted, SellLocalAmount, SellOSAmount, SellOSCurrencyKey, SellOSGSTVATAmount, "
                    "SellPostedTransactionNumber, SellPostedTransactionType, SellTransactionDateKey, SellTransactionTime, SellDueDateKey, SellDueTime, SellFullyPaidDateKey, SellFullyPaidTime, "
                    "SellOutstandingAmount, SupplierReference, SellReference, CompanyKey, EventUserKey, DataProviderKey) VALUES (" + insert_placeholders + ")",
                    rows
                )
                if counters is not None:
                    try:
                        print(f"  [debug] Inserted CSL Shipment-level ChargeLines rows={len(rows)} for FactShipmentKey={fact_ship_key}")
                    except Exception:
                        pass
                if counters:
                    counters.add('Dwh2.FactChargeLine', added=len(rows))

    # Shipment-level JobCosting header (CSL) -> FactJobCosting (parent = Shipment)
    if fact_ship_key and fact.get('JobCostingHeaderShipment'):
        h = fact['JobCostingHeaderShipment']  # type: ignore[assignment]
        brk = None; dpk = None; hbk = None; osk = None; curk2 = None
        br_code, br_name = (h.get('Branch') or (None, None))
        if br_code:
            brk = ensure_branch(cur, br_code, br_name)
        dp_code, dp_name = (h.get('Department') or (None, None))
        if dp_code:
            try:
                dpk = _upsert_scalar_dim(cur, 'Dwh2.DimDepartment', 'Code', dp_code, 'Name', dp_name or dp_code, key_col='DepartmentKey')
            except Exception:
                cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimDepartment WHERE Code=?) INSERT INTO Dwh2.DimDepartment (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimDepartment SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", dp_code, dp_code, dp_name or dp_code, dp_name or dp_code, dp_code)
                cur.execute("SELECT DepartmentKey FROM Dwh2.DimDepartment WHERE Code=?", dp_code)
                r = cur.fetchone(); dpk = int(r[0]) if r else None
        hb_code, hb_name = (h.get('HomeBranch') or (None, None))
        if hb_code:
            hbk = ensure_branch(cur, hb_code, hb_name)
        os_code, os_name = (h.get('OperationsStaff') or (None, None))
        if os_code:
            try:
                osk = _upsert_scalar_dim(cur, 'Dwh2.DimUser', 'Code', os_code, 'Name', os_name or os_code, key_col='UserKey')
            except Exception:
                cur.execute("IF NOT EXISTS (SELECT 1 FROM Dwh2.DimUser WHERE Code=?) INSERT INTO Dwh2.DimUser (Code,Name) VALUES (?,?); ELSE UPDATE Dwh2.DimUser SET Name=?,UpdatedAt=SYSUTCDATETIME() WHERE Code=?;", os_code, os_code, os_name or os_code, os_name or os_code, os_code)
                cur.execute("SELECT UserKey FROM Dwh2.DimUser WHERE Code=?", os_code)
                r = cur.fetchone(); osk = int(r[0]) if r else None
        cur_code2, cur_desc2 = (h.get('Currency') or (None, None))
        if cur_code2:
            curk2 = ensure_currency(cur, cur_code2, cur_desc2)
        def _dec_or_none2(v: Optional[str]) -> Optional[float]:
            try:
                return float(v) if v is not None and v != '' else None
            except Exception:
                return None
        # Detect pre-existence for accurate counters
        existed_ship = False
        try:
            cur.execute("SELECT 1 FROM Dwh2.FactJobCosting WHERE FactShipmentKey=?", fact_ship_key)
            existed_ship = cur.fetchone() is not None
        except Exception:
            existed_ship = False
        cols_jc = [
            ('FactShipmentKey', fact_ship_key),
            ('FactSubShipmentKey', None),
            ('FactAccountsReceivableTransactionKey', None),
            ('Source', 'CSL'),
            ('BranchKey', brk), ('DepartmentKey', dpk), ('HomeBranchKey', hbk), ('OperationsStaffKey', osk), ('CurrencyKey', curk2),
            ('ClientContractNumber', h.get('ClientContractNumber')),
            ('AccrualNotRecognized', _dec_or_none2(h.get('AccrualNotRecognized'))),
            ('AccrualRecognized', _dec_or_none2(h.get('AccrualRecognized'))),
            ('AgentRevenue', _dec_or_none2(h.get('AgentRevenue'))),
            ('LocalClientRevenue', _dec_or_none2(h.get('LocalClientRevenue'))),
            ('OtherDebtorRevenue', _dec_or_none2(h.get('OtherDebtorRevenue'))),
            ('TotalAccrual', _dec_or_none2(h.get('TotalAccrual'))),
            ('TotalCost', _dec_or_none2(h.get('TotalCost'))),
            ('TotalJobProfit', _dec_or_none2(h.get('TotalJobProfit'))),
            ('TotalRevenue', _dec_or_none2(h.get('TotalRevenue'))),
            ('TotalWIP', _dec_or_none2(h.get('TotalWIP'))),
            ('WIPNotRecognized', _dec_or_none2(h.get('WIPNotRecognized'))),
            ('WIPRecognized', _dec_or_none2(h.get('WIPRecognized'))),
            ('CompanyKey', company_key), ('EventUserKey', simple_keys.get('UserKey')), ('DataProviderKey', simple_keys.get('DataProviderKey')),
        ]
        coln = [c for c,_ in cols_jc]; vals = [v for _,v in cols_jc]
        placeholders = ','.join(['?']*len(coln))
        set_clause = ', '.join([f'[{c}]=?' for c in coln[3:]]) + ', UpdatedAt=SYSUTCDATETIME()'
        set_vals = vals[3:]
        cur.execute(
            "IF EXISTS (SELECT 1 FROM Dwh2.FactJobCosting WHERE FactShipmentKey=?) "
            "UPDATE Dwh2.FactJobCosting SET " + set_clause + " WHERE FactShipmentKey=? "
            "ELSE INSERT INTO Dwh2.FactJobCosting (" + ','.join('['+c+']' for c in coln) + ") VALUES (" + placeholders + ")",
            fact_ship_key, *set_vals, fact_ship_key, *vals
        )
        if counters:
            if existed_ship:
                counters.add('Dwh2.FactJobCosting', updated=1)
            else:
                counters.add('Dwh2.FactJobCosting', added=1)


def main(argv):
    start_dt = datetime.now()
    start_perf = time.perf_counter()
    ap = argparse.ArgumentParser()
    # Modo día o rango (mutuamente excluyentes)
    ap.add_argument("--date", help="Fecha en formato YYYYMMDD (carpeta bajo XMLS_COL)")
    ap.add_argument("--from", dest="from_date", help="Fecha inicio (YYYYMMDD)")
    ap.add_argument("--to", dest="to_date", help="Fecha fin (YYYYMMDD)")
    ap.add_argument("--only", choices=["AR", "CSL"], help="Procesar solo AR o CSL")
    ap.add_argument("--limit", type=int, help="Máximo de archivos a procesar")
    ap.add_argument("--quiet", action="store_true", help="Suprime logs de fallback esperados para inserts de dimensiones")
    ap.add_argument("--verbose", action="store_true", help="Muestra un resumen por archivo de tablas afectadas (added/updated)")
    args = ap.parse_args(argv[1:])

    global QUIET
    # Respect environment default; only force quiet when flag is explicitly provided
    if args.quiet:
        QUIET = True

    # Validación de argumentos día vs rango
    has_range = bool(args.from_date or args.to_date)
    if has_range and not (args.from_date and args.to_date):
        print("Debe indicar ambos --from y --to para el modo rango")
        return 2
    if not args.date and not has_range:
        print("Debe indicar --date o bien --from y --to")
        return 2

    # Construir lista de fechas a procesar (YYYYMMDD como str)
    dates: list[str] = []
    if has_range:
        try:
            start_d = datetime.strptime(args.from_date, "%Y%m%d")  # type: ignore[arg-type]
            end_d = datetime.strptime(args.to_date, "%Y%m%d")      # type: ignore[arg-type]
        except Exception:
            print("Formato de fecha inválido. Use YYYYMMDD")
            return 2
        if end_d < start_d:
            print("--to debe ser mayor o igual a --from")
            return 2
        d = start_d
        while d <= end_d:
            dates.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)
    else:
        dates = [args.date]

    cnxn = connect()
    processed = 0
    last_commit = 0
    try:
        cur = cnxn.cursor()
        for date_str in dates:
            date_folder = os.path.join(XML_ROOT, date_str)
            if not os.path.isdir(date_folder):
                # En modo rango: continuar si falta carpeta; en modo día: mantener comportamiento actual de error
                if has_range:
                    print(f"(skip) No existe la carpeta de fecha: {date_folder}")
                    continue
                else:
                    print(f"No existe la carpeta de fecha: {date_folder}")
                    # Resumen de tiempos (ejecución corta)
                    end_dt = datetime.now()
                    elapsed = time.perf_counter() - start_perf
                    duration = timedelta(seconds=elapsed)
                    print(f"Inicio: {start_dt:%Y-%m-%d %H:%M:%S}")
                    print(f"Fin: {end_dt:%Y-%m-%d %H:%M:%S}")
                    print(f"Duración: {duration}")
                    return 1

            ar_files = [] if args.only == "CSL" else sorted(glob(os.path.join(date_folder, "AR_*.xml")))
            csl_files = [] if args.only == "AR" else sorted(glob(os.path.join(date_folder, "CSL*.xml")))
            if args.limit:
                ar_files = ar_files[: args.limit]
                csl_files = csl_files[: max(0, args.limit - len(ar_files))]

            # AR
            for p in ar_files:
                counters = TableCounter() if args.verbose else None
                dims, fact = parse_ar(p)
                upsert_ar(cur, dims, fact, counters)
                # Control ingestion logging (AR)
                src, fname, dkey, tstr = _parse_file_ingestion(p, folder_date=date_str)
                pre_exists = None
                try:
                    cur.execute("SELECT 1 FROM Dwh2.FactFileIngestion WHERE FileName=?", fname)
                    pre_exists = cur.fetchone()
                except Exception:
                    pre_exists = None
                _insert_file_ingestion(cur, src, fname, dkey, tstr)
                if counters is not None and fname:
                    try:
                        cur.execute("SELECT 1 FROM Dwh2.FactFileIngestion WHERE FileName=?", fname)
                        post_exists = cur.fetchone()
                        if not pre_exists and post_exists:
                            counters.add('Dwh2.FactFileIngestion', added=1)
                    except Exception:
                        pass
                last_commit += 1
                if last_commit >= max(1, COMMIT_EVERY):
                    cnxn.commit()
                    last_commit = 0
                processed += 1
                print(f"AR OK: {os.path.basename(p)}")
                # Per-file resume
                if counters is not None:
                    for line in counters.summary_lines():
                        print("  ", line)
            # CSL
            for p in csl_files:
                counters = TableCounter() if args.verbose else None
                dims, fact = parse_csl(p)
                upsert_csl(cur, dims, fact, counters)
                # Control ingestion logging (CSL)
                src, fname, dkey, tstr = _parse_file_ingestion(p, folder_date=date_str)
                pre_exists = None
                try:
                    cur.execute("SELECT 1 FROM Dwh2.FactFileIngestion WHERE FileName=?", fname)
                    pre_exists = cur.fetchone()
                except Exception:
                    pre_exists = None
                _insert_file_ingestion(cur, src, fname, dkey, tstr)
                if counters is not None and fname:
                    try:
                        cur.execute("SELECT 1 FROM Dwh2.FactFileIngestion WHERE FileName=?", fname)
                        post_exists = cur.fetchone()
                        if not pre_exists and post_exists:
                            counters.add('Dwh2.FactFileIngestion', added=1)
                    except Exception:
                        pass
                last_commit += 1
                if last_commit >= max(1, COMMIT_EVERY):
                    cnxn.commit()
                    last_commit = 0
                processed += 1
                print(f"CSL OK: {os.path.basename(p)}")
                # Per-file resume (CSL)
                if counters is not None:
                    for line in counters.summary_lines():
                        print("  ", line)
        status = 0
    except KeyboardInterrupt:
        print("Ejecución interrumpida por el usuario")
        status = 130
    except Exception as e:
        print(f"Error durante la carga: {e}")
        status = 1
    finally:
        try:
            # Final commit if pending work exists
            if last_commit > 0:
                cnxn.commit()
            cnxn.close()
        except Exception:
            pass
        # Resumen siempre al final
        print(f"Procesados: {processed} archivo(s)")
        end_dt = datetime.now()
        elapsed = time.perf_counter() - start_perf
        duration = timedelta(seconds=elapsed)
        print(f"Inicio: {start_dt:%Y-%m-%d %H:%M:%S}")
        print(f"Fin: {end_dt:%Y-%m-%d %H:%M:%S}")
        print(f"Duración: {duration}")
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
