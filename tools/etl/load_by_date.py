#!/usr/bin/env python3
"""
ETL por fecha: recorre XMLS_COL/YYYYMMDD, procesa AR_*.xml y CSL*.xml, y hace upsert directo en Azure SQL.

Uso:
    python -m tools.etl.load_by_date --date 20250707 [--only AR|CSL] [--limit N]

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
from typing import Dict, Optional, Tuple

import pyodbc
from lxml import etree

from .config import build_connection_string, Settings

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
XML_ROOT = Settings.xml_root or os.path.join(ROOT, "XMLS_COL")
NS = {"u": "http://www.cargowise.com/Schemas/Universal/2011/11"}
# Quiet mode suppresses expected fallback logs
QUIET = False
# Commit batching: commit every N files (default 1 = current behavior)
COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "5"))
class UpsertError(Exception):
    pass

# Per-run caches to reduce DB round-trips for dimensions
# Keyed by (fully-qualified table name like [Dwh2].[DimCountry], code)
_DIM_KEY_CACHE: Dict[Tuple[str, str], int] = {}
_DIM_ATTR_CACHE: Dict[Tuple[str, str], Tuple[Optional[str], Tuple[Tuple[str, object], ...]]] = {}


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
    return pyodbc.connect(conn_str)


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
    }
    return dims, fact


def upsert_ar(cur: pyodbc.Cursor, dims: Dict, fact: Dict) -> None:
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
    else:
        col_names = ["Number"] + [c for c, _ in cols]
        placeholders = ",".join(["?"] * len(col_names))
        params = [number] + [v for _, v in cols]
        cur.execute("INSERT INTO Dwh2.FactAccountsReceivableTransaction ([" + "],[".join(col_names) + "]) VALUES (" + placeholders + ")", *params)
        cur.execute("SELECT FactAccountsReceivableTransactionKey FROM Dwh2.FactAccountsReceivableTransaction WHERE [Number] = ?", number)
        r2 = cur.fetchone()
        fact_key = int(r2[0]) if r2 else None

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

    # Message numbers
    if fact_key and fact.get("MessageNumbers"):
        for mtype, mval in fact.get("MessageNumbers"):
            try:
                cur.execute(
                    "IF NOT EXISTS (SELECT 1 FROM Dwh2.FactARMessageNumber WHERE FactAccountsReceivableTransactionKey=? AND [Type]=? AND [Value]=?) "
                    "INSERT INTO Dwh2.FactARMessageNumber (FactAccountsReceivableTransactionKey, [Type], [Value]) VALUES (?,?,?);",
                    fact_key, mtype, mval, fact_key, mtype, mval
                )
            except Exception:
                pass

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
    }
    return dims, fact


def upsert_csl(cur: pyodbc.Cursor, dims: Dict, fact: Dict) -> None:
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
        else:
            # Insert new
            col_names = list(cols.keys())
            placeholders = ",".join(["?"] * len(col_names))
            cur.execute("INSERT INTO Dwh2.FactShipment ([" + "],[".join(col_names) + "]) VALUES (" + placeholders + ")", *list(cols.values()))
            cur.execute("SELECT TOP 1 FactShipmentKey FROM Dwh2.FactShipment WHERE ShipmentJobKey IS NOT NULL AND ShipmentJobKey = ? ORDER BY FactShipmentKey DESC", shipment_job_key)
            r = cur.fetchone()
            fact_ship_key = int(r[0]) if r else None
    else:
        # No business key => always insert
        col_names = list(cols.keys())
        placeholders = ",".join(["?"] * len(col_names))
        cur.execute("INSERT INTO Dwh2.FactShipment ([" + "],[".join(col_names) + "]) VALUES (" + placeholders + ")", *list(cols.values()))
        cur.execute("SELECT TOP 1 FactShipmentKey FROM Dwh2.FactShipment ORDER BY FactShipmentKey DESC")
        r = cur.fetchone()
        fact_ship_key = int(r[0]) if r else None

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
                        "IF NOT EXISTS (SELECT 1 FROM Dwh2.BridgeFactShipmentOrganization WHERE FactShipmentKey=? AND OrganizationKey=? AND AddressType=?) "
                        "INSERT INTO Dwh2.BridgeFactShipmentOrganization (FactShipmentKey, OrganizationKey, AddressType) VALUES (?,?,?);",
                        fact_ship_key, org_key, org.get("AddressType") or "", fact_ship_key, org_key, org.get("AddressType") or ""
                    )
                except Exception:
                    pass


def main(argv):
    start_dt = datetime.now()
    start_perf = time.perf_counter()
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="Fecha en formato YYYYMMDD (carpeta bajo XMLS_COL)")
    ap.add_argument("--only", choices=["AR", "CSL"], help="Procesar solo AR o CSL")
    ap.add_argument("--limit", type=int, help="Máximo de archivos a procesar")
    ap.add_argument("--quiet", action="store_true", help="Suprime logs de fallback esperados para inserts de dimensiones")
    args = ap.parse_args(argv[1:])

    global QUIET
    QUIET = bool(args.quiet)

    date_folder = os.path.join(XML_ROOT, args.date)
    if not os.path.isdir(date_folder):
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

    cnxn = connect()
    processed = 0
    last_commit = 0
    try:
        cur = cnxn.cursor()
        # AR
        for p in ar_files:
            dims, fact = parse_ar(p)
            upsert_ar(cur, dims, fact)
            last_commit += 1
            if last_commit >= max(1, COMMIT_EVERY):
                cnxn.commit()
                last_commit = 0
            processed += 1
            print(f"AR OK: {os.path.basename(p)}")
        # CSL
        for p in csl_files:
            dims, fact = parse_csl(p)
            upsert_csl(cur, dims, fact)
            last_commit += 1
            if last_commit >= max(1, COMMIT_EVERY):
                cnxn.commit()
                last_commit = 0
            processed += 1
            print(f"CSL OK: {os.path.basename(p)}")
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
