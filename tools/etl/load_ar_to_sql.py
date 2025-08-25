#!/usr/bin/env python3
"""
Extract a limited set of AR_*.xml files and generate a T-SQL script to load
into the Dwh2 star schema (dimensions upsert + fact inserts).

This does not require a DB connection. It writes tools/sql/out/load_ar_YYYYMMDDHHMMSS.sql
that you can run in Azure SQL.

Usage:
  python tools/etl/load_ar_to_sql.py [--limit 50]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from glob import iglob
from typing import Dict, List, Optional, Tuple

from lxml import etree

from .exec_sql import run_sql_file

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
XML_ROOT = os.path.join(ROOT, "XMLS_COL")
OUT_DIR = os.path.join(ROOT, "tools", "sql", "out")
NS = {"u": "http://www.cargowise.com/Schemas/Universal/2011/11"}


def text(node: Optional[etree._Element]) -> str:
    return (node.text or "").strip() if node is not None else ""


def sql_quote(v: Optional[str]) -> str:
    s = "" if v is None else str(v)
    return s.replace("'", "''")


def parse_datekey(s: str) -> Optional[int]:
    s = (s or "").strip()
    if not s:
        return None
    # accept YYYY-MM-DD or YYYY-MM-DDTHH:MM...
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if not m:
        return None
    return int(m.group(1) + m.group(2) + m.group(3))


def find_files(limit: Optional[int]) -> List[str]:
    files = sorted(iglob(os.path.join(XML_ROOT, "**", "AR_*.xml"), recursive=True))
    return files[:limit] if limit else files


def extract_from_xml(path: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Return (dims, fact) dictionaries with natural keys and measures."""
    parser = etree.XMLParser(remove_blank_text=False, ns_clean=True)
    doc = etree.parse(path, parser)
    root = doc.getroot()

    # TransactionInfo/DataContext
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

    ent_id = text(dc.find("u:EnterpriseID", NS)) if dc is not None else ""
    srv_id = text(dc.find("u:ServerID", NS)) if dc is not None else ""
    provider = text(dc.find("u:DataProvider", NS)) if dc is not None else ""

    trigger_date = text(dc.find("u:TriggerDate", NS)) if dc is not None else ""
    trigger_datekey = parse_datekey(trigger_date)

    # Core AR fields
    # Many fields live under root alongside TransactionInfo; navigate directly
    number = text(root.find(".//u:Number", NS))
    ledger = text(root.find(".//u:Ledger", NS))

    local_currency = root.find(".//u:LocalCurrency", NS)
    lc_code = text(local_currency.find("u:Code", NS)) if local_currency is not None else ""
    lc_desc = text(local_currency.find("u:Description", NS)) if local_currency is not None else ""

    account_group = root.find(".//u:ARAccountGroup", NS)
    ag_code = text(account_group.find("u:Code", NS)) if account_group is not None else ""
    ag_desc = text(account_group.find("u:Description", NS)) if account_group is not None else ""

    # Dates
    transaction_date = text(root.find(".//u:TransactionDate", NS))
    post_date = text(root.find(".//u:PostDate", NS))
    due_date = text(root.find(".//u:DueDate", NS))
    tk = parse_datekey(transaction_date)
    pk = parse_datekey(post_date)
    dk = parse_datekey(due_date)

    # Amounts
    def dec(node_name: str) -> Optional[str]:
        val = text(root.find(f".//u:{node_name}", NS))
        return val if val != "" else None

    amt_local_ex_vat = dec("LocalExVATAmount")
    amt_local_vat = dec("LocalVATAmount")
    amt_local_tax = dec("LocalTaxTransactionsAmount")
    amt_local_total = dec("LocalTotal")

    # Flags
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

    dims = {
        "Country": (comp_country_code, comp_country_name),
        "Company": (company_code, company_name, comp_country_code),
        "Department": (dept_code, dept_name),
        "EventType": (et_code, et_desc),
        "ActionPurpose": (ap_code, ap_desc),
        "User": (usr_code, usr_name),
        "Enterprise": (ent_id,),
        "Server": (srv_id,),
        "DataProvider": (provider,),
        "Currency": (lc_code, lc_desc),
        "AccountGroup": (ag_code, ag_desc, "AR"),
    }

    fact = {
        "Number": number,
        "Ledger": ledger or None,
        "TransactionDateKey": tk,
        "PostDateKey": pk,
        "DueDateKey": dk,
        "TriggerDateKey": trigger_datekey,
        "LocalExVATAmount": amt_local_ex_vat,
        "LocalVATAmount": amt_local_vat,
        "LocalTaxTransactionsAmount": amt_local_tax,
        "LocalTotal": amt_local_total,
        "IsCancelled": is_cancelled,
        "IsCreatedByMatchingProcess": is_created_by_matching,
        "IsPrinted": is_printed,
        # Dimension natural keys for lookup
        "nk": {
            "CountryCode": comp_country_code,
            "CompanyCode": company_code,
            "DepartmentCode": dept_code,
            "EventTypeCode": et_code,
            "ActionPurposeCode": ap_code,
            "UserCode": usr_code,
            "EnterpriseId": ent_id,
            "ServerId": srv_id,
            "ProviderCode": provider,
            "LocalCurrencyCode": lc_code,
            "AccountGroupCode": ag_code,
        }
    }

    return dims, fact


def emit_merge(table: str, cols: List[str], rows: List[Tuple], key_cols: List[str]) -> List[str]:
    if not rows:
        return []
    lines: List[str] = []
    # build a VALUES list
    value_rows = []
    for r in rows:
        vals = []
        for v in r:
            if v is None:
                vals.append("NULL")
            else:
                s = str(v).replace("'", "''")
                vals.append(f"'{s}'")
        value_rows.append("(" + ",".join(vals) + ")")
    src = "VALUES\n    " + ",\n    ".join(value_rows)
    alias_cols = ", ".join([f"[{c}]" for c in cols])
    key_match = " AND ".join([f"T.[{c}] = S.[{c}]" for c in key_cols])
    set_updates = ", ".join([f"T.[{c}] = S.[{c}]" for c in cols if c not in key_cols])
    insert_cols = ", ".join([f"[{c}]" for c in cols])
    insert_src = ", ".join([f"S.[{c}]" for c in cols])
    lines.append(f";WITH S([{alias_cols}]) AS ({src})")
    lines.append(f"MERGE {table} AS T")
    lines.append(f"USING S ON {key_match}")
    if set_updates:
        lines.append(f"WHEN MATCHED THEN UPDATE SET {set_updates}, T.[UpdatedAt] = SYSUTCDATETIME()")
    lines.append(f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_src})")
    lines.append(";")
    return lines


def generate_sql(dims_pool: Dict[str, set], facts: List[Dict]) -> str:
    lines: List[str] = []
    ap = lines.append
    ap("BEGIN TRAN;")

    # Emit dimension MERGEs (deduped)
    # Country
    countries = sorted(dims_pool["Country"])  # tuples (Code, Name)
    ap("-- Upsert Countries")
    ap("\n".join(emit_merge("Dwh2.DimCountry", ["Code", "Name"], countries, ["Code"])) or "-- none")

    # Company (Code, Name, CountryCode) with CountryKey resolved post-merge
    companies = sorted(dims_pool["Company"])  # (Code, Name, CountryCode)
    ap("-- Upsert Companies and set CountryKey from Country.Code")
    if companies:
        comp_rows = []
        for (code, name, ccode) in companies:
            code_s = (code or "").replace("'", "''")
            name_s = (name or "").replace("'", "''")
            ccode_s = (ccode or "").replace("'", "''")
            comp_rows.append(f"('{code_s}','{name_s}','{ccode_s}')")
        ap("DECLARE @CompanySource TABLE ([Code] NVARCHAR(50), [Name] NVARCHAR(200), [CountryCode] NVARCHAR(10));")
        ap("INSERT INTO @CompanySource ([Code],[Name],[CountryCode]) VALUES\n    " + ",\n    ".join(comp_rows) + ";")
        ap("MERGE Dwh2.DimCompany AS T\n"
           "USING @CompanySource AS S ON T.[Code] = S.[Code]\n"
           "WHEN MATCHED THEN UPDATE SET T.[Name] = S.[Name], T.[UpdatedAt] = SYSUTCDATETIME()\n"
           "WHEN NOT MATCHED THEN INSERT ([Code],[Name]) VALUES (S.[Code],S.[Name]);")
        ap("UPDATE c SET c.CountryKey = co.CountryKey, c.UpdatedAt = SYSUTCDATETIME()\n"
           "FROM Dwh2.DimCompany c\n"
           "JOIN @CompanySource s ON s.[Code] = c.[Code]\n"
           "JOIN Dwh2.DimCountry co ON co.[Code] = s.[CountryCode]\n"
           "WHERE c.CountryKey IS NULL;")
    else:
        ap("-- none")

    # Department
    departments = sorted(dims_pool["Department"])  # (Code, Name)
    ap("-- Upsert Departments")
    ap("\n".join(emit_merge("Dwh2.DimDepartment", ["Code", "Name"], departments, ["Code"])) or "-- none")

    # EventType
    event_types = sorted(dims_pool["EventType"])  # (Code, Description)
    ap("-- Upsert EventTypes")
    ap("\n".join(emit_merge("Dwh2.DimEventType", ["Code", "Description"], event_types, ["Code"])) or "-- none")

    # ActionPurpose
    action_purposes = sorted(dims_pool["ActionPurpose"])  # (Code, Description)
    ap("-- Upsert ActionPurposes")
    ap("\n".join(emit_merge("Dwh2.DimActionPurpose", ["Code", "Description"], action_purposes, ["Code"])) or "-- none")

    # User
    users = sorted(dims_pool["User"])  # (Code, Name)
    ap("-- Upsert Users")
    ap("\n".join(emit_merge("Dwh2.DimUser", ["Code", "Name"], users, ["Code"])) or "-- none")

    # Enterprise
    enterprises = sorted(dims_pool["Enterprise"])  # (EnterpriseId,)
    ap("-- Upsert Enterprises")
    ap("\n".join(emit_merge("Dwh2.DimEnterprise", ["EnterpriseId"], enterprises, ["EnterpriseId"])) or "-- none")

    # Server
    servers = sorted(dims_pool["Server"])  # (ServerId,)
    ap("-- Upsert Servers")
    ap("\n".join(emit_merge("Dwh2.DimServer", ["ServerId"], servers, ["ServerId"])) or "-- none")

    # DataProvider
    providers = sorted(dims_pool["DataProvider"])  # (ProviderCode,)
    ap("-- Upsert DataProviders")
    ap("\n".join(emit_merge("Dwh2.DimDataProvider", ["ProviderCode"], providers, ["ProviderCode"])) or "-- none")

    # Currency
    currencies = sorted(dims_pool["Currency"])  # (Code, Description)
    ap("-- Upsert Currencies")
    ap("\n".join(emit_merge("Dwh2.DimCurrency", ["Code", "Description"], currencies, ["Code"])) or "-- none")

    # AccountGroup
    account_groups = sorted(dims_pool["AccountGroup"])  # (Code, Description, Type)
    ap("-- Upsert AccountGroups")
    ap("\n".join(emit_merge("Dwh2.DimAccountGroup", ["Code", "Description", "Type"], account_groups, ["Code"])) or "-- none")

    # (Company.CountryKey already updated above using CompanySource)

    # Fact inserts
    ap("-- Insert Facts")
    for f in facts:
        nk = f["nk"]
        cols = [
            "Number", "Ledger",
            "TransactionDateKey", "PostDateKey", "DueDateKey", "TriggerDateKey",
            "LocalExVATAmount", "LocalVATAmount", "LocalTaxTransactionsAmount", "LocalTotal",
            "IsCancelled", "IsCreatedByMatchingProcess", "IsPrinted",
            "CompanyKey", "DepartmentKey", "EventTypeKey", "ActionPurposeKey", "UserKey",
            "EnterpriseKey", "ServerKey", "DataProviderKey", "LocalCurrencyKey", "AccountGroupKey",
        ]
        vals: List[str] = []
        def q(v):
            if v is None:
                return "NULL"
            if isinstance(v, (int, float)):
                return str(v)
            s = str(v).replace("'", "''")
            return f"'{s}'"

        vals.append(q(f.get("Number")))
        vals.append(q(f.get("Ledger")))
        for k in ("TransactionDateKey", "PostDateKey", "DueDateKey", "TriggerDateKey"):
            vals.append(q(f.get(k)))
        for k in ("LocalExVATAmount", "LocalVATAmount", "LocalTaxTransactionsAmount", "LocalTotal"):
            vals.append(q(f.get(k)))
        for k in ("IsCancelled", "IsCreatedByMatchingProcess", "IsPrinted"):
            vals.append(q(f.get(k)))
        # Dimension subqueries by natural key (pre-escaped)
        company_code_sql = sql_quote(nk.get('CompanyCode'))
        dept_code_sql = sql_quote(nk.get('DepartmentCode'))
        event_code_sql = sql_quote(nk.get('EventTypeCode'))
        ap_code_sql = sql_quote(nk.get('ActionPurposeCode'))
        user_code_sql = sql_quote(nk.get('UserCode'))
        enterprise_id_sql = sql_quote(nk.get('EnterpriseId'))
        server_id_sql = sql_quote(nk.get('ServerId'))
        provider_code_sql = sql_quote(nk.get('ProviderCode'))
        currency_code_sql = sql_quote(nk.get('LocalCurrencyCode'))
        ag_code_sql = sql_quote(nk.get('AccountGroupCode'))

        vals.append(f"(SELECT CompanyKey FROM Dwh2.DimCompany WHERE Code = '{company_code_sql}')")
        vals.append(f"(SELECT DepartmentKey FROM Dwh2.DimDepartment WHERE Code = '{dept_code_sql}')")
        vals.append(f"(SELECT EventTypeKey FROM Dwh2.DimEventType WHERE Code = '{event_code_sql}')")
        vals.append(f"(SELECT ActionPurposeKey FROM Dwh2.DimActionPurpose WHERE Code = '{ap_code_sql}')")
        vals.append(f"(SELECT UserKey FROM Dwh2.DimUser WHERE Code = '{user_code_sql}')")
        vals.append(f"(SELECT EnterpriseKey FROM Dwh2.DimEnterprise WHERE EnterpriseId = '{enterprise_id_sql}')")
        vals.append(f"(SELECT ServerKey FROM Dwh2.DimServer WHERE ServerId = '{server_id_sql}')")
        vals.append(f"(SELECT DataProviderKey FROM Dwh2.DimDataProvider WHERE ProviderCode = '{provider_code_sql}')")
        vals.append(f"(SELECT CurrencyKey FROM Dwh2.DimCurrency WHERE Code = '{currency_code_sql}')")
        vals.append(f"(SELECT AccountGroupKey FROM Dwh2.DimAccountGroup WHERE Code = '{ag_code_sql}')")

        ap("INSERT INTO Dwh2.FactAccountsReceivableTransaction (" + ", ".join(cols) + ")")
        ap("VALUES (" + ", ".join(vals) + ");")

    ap("COMMIT;")
    return "\n".join(lines)


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50, help="Limit number of AR XMLs to process")
    parser.add_argument("--execute", action="store_true", help="Execute the generated SQL against Azure SQL")
    args = parser.parse_args(argv[1:])

    files = find_files(args.limit)
    if not files:
        print("No AR_*.xml files found.")
        return 1

    dims_pool: Dict[str, set] = defaultdict(set)
    facts: List[Dict] = []
    for p in files:
        dims, fact = extract_from_xml(p)
        # Collect dims
        for k, v in dims.items():
            dims_pool[k].add(tuple(v))
        facts.append(fact)

    os.makedirs(OUT_DIR, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    out_path = os.path.join(OUT_DIR, f"load_ar_{ts}.sql")
    sql = generate_sql(dims_pool, facts)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(sql)
    print(f"Wrote: {out_path}")
    if args.execute:
        print("Executing SQL against Azure SQL...")
        run_sql_file(out_path)
        print("Execution complete.")
    else:
        print("Review the script and execute it in Azure SQL. Ensure DimDate is populated for referenced DateKeys.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
