# from __future__ import annotations
# import argparse
# import json
# import logging
# import os
# import sys
# from datetime import datetime
# from pathlib import Path
# from typing import Optional
# import pandas as pd
# from dotenv import load_dotenv
# from sqlalchemy import text

# logging.basicConfig(
#     format="%(asctime)s  %(levelname)-8s  %(message)s",
#     datefmt="%H:%M:%S",
#     level=logging.INFO,
# )
# log = logging.getLogger("dq_accuracy")

# VALID_LE_BOOKS: frozenset = frozenset()  # populated at runtime via fetch_valid_le_books(engine)

# from dq_rules import (  # noqa: E402
#     VALID_ACCOUNT_STATUS, VALID_PERFORMANCE_CLASS, VALID_GENDER,
#     VALID_ACCOUNT_TYPE, CORPORATE_LEGAL_STATUS, ALL_CORPORATE_LEGAL_STATUS,
#     PENSION_ACCOUNT_TYPES, INSURANCE_ACCOUNT_TYPES, INSURANCE_STATUS_VALID,
#     VALID_CORP_VISION_SBU, INSURANCE_LE_BOOKS_GI, INSURANCE_LE_BOOKS_LI,
#     VALID_GI_DEAL_SUB_TYPES, VALID_LI_DEAL_SUB_TYPES, VALID_INS_CONTRACT_STATUS,
#     VALID_LOAN_DEAL_SUB_TYPES, VALID_DEP_DEAL_SUB_TYPES,
#     DORMANCY_DAYS, REGISTRATION_NUMBER_CODE,
#     #ACC_RULE_META as RULE_META,
#     #ACCURACY_COLUMNS,
#     #ACC_TABLE_RULES as TABLE_RULES,
# )

# #TARGET_TABLES = list(ACCURACY_COLUMNS.keys())


# from db_utils import CATEGORY_TYPES, build_connection_string, get_engine, get_valid_le_books  # noqa: F401


# def fetch_table(engine, table_name: str, columns: list[str],
#                 db_schema: str, limit: int,
#                 valid_le_books: frozenset = frozenset()) -> pd.DataFrame:
#     # introspect schema to drop any requested columns that don't exist, then fetch
#     from sqlalchemy import inspect as sa_inspect

#     try:
#         inspector = sa_inspect(engine)
#         db_cols: set[str] = set()
#         for schema in (db_schema, None):  # try specified schema then public fallback
#             try:
#                 cols = inspector.get_columns(table_name, schema=schema)
#                 if cols:
#                     db_cols = {c["name"].lower() for c in cols}
#                     break
#             except Exception:
#                 continue
#         columns = [c for c in columns if c in db_cols]  # restrict to columns that actually exist
#     except Exception as exc:
#         log.warning("Cannot introspect '%s': %s", table_name, exc)

#     if not columns:
#         log.warning("  No accuracy columns found in DB for '%s'", table_name)
#         return pd.DataFrame()

#     quoted = ", ".join(f'"{c}"' for c in columns)

#     where = ""
#     if valid_le_books and "le_book" in columns:
#         codes = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
#         where = f' WHERE "le_book" IN ({codes})'

#     if limit > 0:
#         sql    = text(f'SELECT {quoted} FROM "{db_schema}"."{table_name}"{where} LIMIT :lim')
#         params = {"lim": limit}
#     else:
#         sql    = text(f'SELECT {quoted} FROM "{db_schema}"."{table_name}"{where}')
#         params = {}
#     try:
#         with engine.connect() as conn:
#             df = pd.read_sql(sql, conn, params=params)
#         df.columns = [c.lower() for c in df.columns]
#         log.info("  '%s'  %d rows × %d cols fetched", table_name, len(df), len(df.columns))
#         return df
#     except Exception as exc:
#         log.error("  Query failed for '%s': %s", table_name, exc)
#         return pd.DataFrame()


# def _pct(valid: int, total: int) -> float:  # safe percentage: returns 100.0 when total is 0
#     return round(valid / total * 100, 2) if total else 100.0


# def _single_col(df: pd.DataFrame, col: str,
#                 valid_set: frozenset, normalise=None) -> Optional[tuple[int, int, int]]:
#     # drop nulls (completeness concern), optionally normalise values, then check membership
#     if col not in df.columns:
#         return None
#     series = df[col].dropna()
#     if series.empty:
#         return None
#     if normalise:
#         series = series.map(normalise)
#     valid_mask = series.isin(valid_set)
#     total   = len(series)
#     valid   = int(valid_mask.sum())
#     return valid, total - valid, total


# def run_rule(rule_id: str, df: pd.DataFrame) -> Optional[tuple[int, int, int]]:
#     # dispatch to per-rule validation logic; returns (valid, invalid, total) or None if not applicable
#     if df.empty:
#         return None

#     if rule_id == "ACC-001":  # le_book must be in BNR institution code set
#         return _single_col(df, "le_book", VALID_LE_BOOKS,
#                            normalise=lambda x: str(x).strip())

#     if rule_id == "ACC-002":  # account_status must be a valid numeric code; coerce to int first
#         if "account_status" not in df.columns:
#             return None
#         series = df["account_status"].dropna()
#         if series.empty:
#             return None
#         try:
#             series = series.astype(int)
#         except (ValueError, TypeError):
#             pass
#         valid_mask = series.isin(VALID_ACCOUNT_STATUS)
#         total = len(series)
#         valid = int(valid_mask.sum())
#         return valid, total - valid, total

#     if rule_id == "ACC-003":  # performance_class must match BNR loan classification codes
#         return _single_col(df, "performance_class", VALID_PERFORMANCE_CLASS,
#                            normalise=lambda x: str(x).strip().upper())

#     if rule_id == "ACC-004":  # customer_gender must be M, F, or C
#         return _single_col(df, "customer_gender", VALID_GENDER,
#                            normalise=lambda x: str(x).strip().upper())

#     if rule_id == "ACC-005":  # account_type must be a valid BNR product code
#         return _single_col(df, "account_type", VALID_ACCOUNT_TYPE,
#                            normalise=lambda x: str(x).strip().upper())

#     if rule_id == "ACC-010":  # corporate legal_status requires gender == C
#         needed = ["customer_gender", "legal_status"]
#         if not all(c in df.columns for c in needed):
#             return None
#         sub = df[needed].dropna()
#         if sub.empty:
#             return None
#         try:
#             ls = sub["legal_status"].astype(int)
#         except (ValueError, TypeError):
#             ls = sub["legal_status"]
#         is_corporate = ls.isin(CORPORATE_LEGAL_STATUS)
#         gender_is_c  = sub["customer_gender"].astype(str).str.strip().str.upper() == "C"
#         invalid_mask = is_corporate & ~gender_is_c  # corporate with non-C gender = invalid
#         total   = len(sub)
#         invalid = int(invalid_mask.sum())
#         return total - invalid, invalid, total

#     if rule_id == "ACC-011":  # pension account types must not appear in the RETL segment
#         needed = ["account_type", "vision_sbu"]
#         if not all(c in df.columns for c in needed):
#             return None
#         sub = df[needed].dropna()
#         if sub.empty:
#             return None
#         is_pension = sub["account_type"].astype(str).str.strip().str.upper().isin(PENSION_ACCOUNT_TYPES)
#         is_retl    = sub["vision_sbu"].astype(str).str.strip().str.upper() == "RETL"
#         invalid_mask = is_pension & is_retl  # pension product in retail segment = invalid
#         total   = len(sub)
#         invalid = int(invalid_mask.sum())
#         return total - invalid, invalid, total

#     if rule_id == "ACC-012":  # corporate customers (gender == C) must have marital_status == NA
#         needed = ["marital_status", "customer_gender"]
#         if not all(c in df.columns for c in needed):
#             return None
#         sub = df[needed].dropna()
#         if sub.empty:
#             return None
#         is_corporate  = sub["customer_gender"].astype(str).str.strip().str.upper() == "C"
#         marital_is_na = sub["marital_status"].astype(str).str.strip().str.upper() == "NA"
#         invalid_mask  = is_corporate & ~marital_is_na  # corporate with non-NA marital = invalid
#         total   = len(sub)
#         invalid = int(invalid_mask.sum())
#         return total - invalid, invalid, total

#     if rule_id == "ACC-013":  # le_book must be exactly 3 numeric characters (zero-padded)
#         if "le_book" not in df.columns:
#             return None
#         series = df["le_book"].dropna()
#         if series.empty:
#             return None
#         s = series.astype(str).str.strip()
#         valid_mask = (s.str.len() == 3) & s.str.match(r"^\d{3}$")
#         total = len(series)
#         valid = int(valid_mask.sum())
#         return valid, total - valid, total

#     # ── ACC-014: all accounts must be active (account_status == 0) ────────────
#     if rule_id == "ACC-014":
#         if "account_status" not in df.columns:
#             return None
#         s = pd.to_numeric(df["account_status"], errors="coerce").dropna()
#         if s.empty:
#             return None
#         valid = int((s == 0).sum())
#         return valid, len(s) - valid, len(s)

#     # ── ACC-015: performance_class only on LAA account type ───────────────────
#     if rule_id == "ACC-015":
#         needed = ["account_type", "performance_class"]
#         if not all(c in df.columns for c in needed):
#             return None
#         sub = df[needed].dropna(subset=["performance_class"])
#         if sub.empty:
#             return None
#         at = sub["account_type"].astype(str).str.strip().str.upper()
#         valid_mask = at == "LAA"
#         total = len(sub)
#         valid = int(valid_mask.sum())
#         return valid, total - valid, total

#     # ── ACC-016: written-off loan accounts must have Closed status (2) ────────
#     if rule_id == "ACC-016":
#         needed = ["account_type", "performance_class", "account_status"]
#         if not all(c in df.columns for c in needed):
#             return None
#         at  = df["account_type"].astype(str).str.strip().str.upper()
#         pc  = df["performance_class"].astype(str).str.strip().str.upper()
#         ast = pd.to_numeric(df["account_status"], errors="coerce")
#         wo_loans = (at == "LAA") & (pc == "WO")
#         sub = df[wo_loans].copy()
#         if sub.empty:
#             return None
#         ast_sub = pd.to_numeric(sub["account_status"], errors="coerce").dropna()
#         if ast_sub.empty:
#             return None
#         valid = int((ast_sub == 2).sum())
#         return valid, len(ast_sub) - valid, len(ast_sub)

#     # ── ACC-017: vision_sbu must not be NOT_APPLICABLE or OTHERS ─────────────
#     if rule_id == "ACC-017":
#         if "vision_sbu" not in df.columns:
#             return None
#         s = df["vision_sbu"].dropna().astype(str).str.strip().str.upper()
#         if s.empty:
#             return None
#         invalid_mask = s.isin({"NOT APPLICABLE", "OTHERS", "NOT_APPLICABLE", "OTHER"})
#         total = len(s)
#         valid = int((~invalid_mask).sum())
#         return valid, total - valid, total

#     # ── ACC-018: active accounts with no transaction in DORMANCY_DAYS+ days ──
#     if rule_id == "ACC-018":
#         needed = ["account_status", "last_tran_date"]
#         if not all(c in df.columns for c in needed):
#             return None
#         ast = pd.to_numeric(df["account_status"], errors="coerce")
#         ldt = pd.to_datetime(df["last_tran_date"], errors="coerce", utc=False)
#         if getattr(ldt.dt, "tz", None) is not None:
#             ldt = ldt.dt.tz_localize(None)
#         active = ast == 0
#         sub = df[active & ldt.notna()].copy()
#         if sub.empty:
#             return None
#         days_since = (pd.Timestamp.today().normalize() -
#                       ldt[active & ldt.notna()]).dt.days
#         valid = int((days_since < DORMANCY_DAYS).sum())
#         total = len(days_since)
#         return valid, total - valid, total

#     # ── ACC-019: active insurance accounts must be IP or OAB ─────────────────
#     if rule_id == "ACC-019":
#         needed = ["account_status", "account_type"]
#         if not all(c in df.columns for c in needed):
#             return None
#         ast = pd.to_numeric(df["account_status"], errors="coerce")
#         at  = df["account_type"].astype(str).str.strip().str.upper()
#         ins_active = (ast == 0) & at.isin({"IP", "OAB", "CAA", "SBA", "TDA",
#                                             "SED", "LAA", "TRUSTAC",
#                                             "MPSDC", "MPSDB", "VCOPSDC",
#                                             "VCOPSDB", "VPPSDC", "VPPSDB"})
#         # Scope: rows where account_type is an insurance type and status is active
#         ins_active = (ast == 0) & df["account_type"].notna()
#         ins_types_present = at.isin(INSURANCE_ACCOUNT_TYPES)
#         sub_active = df[ast == 0].copy()
#         if sub_active.empty:
#             return None
#         at_sub = sub_active["account_type"].astype(str).str.strip().str.upper()
#         # Only check rows that currently have any VALID_ACCOUNT_TYPE but scope to
#         # known insurance LE_BOOKs
#         lb = pd.to_numeric(sub_active.get("le_book", pd.Series(dtype="object")),
#                            errors="coerce")
#         is_ins_lb = lb.isin(INSURANCE_LE_BOOKS_GI | INSURANCE_LE_BOOKS_LI)
#         ins_sub = sub_active[is_ins_lb]
#         if ins_sub.empty:
#             return None
#         at_ins = ins_sub["account_type"].astype(str).str.strip().str.upper()
#         valid = int(at_ins.isin(INSURANCE_ACCOUNT_TYPES).sum())
#         return valid, len(at_ins) - valid, len(at_ins)

#     # ── ACC-020: insurance account status must be in {0,1,2,4} ───────────────
#     if rule_id == "ACC-020":
#         if "account_status" not in df.columns:
#             return None
#         lb = pd.to_numeric(df.get("le_book", pd.Series(dtype="object")),
#                            errors="coerce")
#         is_ins = lb.isin(INSURANCE_LE_BOOKS_GI | INSURANCE_LE_BOOKS_LI)
#         sub = df[is_ins].copy()
#         if sub.empty:
#             return None
#         s = pd.to_numeric(sub["account_status"], errors="coerce").dropna()
#         if s.empty:
#             return None
#         valid = int(s.isin(INSURANCE_STATUS_VALID).sum())
#         return valid, len(s) - valid, len(s)

#     # ── ACC-021: all customers must have active status (customer_status == 0) ─
#     if rule_id == "ACC-021":
#         if "customer_status" not in df.columns:
#             return None
#         s = pd.to_numeric(df["customer_status"], errors="coerce").dropna()
#         if s.empty:
#             return None
#         valid = int((s == 0).sum())
#         return valid, len(s) - valid, len(s)

#     # ── ACC-022: individual customers must have non-corporate legal status ─────
#     if rule_id == "ACC-022":
#         needed = ["customer_gender", "legal_status"]
#         if not all(c in df.columns for c in needed):
#             return None
#         sub = df[needed].dropna()
#         if sub.empty:
#             return None
#         is_indiv = sub["customer_gender"].astype(str).str.strip().str.upper() != "C"
#         indiv = sub[is_indiv]
#         if indiv.empty:
#             return None
#         try:
#             ls = indiv["legal_status"].astype(int)
#         except (ValueError, TypeError):
#             ls = pd.to_numeric(indiv["legal_status"], errors="coerce")
#         valid_mask = ~ls.isin(ALL_CORPORATE_LEGAL_STATUS)
#         total = len(indiv)
#         valid = int(valid_mask.sum())
#         return valid, total - valid, total

#     # ── ACC-023: individual with Registration Number ID must not have TIN ─────
#     if rule_id == "ACC-023":
#         needed = ["national_id_type", "customer_tin", "customer_gender"]
#         if not all(c in df.columns for c in needed):
#             return None
#         sub = df[needed].dropna(subset=["national_id_type"])
#         if sub.empty:
#             return None
#         is_indiv = sub["customer_gender"].astype(str).str.strip().str.upper() != "C"
#         id_type  = pd.to_numeric(sub["national_id_type"], errors="coerce")
#         is_reg   = id_type.isin(REGISTRATION_NUMBER_CODE)
#         scope    = sub[is_indiv & is_reg]
#         if scope.empty:
#             return None
#         has_tin = scope["customer_tin"].notna() & \
#                   (scope["customer_tin"].astype(str).str.strip() != "")
#         total = len(scope)
#         valid = int((~has_tin).sum())
#         return valid, total - valid, total

#     # ── ACC-024: individual customers must have RETL vision_sbu ──────────────
#     if rule_id == "ACC-024":
#         needed = ["customer_gender", "vision_sbu"]
#         if not all(c in df.columns for c in needed):
#             return None
#         sub = df[needed].dropna()
#         if sub.empty:
#             return None
#         is_indiv = sub["customer_gender"].astype(str).str.strip().str.upper() != "C"
#         indiv    = sub[is_indiv]
#         if indiv.empty:
#             return None
#         s = indiv["vision_sbu"].astype(str).str.strip().str.upper()
#         valid = int((s == "RETL").sum())
#         return valid, len(s) - valid, len(s)

#     # ── ACC-025: corporate customers must have a valid business vision_sbu ────
#     if rule_id == "ACC-025":
#         needed = ["customer_gender", "vision_sbu"]
#         if not all(c in df.columns for c in needed):
#             return None
#         sub = df[needed].dropna()
#         if sub.empty:
#             return None
#         is_corp = sub["customer_gender"].astype(str).str.strip().str.upper() == "C"
#         corp    = sub[is_corp]
#         if corp.empty:
#             return None
#         s = corp["vision_sbu"].astype(str).str.strip().str.upper()
#         valid = int(s.isin(VALID_CORP_VISION_SBU).sum())
#         return valid, len(s) - valid, len(s)

#     # ── ACC-026: individual customers must have a real marital status (not NA) ─
#     if rule_id == "ACC-026":
#         needed = ["customer_gender", "marital_status"]
#         if not all(c in df.columns for c in needed):
#             return None
#         sub = df[needed].dropna()
#         if sub.empty:
#             return None
#         is_indiv = sub["customer_gender"].astype(str).str.strip().str.upper() != "C"
#         indiv    = sub[is_indiv]
#         if indiv.empty:
#             return None
#         ms = indiv["marital_status"].astype(str).str.strip().str.upper()
#         valid = int((ms != "NA").sum())
#         return valid, len(ms) - valid, len(ms)

#     # ── ACC-027: all contracts must have Active status (contract_status == 0) ─
#     if rule_id == "ACC-027":
#         if "contract_status" not in df.columns:
#             return None
#         s = pd.to_numeric(df["contract_status"], errors="coerce").dropna()
#         if s.empty:
#             return None
#         valid = int((s == 0).sum())
#         return valid, len(s) - valid, len(s)

#     # ── ACC-028: loan deal sub-type must match duration bracket ───────────────
#     if rule_id == "ACC-028":
#         needed = ["deal_sub_type", "start_date", "maturity_date"]
#         if not all(c in df.columns for c in needed):
#             return None
#         dst  = df["deal_sub_type"].astype(str).str.strip().str.upper()
#         scope = df[dst.isin(VALID_LOAN_DEAL_SUB_TYPES)].copy()
#         if scope.empty:
#             return None
#         sd = pd.to_datetime(scope["start_date"],    errors="coerce", utc=False)
#         md = pd.to_datetime(scope["maturity_date"], errors="coerce", utc=False)
#         if getattr(sd.dt, "tz", None): sd = sd.dt.tz_localize(None)
#         if getattr(md.dt, "tz", None): md = md.dt.tz_localize(None)
#         pair = pd.DataFrame({"dst": scope["deal_sub_type"].astype(str).str.strip().str.upper(),
#                              "sd": sd, "md": md}).dropna()
#         if pair.empty:
#             return None
#         years = (pair["md"] - pair["sd"]).dt.days / 365.25
#         expected = pd.Series("", index=pair.index)
#         expected[years < 1]                     = "SHTLON"
#         expected[(years >= 1) & (years <= 3)]   = "MEDLON"
#         expected[years > 3]                     = "LNGLON"
#         valid = int((pair["dst"] == expected).sum())
#         return valid, len(pair) - valid, len(pair)

#     # ── ACC-029: deposit deal sub-type must match duration bracket ────────────
#     if rule_id == "ACC-029":
#         needed = ["deal_sub_type", "start_date", "maturity_date"]
#         if not all(c in df.columns for c in needed):
#             return None
#         dst  = df["deal_sub_type"].astype(str).str.strip().str.upper()
#         scope = df[dst.isin(VALID_DEP_DEAL_SUB_TYPES)].copy()
#         if scope.empty:
#             return None
#         sd = pd.to_datetime(scope["start_date"],    errors="coerce", utc=False)
#         md = pd.to_datetime(scope["maturity_date"], errors="coerce", utc=False)
#         if getattr(sd.dt, "tz", None): sd = sd.dt.tz_localize(None)
#         if getattr(md.dt, "tz", None): md = md.dt.tz_localize(None)
#         pair = pd.DataFrame({"dst": scope["deal_sub_type"].astype(str).str.strip().str.upper(),
#                              "sd": sd, "md": md}).dropna()
#         if pair.empty:
#             return None
#         years = (pair["md"] - pair["sd"]).dt.days / 365.25
#         expected = pd.Series("", index=pair.index)
#         expected[years < 1]                     = "SHTDEP"
#         expected[(years >= 1) & (years <= 3)]   = "MEDDEP"
#         expected[years > 3]                     = "LNGDEP"
#         valid = int((pair["dst"] == expected).sum())
#         return valid, len(pair) - valid, len(pair)

#     # ── ACC-030: treasury loans must not be RETL ──────────────────────────────
#     if rule_id == "ACC-030":
#         needed = ["deal_type", "vision_sbu"]
#         if not all(c in df.columns for c in needed):
#             return None
#         dt  = df["deal_type"].astype(str).str.strip().str.upper()
#         sbu = df["vision_sbu"].astype(str).str.strip().str.upper()
#         scope = df[dt == "TRSLON"].copy()
#         if scope.empty:
#             return None
#         sbu_scope = scope["vision_sbu"].astype(str).str.strip().str.upper()
#         valid = int((sbu_scope != "RETL").sum())
#         return valid, len(sbu_scope) - valid, len(sbu_scope)

#     # ── ACC-031: GI LE_BOOKs must have GENINS deal_type ──────────────────────
#     if rule_id == "ACC-031":
#         needed = ["le_book", "deal_type"]
#         if not all(c in df.columns for c in needed):
#             return None
#         lb = pd.to_numeric(df["le_book"], errors="coerce")
#         scope = df[lb.isin(INSURANCE_LE_BOOKS_GI)].copy()
#         if scope.empty:
#             return None
#         dt = scope["deal_type"].astype(str).str.strip().str.upper()
#         valid = int((dt == "GENINS").sum())
#         return valid, len(dt) - valid, len(dt)

#     # ── ACC-032: LI LE_BOOKs must have LIFINS deal_type ──────────────────────
#     if rule_id == "ACC-032":
#         needed = ["le_book", "deal_type"]
#         if not all(c in df.columns for c in needed):
#             return None
#         lb = pd.to_numeric(df["le_book"], errors="coerce")
#         scope = df[lb.isin(INSURANCE_LE_BOOKS_LI)].copy()
#         if scope.empty:
#             return None
#         dt = scope["deal_type"].astype(str).str.strip().str.upper()
#         valid = int((dt == "LIFINS").sum())
#         return valid, len(dt) - valid, len(dt)

#     # ── ACC-033: GENINS contracts must use approved deal sub-types ────────────
#     if rule_id == "ACC-033":
#         needed = ["deal_type", "deal_sub_type"]
#         if not all(c in df.columns for c in needed):
#             return None
#         dt  = df["deal_type"].astype(str).str.strip().str.upper()
#         scope = df[dt == "GENINS"].copy()
#         if scope.empty:
#             return None
#         dst = scope["deal_sub_type"].astype(str).str.strip().str.upper()
#         valid = int(dst.isin(VALID_GI_DEAL_SUB_TYPES).sum())
#         return valid, len(dst) - valid, len(dst)

#     # ── ACC-034: LIFINS contracts must use approved deal sub-types ────────────
#     if rule_id == "ACC-034":
#         needed = ["deal_type", "deal_sub_type"]
#         if not all(c in df.columns for c in needed):
#             return None
#         dt  = df["deal_type"].astype(str).str.strip().str.upper()
#         scope = df[dt == "LIFINS"].copy()
#         if scope.empty:
#             return None
#         dst = scope["deal_sub_type"].astype(str).str.strip().str.upper()
#         valid = int(dst.isin(VALID_LI_DEAL_SUB_TYPES).sum())
#         return valid, len(dst) - valid, len(dst)

#     # ── ACC-035: insurance contract status must be 0, 1, or 2 ────────────────
#     if rule_id == "ACC-035":
#         if "contract_status" not in df.columns:
#             return None
#         lb = pd.to_numeric(df.get("le_book", pd.Series(dtype="object")),
#                            errors="coerce")
#         is_ins = lb.isin(INSURANCE_LE_BOOKS_GI | INSURANCE_LE_BOOKS_LI)
#         scope = df[is_ins].copy()
#         if scope.empty:
#             return None
#         s = pd.to_numeric(scope["contract_status"], errors="coerce").dropna()
#         if s.empty:
#             return None
#         valid = int(s.isin(VALID_INS_CONTRACT_STATUS).sum())
#         return valid, len(s) - valid, len(s)

#     # ── ACC-036: regulatory_provision must not equal provision_held ───────────
#     if rule_id == "ACC-036":
#         needed = ["regulatory_provision", "provision_held"]
#         if not all(c in df.columns for c in needed):
#             return None
#         rp = pd.to_numeric(df["regulatory_provision"], errors="coerce")
#         ph = pd.to_numeric(df["provision_held"],       errors="coerce")
#         pair = pd.DataFrame({"rp": rp, "ph": ph}).dropna()
#         if pair.empty:
#             return None
#         valid = int((pair["rp"] != pair["ph"]).sum())
#         return valid, len(pair) - valid, len(pair)

#     # ── ACC-037: performance_class must match overdue-day bracket ─────────────
#     if rule_id == "ACC-037":
#         needed = ["performance_class", "date_past_due", "year_month", "le_book"]
#         if not all(c in df.columns for c in needed):
#             return None

#         pc  = df["performance_class"].astype(str).str.strip().str.upper()
#         dpd = pd.to_datetime(df["date_past_due"], errors="coerce", utc=False)
#         if getattr(dpd.dt, "tz", None): dpd = dpd.dt.tz_localize(None)
#         lb  = pd.to_numeric(df["le_book"], errors="coerce")

#         # derive month-end from year_month (stored as YYYYMM integer or string)
#         ym_str = df["year_month"].astype(str).str.strip().str[:6]
#         month_end = pd.to_datetime(ym_str, format="%Y%m", errors="coerce") + \
#                     pd.offsets.MonthEnd(0)

#         work = pd.DataFrame({
#             "pc": pc, "dpd": dpd, "lb": lb, "me": month_end
#         })
#         # only check non-normal loans with valid past-due dates
#         non_nl = work["pc"] != "NL"
#         valid_dpd = work["dpd"].notna() & (work["dpd"].dt.year > 1900)
#         scope = work[non_nl & valid_dpd & work["me"].notna() & work["lb"].notna()].copy()
#         if scope.empty:
#             return None

#         overdue = (scope["me"] - scope["dpd"]).dt.days.clip(lower=0)
#         is_bank = (scope["lb"] >= 10) & (scope["lb"] < 90)

#         def _expected(row):
#             od, bank = row["overdue"], row["is_bank"]
#             if bank:
#                 if od < 30:   return "NL"
#                 if od < 90:   return "WL"
#                 if od < 180:  return "SL"
#                 if od < 365:  return "DL"
#                 if od <= 720: return "LL"
#                 return "WO"
#             else:
#                 if od == 0:   return "NL"
#                 if od < 30:   return "WL"
#                 if od < 90:   return "SL"
#                 if od < 180:  return "DL"
#                 if od < 365:  return "LL"
#                 return "WO"

#         scope = scope.copy()
#         scope["overdue"]  = overdue
#         scope["is_bank"]  = is_bank
#         scope["expected"] = scope.apply(_expected, axis=1)
#         valid = int((scope["pc"] == scope["expected"]).sum())
#         return valid, len(scope) - valid, len(scope)

#     log.warning("Unknown rule_id: %s", rule_id)
#     return None


# def run_rule_mask(rule_id: str, df: pd.DataFrame) -> pd.Series:
#     """Return bool Series (True = row fails the rule, same index as df)."""
#     false = pd.Series(False, index=df.index)
#     if df.empty:
#         return false

#     if rule_id == "ACC-001":
#         if "le_book" not in df.columns:
#             return false
#         s = df["le_book"].astype(str).str.strip()
#         return df["le_book"].notna() & ~s.isin(VALID_LE_BOOKS)

#     if rule_id == "ACC-002":
#         if "account_status" not in df.columns:
#             return false
#         s = pd.to_numeric(df["account_status"], errors="coerce")
#         return s.notna() & ~s.isin(VALID_ACCOUNT_STATUS)

#     if rule_id == "ACC-003":
#         if "performance_class" not in df.columns:
#             return false
#         s = df["performance_class"].astype(str).str.strip().str.upper()
#         return df["performance_class"].notna() & ~s.isin(VALID_PERFORMANCE_CLASS)

#     if rule_id == "ACC-004":
#         if "customer_gender" not in df.columns:
#             return false
#         s = df["customer_gender"].astype(str).str.strip().str.upper()
#         return df["customer_gender"].notna() & ~s.isin(VALID_GENDER)

#     if rule_id == "ACC-005":
#         if "account_type" not in df.columns:
#             return false
#         s = df["account_type"].astype(str).str.strip().str.upper()
#         return df["account_type"].notna() & ~s.isin(VALID_ACCOUNT_TYPE)

#     if rule_id == "ACC-010":
#         needed = ["customer_gender", "legal_status"]
#         if not all(c in df.columns for c in needed):
#             return false
#         both_notna  = df["customer_gender"].notna() & df["legal_status"].notna()
#         ls          = pd.to_numeric(df["legal_status"], errors="coerce")
#         is_corp     = ls.isin(CORPORATE_LEGAL_STATUS)
#         gender_is_c = df["customer_gender"].astype(str).str.strip().str.upper() == "C"
#         return both_notna & is_corp & ~gender_is_c

#     if rule_id == "ACC-011":
#         needed = ["account_type", "vision_sbu"]
#         if not all(c in df.columns for c in needed):
#             return false
#         both_notna = df["account_type"].notna() & df["vision_sbu"].notna()
#         is_pension  = df["account_type"].astype(str).str.strip().str.upper().isin(PENSION_ACCOUNT_TYPES)
#         is_retl     = df["vision_sbu"].astype(str).str.strip().str.upper() == "RETL"
#         return both_notna & is_pension & is_retl

#     if rule_id == "ACC-012":
#         needed = ["marital_status", "customer_gender"]
#         if not all(c in df.columns for c in needed):
#             return false
#         both_notna    = df["marital_status"].notna() & df["customer_gender"].notna()
#         is_corp       = df["customer_gender"].astype(str).str.strip().str.upper() == "C"
#         marital_is_na = df["marital_status"].astype(str).str.strip().str.upper() == "NA"
#         return both_notna & is_corp & ~marital_is_na

#     if rule_id == "ACC-013":
#         if "le_book" not in df.columns:
#             return false
#         s = df["le_book"].astype(str).str.strip()
#         return df["le_book"].notna() & ~((s.str.len() == 3) & s.str.match(r"^\d{3}$", na=False))

#     if rule_id == "ACC-014":
#         if "account_status" not in df.columns: return false
#         s = pd.to_numeric(df["account_status"], errors="coerce")
#         return s.notna() & (s != 0)

#     if rule_id == "ACC-015":
#         if not all(c in df.columns for c in ["account_type", "performance_class"]): return false
#         has_pc = df["performance_class"].notna()
#         at = df["account_type"].astype(str).str.strip().str.upper()
#         return has_pc & (at != "LAA")

#     if rule_id == "ACC-016":
#         if not all(c in df.columns for c in ["account_type", "performance_class", "account_status"]): return false
#         at  = df["account_type"].astype(str).str.strip().str.upper()
#         pc  = df["performance_class"].astype(str).str.strip().str.upper()
#         ast = pd.to_numeric(df["account_status"], errors="coerce")
#         return (at == "LAA") & (pc == "WO") & ast.notna() & (ast != 2)

#     if rule_id == "ACC-017":
#         if "vision_sbu" not in df.columns: return false
#         s = df["vision_sbu"].astype(str).str.strip().str.upper()
#         return df["vision_sbu"].notna() & s.isin({"NOT APPLICABLE", "OTHERS", "NOT_APPLICABLE", "OTHER"})

#     if rule_id == "ACC-018":
#         if not all(c in df.columns for c in ["account_status", "last_tran_date"]): return false
#         ast = pd.to_numeric(df["account_status"], errors="coerce")
#         ldt = pd.to_datetime(df["last_tran_date"], errors="coerce", utc=False)
#         if getattr(ldt.dt, "tz", None): ldt = ldt.dt.tz_localize(None)
#         days_since = (pd.Timestamp.today().normalize() - ldt).dt.days
#         return (ast == 0) & ldt.notna() & (days_since >= DORMANCY_DAYS)

#     if rule_id == "ACC-019":
#         if not all(c in df.columns for c in ["account_status", "account_type", "le_book"]): return false
#         ast = pd.to_numeric(df["account_status"], errors="coerce")
#         at  = df["account_type"].astype(str).str.strip().str.upper()
#         lb  = pd.to_numeric(df["le_book"], errors="coerce")
#         is_ins_lb = lb.isin(INSURANCE_LE_BOOKS_GI | INSURANCE_LE_BOOKS_LI)
#         return is_ins_lb & (ast == 0) & ~at.isin(INSURANCE_ACCOUNT_TYPES)

#     if rule_id == "ACC-020":
#         if not all(c in df.columns for c in ["account_status", "le_book"]): return false
#         lb  = pd.to_numeric(df["le_book"], errors="coerce")
#         ast = pd.to_numeric(df["account_status"], errors="coerce")
#         return lb.isin(INSURANCE_LE_BOOKS_GI | INSURANCE_LE_BOOKS_LI) & \
#                ast.notna() & ~ast.isin(INSURANCE_STATUS_VALID)

#     if rule_id == "ACC-021":
#         if "customer_status" not in df.columns: return false
#         s = pd.to_numeric(df["customer_status"], errors="coerce")
#         return s.notna() & (s != 0)

#     if rule_id == "ACC-022":
#         if not all(c in df.columns for c in ["customer_gender", "legal_status"]): return false
#         is_indiv = df["customer_gender"].astype(str).str.strip().str.upper() != "C"
#         ls = pd.to_numeric(df["legal_status"], errors="coerce")
#         return df["customer_gender"].notna() & df["legal_status"].notna() & \
#                is_indiv & ls.isin(ALL_CORPORATE_LEGAL_STATUS)

#     if rule_id == "ACC-023":
#         if not all(c in df.columns for c in ["national_id_type", "customer_tin", "customer_gender"]): return false
#         is_indiv = df["customer_gender"].astype(str).str.strip().str.upper() != "C"
#         id_type  = pd.to_numeric(df["national_id_type"], errors="coerce")
#         has_tin  = df["customer_tin"].notna() & (df["customer_tin"].astype(str).str.strip() != "")
#         return df["national_id_type"].notna() & is_indiv & id_type.isin(REGISTRATION_NUMBER_CODE) & has_tin

#     if rule_id == "ACC-024":
#         if not all(c in df.columns for c in ["customer_gender", "vision_sbu"]): return false
#         is_indiv = df["customer_gender"].astype(str).str.strip().str.upper() != "C"
#         sbu = df["vision_sbu"].astype(str).str.strip().str.upper()
#         return df["customer_gender"].notna() & df["vision_sbu"].notna() & is_indiv & (sbu != "RETL")

#     if rule_id == "ACC-025":
#         if not all(c in df.columns for c in ["customer_gender", "vision_sbu"]): return false
#         is_corp = df["customer_gender"].astype(str).str.strip().str.upper() == "C"
#         sbu = df["vision_sbu"].astype(str).str.strip().str.upper()
#         return df["customer_gender"].notna() & df["vision_sbu"].notna() & \
#                is_corp & ~sbu.isin(VALID_CORP_VISION_SBU)

#     if rule_id == "ACC-026":
#         if not all(c in df.columns for c in ["customer_gender", "marital_status"]): return false
#         is_indiv = df["customer_gender"].astype(str).str.strip().str.upper() != "C"
#         ms = df["marital_status"].astype(str).str.strip().str.upper()
#         return df["customer_gender"].notna() & df["marital_status"].notna() & is_indiv & (ms == "NA")

#     if rule_id == "ACC-027":
#         if "contract_status" not in df.columns: return false
#         s = pd.to_numeric(df["contract_status"], errors="coerce")
#         return s.notna() & (s != 0)

#     if rule_id == "ACC-028":
#         if not all(c in df.columns for c in ["deal_sub_type", "start_date", "maturity_date"]): return false
#         dst = df["deal_sub_type"].astype(str).str.strip().str.upper()
#         in_scope = dst.isin(VALID_LOAN_DEAL_SUB_TYPES)
#         sd = pd.to_datetime(df["start_date"],    errors="coerce", utc=False)
#         md = pd.to_datetime(df["maturity_date"], errors="coerce", utc=False)
#         if getattr(sd.dt, "tz", None): sd = sd.dt.tz_localize(None)
#         if getattr(md.dt, "tz", None): md = md.dt.tz_localize(None)
#         years = (md - sd).dt.days / 365.25
#         expected = pd.Series("", index=df.index)
#         expected[years < 1]                   = "SHTLON"
#         expected[(years >= 1) & (years <= 3)] = "MEDLON"
#         expected[years > 3]                   = "LNGLON"
#         return in_scope & sd.notna() & md.notna() & (dst != expected)

#     if rule_id == "ACC-029":
#         if not all(c in df.columns for c in ["deal_sub_type", "start_date", "maturity_date"]): return false
#         dst = df["deal_sub_type"].astype(str).str.strip().str.upper()
#         in_scope = dst.isin(VALID_DEP_DEAL_SUB_TYPES)
#         sd = pd.to_datetime(df["start_date"],    errors="coerce", utc=False)
#         md = pd.to_datetime(df["maturity_date"], errors="coerce", utc=False)
#         if getattr(sd.dt, "tz", None): sd = sd.dt.tz_localize(None)
#         if getattr(md.dt, "tz", None): md = md.dt.tz_localize(None)
#         years = (md - sd).dt.days / 365.25
#         expected = pd.Series("", index=df.index)
#         expected[years < 1]                   = "SHTDEP"
#         expected[(years >= 1) & (years <= 3)] = "MEDDEP"
#         expected[years > 3]                   = "LNGDEP"
#         return in_scope & sd.notna() & md.notna() & (dst != expected)

#     if rule_id == "ACC-030":
#         if not all(c in df.columns for c in ["deal_type", "vision_sbu"]): return false
#         dt  = df["deal_type"].astype(str).str.strip().str.upper()
#         sbu = df["vision_sbu"].astype(str).str.strip().str.upper()
#         return df["deal_type"].notna() & df["vision_sbu"].notna() & (dt == "TRSLON") & (sbu == "RETL")

#     if rule_id == "ACC-031":
#         if not all(c in df.columns for c in ["le_book", "deal_type"]): return false
#         lb = pd.to_numeric(df["le_book"], errors="coerce")
#         dt = df["deal_type"].astype(str).str.strip().str.upper()
#         return lb.isin(INSURANCE_LE_BOOKS_GI) & df["deal_type"].notna() & (dt != "GENINS")

#     if rule_id == "ACC-032":
#         if not all(c in df.columns for c in ["le_book", "deal_type"]): return false
#         lb = pd.to_numeric(df["le_book"], errors="coerce")
#         dt = df["deal_type"].astype(str).str.strip().str.upper()
#         return lb.isin(INSURANCE_LE_BOOKS_LI) & df["deal_type"].notna() & (dt != "LIFINS")

#     if rule_id == "ACC-033":
#         if not all(c in df.columns for c in ["deal_type", "deal_sub_type"]): return false
#         dt  = df["deal_type"].astype(str).str.strip().str.upper()
#         dst = df["deal_sub_type"].astype(str).str.strip().str.upper()
#         return (dt == "GENINS") & df["deal_sub_type"].notna() & ~dst.isin(VALID_GI_DEAL_SUB_TYPES)

#     if rule_id == "ACC-034":
#         if not all(c in df.columns for c in ["deal_type", "deal_sub_type"]): return false
#         dt  = df["deal_type"].astype(str).str.strip().str.upper()
#         dst = df["deal_sub_type"].astype(str).str.strip().str.upper()
#         return (dt == "LIFINS") & df["deal_sub_type"].notna() & ~dst.isin(VALID_LI_DEAL_SUB_TYPES)

#     if rule_id == "ACC-035":
#         if not all(c in df.columns for c in ["contract_status", "le_book"]): return false
#         lb = pd.to_numeric(df["le_book"], errors="coerce")
#         s  = pd.to_numeric(df["contract_status"], errors="coerce")
#         return lb.isin(INSURANCE_LE_BOOKS_GI | INSURANCE_LE_BOOKS_LI) & \
#                s.notna() & ~s.isin(VALID_INS_CONTRACT_STATUS)

#     if rule_id == "ACC-036":
#         if not all(c in df.columns for c in ["regulatory_provision", "provision_held"]): return false
#         rp = pd.to_numeric(df["regulatory_provision"], errors="coerce")
#         ph = pd.to_numeric(df["provision_held"],       errors="coerce")
#         return rp.notna() & ph.notna() & (rp == ph)

#     if rule_id == "ACC-037":
#         needed = ["performance_class", "date_past_due", "year_month", "le_book"]
#         if not all(c in df.columns for c in needed): return false
#         pc  = df["performance_class"].astype(str).str.strip().str.upper()
#         dpd = pd.to_datetime(df["date_past_due"], errors="coerce", utc=False)
#         if getattr(dpd.dt, "tz", None): dpd = dpd.dt.tz_localize(None)
#         lb  = pd.to_numeric(df["le_book"], errors="coerce")
#         ym_str   = df["year_month"].astype(str).str.strip().str[:6]
#         month_end = pd.to_datetime(ym_str, format="%Y%m", errors="coerce") + \
#                     pd.offsets.MonthEnd(0)
#         valid_dpd = dpd.notna() & (dpd.dt.year > 1900)
#         non_nl    = pc != "NL"
#         in_scope  = non_nl & valid_dpd & month_end.notna() & lb.notna()
#         overdue   = (month_end - dpd).dt.days.clip(lower=0)
#         is_bank   = (lb >= 10) & (lb < 90)
#         expected  = pd.Series("", index=df.index)
#         # banks
#         b = is_bank & in_scope
#         expected[b & (overdue < 30)]                    = "NL"
#         expected[b & (overdue >= 30)  & (overdue < 90)] = "WL"
#         expected[b & (overdue >= 90)  & (overdue < 180)]= "SL"
#         expected[b & (overdue >= 180) & (overdue < 365)]= "DL"
#         expected[b & (overdue >= 365) & (overdue <= 720)]="LL"
#         expected[b & (overdue > 720)]                   = "WO"
#         # MFIs
#         m = ~is_bank & in_scope
#         expected[m & (overdue == 0)]                    = "NL"
#         expected[m & (overdue >= 1)  & (overdue < 30)] = "WL"
#         expected[m & (overdue >= 30) & (overdue < 90)] = "SL"
#         expected[m & (overdue >= 90) & (overdue < 180)]= "DL"
#         expected[m & (overdue >= 180)& (overdue < 365)]= "LL"
#         expected[m & (overdue >= 365)]                  = "WO"
#         return in_scope & (pc != expected)

#     return false


# def evaluate_table(df: pd.DataFrame, table_name: str) -> dict:
#     # run all applicable rules on a DataFrame; build per-rule and per-le_book score breakdowns
#     rule_ids     = TABLE_RULES.get(table_name, [])
#     rules_out:   dict = {}
#     rule_scores: list[float] = []

#     for rule_id in rule_ids:
#         meta   = RULE_META[rule_id]
#         result = run_rule(rule_id, df)
#         if result is None:
#             continue
#         valid, invalid, total = result
#         score = _pct(valid, total)
#         rule_scores.append(score)

#         lb_breakdown: dict = {}
#         if "le_book" in df.columns:  # per le_book breakdown for this rule
#             for le_val in sorted(df["le_book"].dropna().unique()):
#                 sub_df = df[df["le_book"] == le_val]
#                 sub    = run_rule(rule_id, sub_df)
#                 if sub is None:
#                     continue
#                 sv, si, st = sub
#                 lb_breakdown[str(le_val)] = {
#                     "valid":          sv,
#                     "invalid":        si,
#                     "total":          st,
#                     "accuracy_score": _pct(sv, st),
#                 }

#         rules_out[rule_id] = {
#             "rule_name":         meta["name"],
#             "category":          meta["category"],
#             "fields":            meta["fields"],
#             "valid":             valid,
#             "invalid":           invalid,
#             "total":             total,
#             "accuracy_score":    score,
#             "le_book_breakdown": lb_breakdown,
#         }
#         log.info("  %s  score=%.2f%%  invalid=%d / %d",
#                  rule_id, score, invalid, total)

#     le_book_breakdown: dict = {}
#     if "le_book" in df.columns:  # table-level le_book breakdown: average rule scores per entity
#         for le_val in sorted(df["le_book"].dropna().unique()):
#             lb_key         = str(le_val)
#             lb_rule_scores: list[float] = []
#             lb_rules:       dict = {}
#             for rule_id, rdata in rules_out.items():
#                 lb = rdata["le_book_breakdown"].get(lb_key)
#                 if lb:
#                     lb_rule_scores.append(lb["accuracy_score"])
#                     lb_rules[rule_id] = {
#                         "rule_name":      rules_out[rule_id]["rule_name"],
#                         "accuracy_score": lb["accuracy_score"],
#                         "valid":          lb["valid"],
#                         "invalid":        lb["invalid"],
#                         "total":          lb["total"],
#                     }
#             if lb_rule_scores:
#                 le_book_breakdown[lb_key] = {
#                     "row_count":      int((df["le_book"] == le_val).sum()),
#                     "accuracy_score": round(sum(lb_rule_scores) / len(lb_rule_scores), 2),
#                     "rules":          lb_rules,
#                 }

#     overall = round(sum(rule_scores) / len(rule_scores), 2) if rule_scores else 0.0

#     return {
#         "status":            "evaluated",
#         "row_count":         len(df),
#         "rules_applied":     len(rules_out),
#         "accuracy_score":    overall,
#         "rules":             rules_out,
#         "le_book_breakdown": le_book_breakdown,
#     }


# def evaluate(engine, tables: list[str], db_schema: str,
#              limit: int, output_path: str) -> dict:
#     # orchestrate fetch → rule evaluation → report dict → JSON output for all tables
#     valid_le_books = get_valid_le_books(engine, db_schema)
#     report: dict = {
#         "generated_at": datetime.now().isoformat(timespec="seconds"),
#         "row_limit":    limit,
#         "schema":       db_schema,
#         "tables":       {},
#         "warnings":     {},
#     }

#     all_scores:   list[float] = []
#     all_le_books: set         = set()

#     for table_name in tables:
#         log.info("━━  Table: %s", table_name)
#         columns = ACCURACY_COLUMNS.get(table_name, [])
#         if not columns:
#             log.warning("  No accuracy columns defined — skipping.")
#             continue

#         df = fetch_table(engine, table_name, columns, db_schema, limit, valid_le_books)
#         if df.empty:
#             log.warning("  No data returned — skipping.")
#             report["tables"][table_name] = {"status": "no_data", "row_count": 0}
#             report["warnings"][table_name] = "Table returned 0 rows."
#             continue

#         tbl_report = evaluate_table(df, table_name)
#         report["tables"][table_name] = tbl_report
#         all_scores.append(tbl_report["accuracy_score"])
#         all_le_books.update(tbl_report["le_book_breakdown"].keys())

#         log.info("  Table accuracy: %.2f%%  (%d rules)", tbl_report["accuracy_score"],
#                  tbl_report["rules_applied"])

#     report["le_books"] = sorted(all_le_books)

#     evaluated = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
#     overall   = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0  # average across evaluated tables

#     report["executive_summary"] = {
#         "overall_accuracy_score": overall,
#         "total_tables":           len(report["tables"]),
#         "evaluated_tables":       len(evaluated),
#         "row_limit":              limit,
#     }

#     with open(output_path, "w", encoding="utf-8") as fh:
#         json.dump(report, fh, indent=2, default=str)
# #for table-level and overall accuracy scores
#     # log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
#     # log.info("  OVERALL ACCURACY  %.2f%%  (%d table(s) evaluated)", overall, len(evaluated))
#     # log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

#     return report

# def evaluate_from_dataframes(dataframes: dict, valid_le_books: frozenset,
#                               output_path: str) -> dict:
#     """Run accuracy checks on pre-loaded DataFrames (no DB connection needed)."""
#     global VALID_LE_BOOKS
#     VALID_LE_BOOKS = valid_le_books  # ACC-001 reads this global to check valid institution codes

#     report: dict = {
#         "generated_at": datetime.now().isoformat(timespec="seconds"),
#         "tables":       {},
#         "warnings":     {},
#     }
#     all_scores:   list[float] = []
#     all_le_books: set         = set()

#     for table_name in TARGET_TABLES:
#         df = dataframes.get(table_name, pd.DataFrame())
#         if df.empty:
#             report["tables"][table_name] = {"status": "no_data", "row_count": 0}
#             report["warnings"][table_name] = "No data in this period."
#             continue

#         tbl_report = evaluate_table(df, table_name)
#         report["tables"][table_name] = tbl_report
#         all_scores.append(tbl_report["accuracy_score"])
#         all_le_books.update(tbl_report["le_book_breakdown"].keys())
#         log.info("  %-30s  score=%.2f%%  (%d rules)",
#                  table_name, tbl_report["accuracy_score"], tbl_report["rules_applied"])

#     evaluated = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
#     overall   = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0

#     report["le_books"] = sorted(all_le_books)
#     report["executive_summary"] = {
#         "overall_accuracy_score": overall,
#         "total_tables":           len(report["tables"]),
#         "evaluated_tables":       len(evaluated),
#     }

#     with open(output_path, "w", encoding="utf-8") as fh:
#         json.dump(report, fh, indent=2, default=str)

#     log.info("Accuracy report → %s  (overall %.2f%%)", output_path, overall)
#     return report


# def _acc_rule_sql(rule_id: str, existing: set,
#                    valid_le_books: frozenset) -> tuple[str, str] | None:
#     """Return (total_expr, valid_expr) SQL strings for this rule, or None if cols missing."""
#     def has(*cols): return all(c in existing for c in cols)

#     if rule_id == "ACC-001":
#         if not has("le_book"): return None
#         lb_in = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
#         valid_expr = (
#             f'SUM(CASE WHEN "le_book" IS NOT NULL AND TRIM("le_book"::TEXT) IN ({lb_in}) THEN 1 ELSE 0 END)'
#             if lb_in else 'SUM(CASE WHEN "le_book" IS NOT NULL THEN 1 ELSE 0 END)'
#         )
#         return ('SUM(CASE WHEN "le_book" IS NOT NULL THEN 1 ELSE 0 END)', valid_expr)

#     if rule_id == "ACC-002":
#         if not has("account_status"): return None
#         vals = ", ".join(f"'{v}'" for v in sorted(str(x) for x in VALID_ACCOUNT_STATUS))
#         return (
#             'SUM(CASE WHEN "account_status" IS NOT NULL THEN 1 ELSE 0 END)',
#             f'SUM(CASE WHEN "account_status" IS NOT NULL AND "account_status"::TEXT IN ({vals}) THEN 1 ELSE 0 END)',
#         )

#     if rule_id == "ACC-003":
#         if not has("performance_class"): return None
#         vals = ", ".join(f"'{v}'" for v in sorted(VALID_PERFORMANCE_CLASS))
#         return (
#             'SUM(CASE WHEN "performance_class" IS NOT NULL THEN 1 ELSE 0 END)',
#             f'SUM(CASE WHEN "performance_class" IS NOT NULL AND UPPER(TRIM("performance_class"::TEXT)) IN ({vals}) THEN 1 ELSE 0 END)',
#         )

#     if rule_id == "ACC-004":
#         if not has("customer_gender"): return None
#         vals = ", ".join(f"'{v}'" for v in sorted(VALID_GENDER))
#         return (
#             'SUM(CASE WHEN "customer_gender" IS NOT NULL THEN 1 ELSE 0 END)',
#             f'SUM(CASE WHEN "customer_gender" IS NOT NULL AND UPPER(TRIM("customer_gender"::TEXT)) IN ({vals}) THEN 1 ELSE 0 END)',
#         )

#     if rule_id == "ACC-005":
#         if not has("account_type"): return None
#         vals = ", ".join(f"'{v}'" for v in sorted(VALID_ACCOUNT_TYPE))
#         return (
#             'SUM(CASE WHEN "account_type" IS NOT NULL THEN 1 ELSE 0 END)',
#             f'SUM(CASE WHEN "account_type" IS NOT NULL AND UPPER(TRIM("account_type"::TEXT)) IN ({vals}) THEN 1 ELSE 0 END)',
#         )

#     if rule_id == "ACC-010":
#         if not has("customer_gender", "legal_status"): return None
#         corp = ", ".join(f"'{v}'" for v in sorted(str(x) for x in CORPORATE_LEGAL_STATUS))
#         return (
#             'SUM(CASE WHEN "customer_gender" IS NOT NULL AND "legal_status" IS NOT NULL THEN 1 ELSE 0 END)',
#             f'SUM(CASE WHEN "customer_gender" IS NOT NULL AND "legal_status" IS NOT NULL '
#             f'AND ("legal_status"::TEXT NOT IN ({corp}) OR UPPER(TRIM("customer_gender"::TEXT)) = \'C\') THEN 1 ELSE 0 END)',
#         )

#     if rule_id == "ACC-011":
#         if not has("account_type", "vision_sbu"): return None
#         pension = ", ".join(f"'{v}'" for v in sorted(PENSION_ACCOUNT_TYPES))
#         return (
#             'SUM(CASE WHEN "account_type" IS NOT NULL AND "vision_sbu" IS NOT NULL THEN 1 ELSE 0 END)',
#             f'SUM(CASE WHEN "account_type" IS NOT NULL AND "vision_sbu" IS NOT NULL '
#             f'AND NOT (UPPER(TRIM("account_type"::TEXT)) IN ({pension}) AND UPPER(TRIM("vision_sbu"::TEXT)) = \'RETL\') THEN 1 ELSE 0 END)',
#         )

#     if rule_id == "ACC-012":
#         if not has("customer_gender", "marital_status"): return None
#         return (
#             'SUM(CASE WHEN "customer_gender" IS NOT NULL AND "marital_status" IS NOT NULL THEN 1 ELSE 0 END)',
#             'SUM(CASE WHEN "customer_gender" IS NOT NULL AND "marital_status" IS NOT NULL '
#             'AND (UPPER(TRIM("customer_gender"::TEXT)) != \'C\' OR UPPER(TRIM("marital_status"::TEXT)) = \'NA\') THEN 1 ELSE 0 END)',
#         )

#     if rule_id == "ACC-013":
#         if not has("le_book"): return None
#         return (
#             'SUM(CASE WHEN "le_book" IS NOT NULL THEN 1 ELSE 0 END)',
#             r"""SUM(CASE WHEN "le_book" IS NOT NULL AND "le_book"::TEXT ~ '^[0-9]{3}$' THEN 1 ELSE 0 END)""",
#         )

#     return None


# def evaluate_from_sql(engine, schema: str, valid_le_books: frozenset,
#                        window_days: int, watermarks: dict, output_path: str,
#                        row_limit: int = 0) -> dict:
#     """Run accuracy checks in pure SQL — one query per table, no DataFrames."""
#     from sqlalchemy import text as _text

#     global VALID_LE_BOOKS
#     VALID_LE_BOOKS = valid_le_books

#     report: dict = {
#         "generated_at": datetime.now().isoformat(timespec="seconds"),
#         "tables":       {},
#         "warnings":     {},
#     }
#     all_scores:   list[float] = []
#     all_le_books: set         = set()

#     lb_clause = (
#         'AND "le_book" IN (' + ", ".join(f"'{lb}'" for lb in sorted(valid_le_books)) + ")"
#         if valid_le_books else ""
#     )

#     with engine.connect() as conn:
#         for table in TARGET_TABLES:
#             log.info("━━  %s", table)
#             rule_ids = TABLE_RULES.get(table, [])
#             acc_cols = ACCURACY_COLUMNS.get(table, [])
#             if not rule_ids or not acc_cols:
#                 continue

#             sq = f'"{schema}"."{table}"'
#             wanted = list(set(acc_cols) | {"le_book", "date_creation", "date_last_modified"})
#             existing = {
#                 r[0] for r in conn.execute(_text("""
#                     SELECT column_name FROM information_schema.columns
#                     WHERE table_schema = :s AND table_name = :t
#                       AND column_name = ANY(:cols)
#                 """), {"s": schema, "t": table, "cols": wanted}).fetchall()
#             }

#             # Build per-rule SQL expressions
#             rule_exprs: dict[str, tuple[str, str]] = {}
#             for rid in rule_ids:
#                 exprs = _acc_rule_sql(rid, existing, valid_le_books)
#                 if exprs:
#                     rule_exprs[rid] = exprs

#             if not rule_exprs:
#                 report["tables"][table] = {"status": "no_data", "row_count": 0}
#                 report["warnings"][table] = "No applicable accuracy columns found."
#                 continue

#             wm     = watermarks.get(table)
#             anchor = f"'{wm[:10]}'::date" if wm else "CURRENT_DATE"
#             date_parts = []
#             if "date_creation" in existing:
#                 date_parts.append(
#                     f'"date_creation" BETWEEN {anchor} - INTERVAL \'{window_days} days\' AND {anchor}'
#                 )
#             if "date_last_modified" in existing:
#                 date_parts.append(
#                     f'"date_last_modified" > \'{wm}\'' if wm else
#                     f'"date_last_modified" BETWEEN {anchor} - INTERVAL \'{window_days} days\' AND {anchor}'
#                 )
#             date_clause = "(" + " OR ".join(date_parts) + ")" if date_parts else "TRUE"

#             # Collect all unique columns needed for the CTE scope
#             scope_cols  = sorted({"le_book"} & existing | {c for c in acc_cols if c in existing})
#             has_lb      = "le_book" in existing
#             lb_select   = '"le_book", ' if has_lb else ""
#             group_by    = 'GROUP BY "le_book" ORDER BY "le_book"' if has_lb else ""

#             rule_selects = []
#             for rid, (tot_expr, val_expr) in rule_exprs.items():
#                 rkey = rid.lower().replace("-", "")
#                 rule_selects.append(f"{tot_expr} AS {rkey}_total,\n       {val_expr} AS {rkey}_valid")

#             limit_clause = f"LIMIT {row_limit}" if row_limit > 0 else ""
#             sql = f"""
#                 WITH scope AS (
#                     SELECT {", ".join(f'"{c}"' for c in scope_cols)}
#                     FROM   {sq}
#                     WHERE  {date_clause}
#                     {lb_clause}
#                     {limit_clause}
#                 )
#                 SELECT {lb_select}COUNT(*) AS total_rows,
#                        {chr(10) + '       ,'.join(rule_selects)}
#                 FROM scope
#                 {group_by}
#             """

#             try:
#                 rows = conn.execute(_text(sql)).mappings().fetchall()
#             except Exception as exc:
#                 log.error("  %s: query failed — %s", table, exc)
#                 conn.rollback()
#                 report["tables"][table] = {"status": "no_data", "row_count": 0}
#                 report["warnings"][table] = str(exc)
#                 continue

#             if not rows:
#                 report["tables"][table] = {"status": "no_data", "row_count": 0}
#                 report["warnings"][table] = "No rows in window."
#                 continue

#             # Aggregate across le_books
#             total_rows = sum(int(r["total_rows"]) for r in rows)

#             rules_out: dict      = {}
#             rule_scores: list[float] = []
#             lb_rule_scores: dict[str, list[float]] = {}

#             for rid in rule_exprs:
#                 rkey    = rid.lower().replace("-", "")
#                 r_total = sum(int(r.get(f"{rkey}_total") or 0) for r in rows)
#                 r_valid = sum(int(r.get(f"{rkey}_valid") or 0) for r in rows)
#                 if r_total == 0:
#                     continue
#                 score = _pct(r_valid, r_total)
#                 rule_scores.append(score)
#                 meta  = RULE_META[rid]

#                 lb_breakdown: dict = {}
#                 if has_lb:
#                     for r in rows:
#                         lb      = str(r["le_book"])
#                         all_le_books.add(lb)
#                         lb_tot  = int(r.get(f"{rkey}_total") or 0)
#                         lb_val  = int(r.get(f"{rkey}_valid") or 0)
#                         if lb_tot == 0:
#                             continue
#                         lb_score = _pct(lb_val, lb_tot)
#                         lb_breakdown[lb] = {
#                             "valid": lb_val, "invalid": lb_tot - lb_val,
#                             "total": lb_tot, "accuracy_score": lb_score,
#                         }
#                         lb_rule_scores.setdefault(lb, []).append(lb_score)

#                 rules_out[rid] = {
#                     "rule_name": meta["name"], "category": meta["category"],
#                     "fields": meta["fields"],
#                     "valid": r_valid, "invalid": r_total - r_valid,
#                     "total": r_total, "accuracy_score": score,
#                     "le_book_breakdown": lb_breakdown,
#                 }
#                 log.info("  %s  score=%.2f%%  invalid=%d / %d", rid, score, r_total - r_valid, r_total)

#             if not rule_scores:
#                 continue

#             table_score   = round(sum(rule_scores) / len(rule_scores), 2)
#             all_scores.append(table_score)

#             le_book_breakdown: dict = {}
#             for lb, lb_scores in lb_rule_scores.items():
#                 lb_row = max(
#                     rules_out[rid]["le_book_breakdown"].get(lb, {}).get("total", 0)
#                     for rid in rules_out
#                 )
#                 le_book_breakdown[lb] = {
#                     "row_count":      lb_row,
#                     "accuracy_score": round(sum(lb_scores) / len(lb_scores), 2),
#                     "rules": {
#                         rid: {
#                             "rule_name":      rules_out[rid]["rule_name"],
#                             "accuracy_score": rules_out[rid]["le_book_breakdown"].get(lb, {}).get("accuracy_score", 0.0),
#                             **{k: rules_out[rid]["le_book_breakdown"].get(lb, {}).get(k, 0)
#                                for k in ("valid", "invalid", "total")},
#                         }
#                         for rid in rules_out if lb in rules_out[rid]["le_book_breakdown"]
#                     },
#                 }

#             report["tables"][table] = {
#                 "status": "evaluated", "row_count": total_rows,
#                 "rules_applied": len(rules_out), "accuracy_score": table_score,
#                 "rules": rules_out, "le_book_breakdown": le_book_breakdown,
#             }
#             log.info("  Table accuracy: %.2f%%  (%d rules)", table_score, len(rules_out))

#     evaluated = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
#     overall   = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0
#     report["le_books"] = sorted(all_le_books)
#     report["executive_summary"] = {
#         "overall_accuracy_score": overall,
#         "total_tables":           len(report["tables"]),
#         "evaluated_tables":       len(evaluated),
#     }
#     with open(output_path, "w", encoding="utf-8") as fh:
#         json.dump(report, fh, indent=2, default=str)
#     log.info("Accuracy report → %s  (overall %.2f%%)", output_path, overall)
#     return report


# #main function
# def main():
#     # CLI entrypoint: parse args, load .env, connect to DB, run evaluate, log summary
#     parser = argparse.ArgumentParser(
#         description="DQ Accuracy Engine — BNR Upload Format For Guidelines v4.1",
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#         epilog="""
# Examples:
#   python dq_accuracy_engine.py
#   python dq_accuracy_engine.py --limit 1000
#   python dq_accuracy_engine.py --limit 0          # full tables
#   python dq_accuracy_engine.py --tables accounts contracts_expanded
#   python dq_accuracy_engine.py --schema data_quality_program --output dq_accuracy_report.json
#   python dq_accuracy_engine.py --env /path/to/.env
#         """,
#     )
#     parser.add_argument("--tables", nargs="+", default=TARGET_TABLES)
#     parser.add_argument("--schema", default="data_quality_program")
#     parser.add_argument("--limit",  type=int, default=100000)
#     parser.add_argument("--output", default="dq_accuracy_report.json")
#     parser.add_argument("--env",    default=".env")
#     args = parser.parse_args()

#     env_path = Path(args.env)
#     if env_path.exists():
#         load_dotenv(dotenv_path=env_path, override=True)
#         log.info("Loaded .env from: %s", env_path.resolve())
#     else:
#         log.warning(".env not found at '%s' — using shell environment.", env_path)

#     log.info("DQ Accuracy Engine")
#     log.info("  Tables : %s", ", ".join(args.tables))
#     log.info("  Schema : %s", args.schema)
#     log.info("  Limit  : %s", f"{args.limit:,} rows" if args.limit else "full table")
#     log.info("  Output : %s", args.output)

#     conn_str = build_connection_string()
#     engine   = get_engine(conn_str)

#     report = evaluate(engine, args.tables, args.schema, args.limit, args.output)
#     s      = report.get("executive_summary", {})
#     log.info("Report written → %s", args.output)
#     log.info("    Overall Accuracy : %.2f%%", s.get("overall_accuracy_score", 0.0))
#     log.info("    Tables evaluated : %d / %d",
#              s.get("evaluated_tables", 0), s.get("total_tables", 0))

#     if report.get("warnings"):
#         log.warning("Tables with issues:")
#         for tbl, msg in report["warnings"].items():
#             log.warning("  %-40s  %s", tbl, msg)


# if __name__ == "__main__":
#     main()
