# Schema validation.
from __future__ import annotations
from sqlalchemy import text

#which of the wanted columns exist in the schema
def existing_columns(conn, schema: str, table: str, wanted: set[str]) -> set[str]:
    return {
        r[0] for r in conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = :s AND table_name = :t AND column_name = ANY(:cols)
        """), {"s": schema, "t": table, "cols": list(wanted)}).fetchall()
    }

#check if columns exist in the schema
def all_columns(conn, schema: str, table: str) -> set[str]:
    rows = conn.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t
    """), {"s": schema, "t": table}).fetchall()
    return {r[0].lower() for r in rows}


_TYPE_MAP = {
    # Numerical
    "integer": "Numerical", "bigint": "Numerical", "smallint": "Numerical",
    "numeric": "Numerical", "decimal": "Numerical", "real": "Numerical",
    "double precision": "Numerical", "float": "Numerical",
    "int2": "Numerical", "int4": "Numerical", "int8": "Numerical",
    # Date / Time
    "date": "Date", "timestamp": "Date", "timestamp without time zone": "Date",
    "timestamp with time zone": "Date", "time": "Date",
    "time without time zone": "Date", "time with time zone": "Date",
    "timetz": "Date", "timestamptz": "Date",
    # Boolean
    "boolean": "Boolean", "bool": "Boolean",
    # Text (catch-all below)
}

def column_types(conn, schema: str, table: str) -> dict[str, str]:
    """Return {column_name: human_readable_type} for all columns in the table."""
    rows = conn.execute(text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t
    """), {"s": schema, "t": table}).fetchall()
    result = {}
    for col, raw_type in rows:
        raw = (raw_type or "").lower().strip()
        result[col.lower()] = _TYPE_MAP.get(raw, "Text")
    return result
