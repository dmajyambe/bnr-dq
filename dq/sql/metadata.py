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
