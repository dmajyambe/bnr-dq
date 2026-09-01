from sqlalchemy import text
from storage.postgres.connection import get_engine

with get_engine().connect() as con:
    # Resolved issues for Bank Of Kigali Plc in Aug 2026 — now in dq_resolved_issues
    rows = con.execute(text("""
SELECT
    le_book, table_name, rule_id, rule_name, detected_at, resolved_at
FROM dq_resolved_issues
WHERE institution_name='Bank Of Kigali Plc'
  AND resolved_at>='2026-08-01' AND resolved_at<'2026-08-31'
ORDER BY table_name ASC;
""")).mappings().fetchall()

for row in rows:
    print(dict(row))
