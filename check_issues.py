from storage.postgres.app_db import get_connection

con = get_connection()

# Resolved issues for Bank Of Kigali Plc in Aug 2026 — now in dq_resolved_issues
rows = con.execute("""
SELECT
    le_book, table_name, rule_id, rule_name, detected_at, resolved_at
FROM dq_resolved_issues
WHERE institution_name='Bank Of Kigali Plc'
  AND resolved_at>='2026-08-01' AND resolved_at<'2026-08-31'
ORDER BY table_name ASC;
""").fetchall()

for row in rows:
    print(dict(row))
