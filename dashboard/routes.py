# Flask streaming routes for issue-report downloads — moved from dq_dashboard_dash.py.
# Imported once from the bottom of dashboard/app.py (after `server` exists) so
# the @server.route decorators register against the real Flask app.
from __future__ import annotations

from dashboard.app import server
from dashboard.data import _DIR


@server.route("/download/issue-report/<le_book>")
def _serve_issue_report(le_book):
    """Stream an institution's latest issue-report ZIP directly (no base64 inlining).
    Used by the download buttons so large reports (100 MB+) download reliably."""
    import re as _re
    from flask import abort, send_file as _flask_send_file
    if not _re.fullmatch(r"[A-Za-z0-9_-]{1,20}", le_book or ""):
        abort(400)
    d = _DIR / "issue_reports"
    zips = sorted(d.glob(f"{le_book}_*.zip"), reverse=True) if d.exists() else []
    if not zips:
        abort(404)
    return _flask_send_file(str(zips[0]), as_attachment=True, download_name=zips[0].name)


@server.route("/download/issue-report/<le_book>/<table>")
def _serve_issue_table(le_book, table):
    """Stream a single {table}.xlsx out of an institution's latest report ZIP."""
    import re as _re, io as _io, zipfile as _zip
    from flask import abort, send_file as _flask_send_file
    if (not _re.fullmatch(r"[A-Za-z0-9_-]{1,20}", le_book or "")
            or not _re.fullmatch(r"[A-Za-z0-9_]{1,40}", table or "")):
        abort(400)
    d = _DIR / "issue_reports"
    zips = sorted([z for z in d.glob(f"{le_book}_*.zip")
                   if not z.name.endswith("_resolved.zip")], reverse=True) if d.exists() else []
    if not zips:
        abort(404)
    member = f"{table}.xlsx"
    try:
        with _zip.ZipFile(zips[0]) as zf:
            if member not in zf.namelist():
                abort(404)
            data = zf.read(member)
    except Exception:
        abort(404)
    month = zips[0].stem.split("_", 1)[-1]
    return _flask_send_file(
        _io.BytesIO(data), as_attachment=True,
        download_name=f"{table}_{le_book}_{month}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@server.route("/download/resolved/<le_book>/<token>")
def _serve_resolved_report(le_book, token):
    """Stream a freshly-built resolved-issues ZIP (written to a temp file by the
    resolved-download callback). The temp file is deleted as it is served."""
    import re as _re, os as _os, io as _io
    from datetime import date as _date
    from flask import abort, send_file as _flask_send_file
    if (not _re.fullmatch(r"[A-Za-z0-9_-]{1,20}", le_book or "")
            or not _re.fullmatch(r"[a-f0-9]{32}", token or "")):
        abort(400)
    path = _DIR / "issue_reports" / ".tmp" / f"{token}.zip"
    if not path.exists():
        abort(404)
    data = path.read_bytes()
    try:
        _os.unlink(path)            # one-shot: delete as we serve it
    except OSError:
        pass
    return _flask_send_file(
        _io.BytesIO(data), as_attachment=True, mimetype="application/zip",
        download_name=f"dq_resolved_{le_book}_{_date.today().strftime('%Y-%m')}.zip")
