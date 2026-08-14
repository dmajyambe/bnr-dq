# Flask streaming routes for issue-report downloads — moved from dq_dashboard_dash.py.
# Imported once from the bottom of dashboard/app.py (after `server` exists) so
# the @server.route decorators register against the real Flask app.
from __future__ import annotations

from dashboard.app import server
from dashboard.data import _DIR


def _check_download_auth(le_book: str) -> bool:
    """Return True if the current session is authorised to download reports for le_book.
    BNR staff (any bnr_* role) can download any institution's report.
    Institution users can only download their own le_book."""
    from flask import session as _fs
    from auth.users import BNR_ROLES
    role = _fs.get("user_role", "")
    if not role:
        return False
    if role in BNR_ROLES:
        return True
    # institution user — verify le_book matches their own
    from auth.users import get_user_institutions, get_user_by_email
    email = _fs.get("user_email", "")
    user  = get_user_by_email(email) if email else None
    if not user:
        return False
    allowed = get_user_institutions(user["user_id"])
    return str(le_book) in [str(lb) for lb in allowed]


@server.route("/download/issue-report/<le_book>")
def _serve_issue_report(le_book):
    """Stream an institution's latest issue-report ZIP directly (no base64 inlining).
    Used by the download buttons so large reports (100 MB+) download reliably."""
    import re as _re
    from flask import abort, send_file as _flask_send_file
    if not _re.fullmatch(r"[A-Za-z0-9_-]{1,20}", le_book or ""):
        abort(400)
    if not _check_download_auth(le_book):
        abort(403)
    d = _DIR / "issue_reports"
    zips = sorted([z for z in d.glob(f"{le_book}_*.zip")
                   if not z.name.endswith("_resolved.zip")], reverse=True) if d.exists() else []
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
    if not _check_download_auth(le_book):
        abort(403)
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


@server.route("/download/resolved/<le_book>")
def _serve_resolved_prebuilt(le_book):
    """Serve the pre-built resolved-issues ZIP written by the resolution scan pipeline."""
    import re as _re
    from flask import abort, send_file as _flask_send_file
    if not _re.fullmatch(r"[A-Za-z0-9_-]{1,20}", le_book or ""):
        abort(400)
    if not _check_download_auth(le_book):
        abort(403)
    d = _DIR / "issue_reports"
    zips = sorted(d.glob(f"{le_book}_*_resolved.zip"), reverse=True) if d.exists() else []
    if not zips:
        abort(404)
    return _flask_send_file(str(zips[0]), as_attachment=True, download_name=zips[0].name)


@server.route("/download/resolved/<le_book>/<table>")
def _serve_resolved_table(le_book, table):
    """Serve a single {table}.xlsx from the correct month's resolved ZIP.

    If a ?month=YYYY-MM param is supplied (set by the dashboard button), serve
    exactly that month's ZIP so the download always matches the 'Report Month'
    shown on screen.  Falls back to iterating newest-first if no month given.
    """
    import re as _re, io as _io, zipfile as _zip
    from flask import abort, request, send_file as _flask_send_file
    if (not _re.fullmatch(r"[A-Za-z0-9_-]{1,20}", le_book or "")
            or not _re.fullmatch(r"[A-Za-z0-9_]{1,40}", table or "")):
        abort(400)
    if not _check_download_auth(le_book):
        abort(403)
    d      = _DIR / "issue_reports"
    member = f"{table}.xlsx"
    month  = request.args.get("month", "")  # e.g. "2026-07"

    if month and _re.fullmatch(r"\d{4}-\d{2}", month):
        # serve the specific month's ZIP — exact match, no guessing
        specific = d / f"{le_book}_{month}_resolved.zip"
        zips = [specific] if specific.exists() else []
    else:
        zips = sorted(d.glob(f"{le_book}_*_resolved.zip"), reverse=True) if d.exists() else []

    for zp in zips:
        try:
            with _zip.ZipFile(zp) as zf:
                if member in zf.namelist():
                    data       = zf.read(member)
                    zip_month  = zp.stem.split("_", 1)[-1]   # e.g. "2026-07_resolved"
                    return _flask_send_file(
                        _io.BytesIO(data), as_attachment=True,
                        download_name=f"{table}_{le_book}_{zip_month}.xlsx",
                        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
        except Exception:
            continue
    abort(404)


