# Generic JSON read/write — replaces the inline json.loads(Path(...).read_text())
# / json.dumps(...) pairs that were duplicated across dq_dashboard_dash.py,
# dq_inst_portal.py, and dq_monthly_detection.py.
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def write_json(path: Path, data: Any, indent: int = 2) -> None:
    path.write_text(json.dumps(data, indent=indent, default=str))
