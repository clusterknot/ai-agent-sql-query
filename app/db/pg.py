from __future__ import annotations
from typing import Any, Dict, List
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.append(ROOT)
from app.config import DSN

engine: Engine = create_engine(DSN, pool_pre_ping=True, future=True)

def run_sql(sql: str, limit_timeout_ms: int = 15000) -> Dict[str, Any]:
    with engine.connect() as conn:
        conn.execute(text(f"SET statement_timeout = {int(limit_timeout_ms)}"))
        res = conn.execute(text(sql))
        cols = list(res.keys())
        rows = [dict(zip(cols, r)) for r in res.fetchall()]
    return {"columns": cols, "rows": rows}

def explain_sql(sql: str) -> Dict[str, Any]:
    with engine.connect() as conn:
        res = conn.execute(text(f"EXPLAIN (FORMAT JSON) {sql}"))
        plan = res.fetchone()[0][0]  # EXPLAIN JSON returns array with one dict
    # Extract quick signals
    est_rows = plan.get("Plan", {}).get("Plan Rows", 0)
    total_cost = plan.get("Plan", {}).get("Total Cost", 0.0)
    return {"raw": plan, "est_rows": int(est_rows), "est_cost": float(total_cost)}
