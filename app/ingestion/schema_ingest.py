from __future__ import annotations
from typing import List, Dict, Any
import yaml
from sqlalchemy import text, create_engine
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.append(ROOT)
from app.config import DSN, ALLOWED_SCHEMAS
from app.vector.faiss_store import add_texts

# Build small, descriptive "cards" for tables, columns, PK/FK, comments, and metrics.

def _list_tables(schema:str) -> List[str]:
    eng = create_engine(DSN, pool_pre_ping=True, future=True)
    q = text("""
      SELECT table_name FROM information_schema.tables
      WHERE table_schema=:s AND table_type='BASE TABLE'
      ORDER BY table_name
    """)
    with eng.connect() as c:
        return [r[0] for r in c.execute(q, {"s": schema})]

def _columns(schema:str, table:str) -> List[Dict[str,Any]]:
    eng = create_engine(DSN, pool_pre_ping=True, future=True)
    q = text("""
      SELECT column_name, data_type, is_nullable, column_default
      FROM information_schema.columns
      WHERE table_schema=:s AND table_name=:t
      ORDER BY ordinal_position
    """)
    with eng.connect() as c:
        # return [dict(r) for r in c.execute(q, {"s": schema, "t": table})]
        return [dict(zip(['column_name','data_type','is_nullable','default'], r)) for r in c.execute(q, {"s": schema, "t": table})]

def _pkeys(schema:str, table:str) -> List[str]:
    eng = create_engine(DSN, pool_pre_ping=True, future=True)
    q = text("""
      SELECT kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema
      WHERE tc.constraint_type='PRIMARY KEY' AND tc.table_schema=:s AND tc.table_name=:t
      ORDER BY kcu.ordinal_position
    """)
    with eng.connect() as c:
        return [r[0] for r in c.execute(q, {"s": schema, "t": table})]

def _fkeys(schema:str, table:str) -> List[Dict[str,str]]:
    eng = create_engine(DSN, pool_pre_ping=True, future=True)
    q = text("""
      SELECT kcu.column_name AS column, ccu.table_name AS ref_table, ccu.column_name AS ref_column
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema
      JOIN information_schema.constraint_column_usage ccu
        ON ccu.constraint_name=tc.constraint_name AND ccu.table_schema=tc.table_schema
      WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_schema=:s AND tc.table_name=:t
      ORDER BY 1
    """)
    with eng.connect() as c:
        return [dict(r) for r in c.execute(q, {"s": schema, "t": table})]

def _table_comment(schema: str, table: str) -> str:
    eng = create_engine(DSN, pool_pre_ping=True, future=True)
    # Fully-qualify the name; to_regclass('schema.table') → regclass or NULL
    q = text("SELECT obj_description(to_regclass(:tbl))")
    qualified = f"{schema}.{table}"
    with eng.connect() as c:
        row = c.execute(q, {"tbl": qualified}).fetchone()
        return (row[0] or "") if row and row[0] else ""

def _schema_card(schema:str, table:str) -> str:
    cols=_columns(schema, table)
    pks=_pkeys(schema, table)
    fks=_fkeys(schema, table)
    desc=_table_comment(schema, table)
    cols_txt="\n".join([f"- {c['column_name']} ({c['data_type']}, nullable={c['is_nullable']}, default={c['default']})" for c in cols])
    pks_txt=", ".join(pks) if pks else "(none)"
    fks_txt="\n".join([f"- {fk['column']} -> {fk['ref_table']}.{fk['ref_column']}" for fk in fks]) or "(none)"
    return f"""DB SCHEMA CARD
TABLE: {schema}.{table}
COLUMNS:
{cols_txt}
PRIMARY_KEY: {pks_txt}
FOREIGN_KEYS:
{fks_txt}
DESCRIPTION:
{desc}
""".strip()

def _metric_cards() -> List[str]:
    # Optional: load metric catalog (name, definition, table, filters, grain)
    try:
        data = yaml.safe_load(open("data/metrics.yaml","r",encoding="utf-8"))
    except Exception:
        return []
    cards=[]
    for m in data.get("metrics",[]):
        cards.append(
f"""METRIC CARD
NAME: {m.get('name')}
DEFINITION: {m.get('definition')}
TABLE: {m.get('table')}
FILTERS: {m.get('filters')}
GRAIN: {m.get('grain')}
""".strip())
    return cards

def ingest_schema_cards(schemas: list[str], per_table_samples:int=0) -> int:
    # samples intentionally ignored here (schema-only). You can extend to add tiny sample rows
    targets=[]
    for s in schemas:
        if s not in ALLOWED_SCHEMAS:
            raise ValueError(f"Schema '{s}' not allowed. Update ALLOWED_SCHEMAS in .env")
        targets += [f"{s}.{t}" for t in _list_tables(s)]
    cards = []
    sources = []
    for full in sorted(set(targets)):
        s,t = full.split(".",1)
        cards.append(_schema_card(s,t))
        sources.append(f"schema://{full}")
    # metric cards
    # m_cards = _metric_cards()
    # cards += m_cards
    # sources += [f"metric://{i}" for i,_ in enumerate(m_cards)]
    return add_texts(cards, sources)

if __name__ == "__main__":
    ingest_schema_cards(["public"])
    