from __future__ import annotations
from typing import List, Dict, Any, Set, Deque
from collections import deque, defaultdict
from sqlalchemy import text, create_engine
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.append(ROOT)
from app.config import DSN, ALLOWED_SCHEMAS, TOP_K
from app.vector.faiss_store import search as vec_search
import yaml

# 1) retrieve_metadata: vector search over schema/metric cards
def retrieve_metadata(query: str, k: int = TOP_K) -> List[str]:
    return vec_search(query, k)

# 2) get_schema_objects: live snapshot (tables, columns, pk/fk)
def get_schema_objects(schema: str) -> Dict[str, Any]:
    if schema not in ALLOWED_SCHEMAS:
        return {"error": f"Schema '{schema}' not allowed"}
    eng = create_engine(DSN, pool_pre_ping=True, future=True)
    out: Dict[str, Any] = {"schema": schema, "tables": {}}
    with eng.connect() as c:
        tables = [r[0] for r in c.execute(text("""
          SELECT table_name FROM information_schema.tables
          WHERE table_schema=:s AND table_type='BASE TABLE' ORDER BY table_name
        """), {"s": schema})]
        for t in tables:
            cols = [dict(zip(['column_name','data_type','is_nullable','default'], r)) for r in c.execute(text("""
              SELECT column_name, data_type FROM information_schema.columns
              WHERE table_schema=:s AND table_name=:t ORDER BY ordinal_position
            """), {"s": schema, "t": t})]
            out["tables"][t] = {"columns": cols}

        fks = [dict(r) for r in c.execute(text("""
          SELECT tc.table_name AS table, kcu.column_name AS column,
                 ccu.table_name AS ref_table, ccu.column_name AS ref_column
          FROM information_schema.table_constraints AS tc
          JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
          JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
          WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_schema=:s
        """), {"s": schema})]
        out["foreign_keys"] = fks
    return out

# 3) propose_join_path: BFS on FK graph between two tables in same schema
def propose_join_path(schema: str, source_table: str, target_table: str) -> Dict[str, Any]:
    meta = get_schema_objects(schema)
    if "error" in meta: return meta
    edges = defaultdict(list)
    for fk in meta.get("foreign_keys", []):
        a = fk["table"]; b = fk["ref_table"]
        edges[a].append({"to": b, "via": f"{fk['table']}.{fk['column']}={fk['ref_table']}.{fk['ref_column']}"})
        # allow reverse traversal as well
        edges[b].append({"to": a, "via": f"{fk['ref_table']}.{fk['ref_column']}={fk['table']}.{fk['column']}"})
    # BFS
    q: Deque[tuple[str, list[str], list[str]]] = deque()
    q.append((source_table, [source_table], []))
    seen: Set[str] = set([source_table])
    while q:
        node, path, joins = q.popleft()
        if node == target_table:
            return {"path": path, "joins": joins}
        for e in edges[node]:
            nxt = e["to"]
            if nxt in seen: continue
            seen.add(nxt)
            q.append((nxt, path+[nxt], joins+[e["via"]]))
    return {"error": f"No FK path from {source_table} to {target_table} in schema {schema}"}

# 4) metric_lookup (optional; from data/metrics.yaml)
# def metric_lookup(name: str) -> Dict[str, Any]:
#     try:
#         data = yaml.safe_load(open("data/metrics.yaml","r",encoding="utf-8")) or {}
#         for m in data.get("metrics", []):
#             if m.get("name","").lower() == name.lower():
#                 return m
#     except Exception:
#         pass
#     return {"error": f"Metric '{name}' not found"}
