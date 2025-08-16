from __future__ import annotations
from typing import Any, Dict, List, Optional
import json, re
import sqlglot 
from sqlalchemy import text, create_engine
from sqlglot import parse_one
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.append(ROOT)
from app.llm.gemini import generate
from app.config import DSN, ALLOWED_SCHEMAS
from app.db.pg import run_sql, explain_sql
from app.config import MAX_SQL_ROWS, MAX_EST_ROWS, MAX_EST_COST, ALLOWED_SCHEMAS

FORBIDDEN = {"INSERT","UPDATE","DELETE","MERGE","CREATE","ALTER","DROP","TRUNCATE","GRANT","REVOKE"}
SCHEMA_QUAL = re.compile(r"\b([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\b")

# --------- PLANNING & GENERATION ---------
def load_metadata_json():
    eng = create_engine(DSN, pool_pre_ping=True, future=True)
    q = text("""
      WITH
  params AS (
    SELECT 'public'::text AS schema_name
  ),

  cols AS (
    SELECT
      c.table_name,
      c.column_name,
      c.data_type,
      c.ordinal_position
    FROM information_schema.columns c
    JOIN params p ON c.table_schema = p.schema_name
    ORDER BY c.table_name, c.ordinal_position
  ),

  pks AS (
    SELECT
      kcu.table_name,
      kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema   = kcu.table_schema
     AND tc.table_name     = kcu.table_name
    JOIN params p ON tc.table_schema = p.schema_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
  ),

  fks AS (
    SELECT
      tc.constraint_name,
      kcu.table_name        AS child_table,
      kcu.column_name       AS child_column,
      ccu.table_name        AS parent_table,
      ccu.column_name       AS parent_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema   = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = tc.constraint_name
     AND ccu.table_schema    = tc.table_schema
    JOIN params p ON tc.table_schema = p.schema_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
  )

SELECT jsonb_pretty(
  jsonb_agg(
    jsonb_build_object(
      'table', t.table_name,
      'columns', t.columns
    )
    ORDER BY t.table_name
  )
) AS schema_json
FROM (
  SELECT
    c.table_name,
    jsonb_agg(
      jsonb_build_object(
        'name', c.column_name,
        'data_type', c.data_type,
        'is_primary_key', (pk.column_name IS NOT NULL),
        'is_foreign_key',
           COALESCE(jsonb_array_length(fk_refs.refs) > 0, FALSE),
        'references', COALESCE(fk_refs.refs, '[]'::jsonb)
      )
      ORDER BY c.ordinal_position
    ) AS columns
  FROM cols c
  LEFT JOIN pks pk
    ON pk.table_name  = c.table_name
   AND pk.column_name = c.column_name
  LEFT JOIN LATERAL (
    SELECT jsonb_agg(
             jsonb_build_object(
               'table', f.parent_table,
               'column', f.parent_column,
               'constraint', f.constraint_name
             )
             ORDER BY f.constraint_name
           ) AS refs
    FROM fks f
    WHERE f.child_table  = c.table_name
      AND f.child_column = c.column_name
  ) fk_refs ON TRUE
  GROUP BY c.table_name
) t;
    """)
    with eng.connect() as c:
        val = c.execute(q).scalar_one()
        if isinstance(val, str):
            val = json.loads(val)
        return val  # Python dict/list
        


def plan_sql(nl_question: str, retrieved_context: List[str]) -> Dict[str, Any]:
    """LLM makes a JSON plan of tables/joins/filters/etc based on schema+metric cards."""
    ctx = load_metadata_json() #"\n---\n".join(retrieved_context[:5])
    # ctx = "\n---\n".join(retrieved_context)
    prompt = f"""
You are a data analyst. From the Context, plan a SQL query in JSON. Only return JSON.
JSON fields: tables, joins (list of objects {{left,right,type}}), select, filters, group_by, order_by.
Context:
{ctx}

Question: {nl_question}
Return JSON only.
"""
    out = generate(prompt)
    # try to extract JSON
    m = re.search(r"\{.*\}", out, re.S)
    js = m.group(0) if m else "{}"
    try:
        return json.loads(js)
    except Exception:
        return {"tables": [], "joins": [], "select": [], "filters": [], "group_by": [], "order_by": [], "metric_refs": []}

def generate_sql(plan: Dict[str, Any], dialect: str = "postgres") -> str:
    """LLM turns plan into SQL. Enforce LIMIT if absent."""
    prompt = f"""
Write a {dialect} SQL from this plan. Use schema-qualified tables (include schema), safe to run, NO comments.
Ensure a LIMIT {MAX_SQL_ROWS} at the end if not logically harmful.

PLAN:
{json.dumps(plan, ensure_ascii=False, indent=2)}

SQL only:
"""
    sql = generate(prompt)
    # enforce LIMIT if missing
    if re.search(r"\bLIMIT\s+\d+", sql, re.I) is None:
        sql += f"\nLIMIT {MAX_SQL_ROWS}"
    return sql.strip()

# --------- SAFETY & VALIDATION ---------

def lint_sql(sql: str, dialect: str = "postgres") -> Dict[str, Any]:
    up = sql.upper()
    if any(k in up for k in FORBIDDEN):
        return {"ok": False, "errors": ["Statement appears to modify DDL/DML"], "warnings": []}
    # must be SELECT/WITH
    if not (up.strip().startswith("SELECT") or up.strip().startswith("WITH")):
        return {"ok": False, "errors": ["Must start with SELECT or WITH"], "warnings": []}
    # schema-qualification: at least one schema.table must appear & all FROM targets should be qualified
    if not SCHEMA_QUAL.search(sql):
        return {"ok": False, "errors": ["Use schema-qualified tables (e.g., public.orders)"], "warnings": []}
    # AST parse
    try:
        parse_one(sql, read=dialect)
    except Exception as e:
        return {"ok": False, "errors": [f"Parse error: {e}"], "warnings": []}
    # LIMIT cap
    m = re.search(r"\bLIMIT\s+(\d+)", sql, re.I)
    if m and int(m.group(1)) > MAX_SQL_ROWS:
        return {"ok": False, "errors": [f"LIMIT exceeds {MAX_SQL_ROWS}"], "warnings": []}
    # deny schemas not allowed
    bad=[]
    for sch,_tbl in SCHEMA_QUAL.findall(sql):
        if sch not in ALLOWED_SCHEMAS:
            bad.append(sch)
    if bad:
        return {"ok": False, "errors": [f"Unallowed schemas: {sorted(set(bad))}"], "warnings": []}
    return {"ok": True, "errors": [], "warnings": []}

def policy_guard(sql: str) -> Dict[str, Any]:
    """Hook to enforce org policies (e.g., PII denylist/tenant filters). Here: stub pass."""
    # You can parse columns with sqlglot and compare to denylist.
    return {"ok": True, "reason": "pass"}

def explain(sql: str) -> Dict[str, Any]:
    return explain_sql(sql)

def cost_gate(plan: Dict[str, Any]) -> Dict[str, Any]:
    est_rows = int(plan.get("est_rows", 0))
    est_cost = float(plan.get("est_cost", 0))
    if est_rows > MAX_EST_ROWS or est_cost > MAX_EST_COST:
        return {
            "pass": False,
            "reason": f"Too expensive: rows={est_rows} cost={est_cost}",
            "suggested_patch": f"Add a date/tenant filter and LIMIT {MAX_SQL_ROWS}"
        }
    return {"pass": True, "reason": "within thresholds", "suggested_patch": None}

def dry_run_sample(sql: str, limit: int = 100) -> Dict[str, Any]:
    # ensure small limit probe
    if re.search(r"\bLIMIT\s+\d+", sql, re.I):
        probe = re.sub(r"\bLIMIT\s+\d+", f"LIMIT {min(limit,MAX_SQL_ROWS)}", sql, flags=re.I)
    else:
        probe = f"{sql.rstrip(';')}\nLIMIT {min(limit,MAX_SQL_ROWS)}"
    try:
        prev = run_sql(probe, limit_timeout_ms=8000)
        return {"ok": True, "preview": prev}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def execute(sql: str) -> Dict[str, Any]:
    return run_sql(sql, limit_timeout_ms=15000)
    # return generate(sql)

# --------- SUMMARIZATION ---------

def summarize_result(question: str, result: Dict[str, Any], context: List[str]) -> str:
    cols = result.get("columns", [])
    rows = result.get("rows", [])[:10]
    ctx = "\n---\n".join(context[:6])
    prompt = f"""
You are a precise analyst. Using the Result and Context, answer the Question in 4-8 sentences.
Include key numbers and how they were computed. If result is empty, say what to change (filters, date range).

Question: {question}

Result columns: {cols}
Result sample (first {len(rows)} rows): {rows}

Context:
{ctx}
"""
    return generate(prompt)


if __name__ == '__main__':
    print(load_metadata_json())
