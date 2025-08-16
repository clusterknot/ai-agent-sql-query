from __future__ import annotations
from langgraph.graph import StateGraph, START, END
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from app.graph.state import QAState
from app.tools.metadata_tools import retrieve_metadata,propose_join_path
from app.tools.sql_tools import (
    plan_sql, generate_sql, lint_sql, policy_guard, explain, cost_gate,
    dry_run_sample, execute, summarize_result
)

# ---- Nodes ----

# app/graph/app.py (snippet)
from app.tools.metadata_tools import propose_join_path

def node_join_hint(state):
    plan = state.get("plan") or {}
    tables = plan.get("tables", [])
    if len(tables) >= 2:
        # assume all in the same allowed schema, e.g., "public"
        schema = "public"
        # try pairwise hints for the first two (you can extend for all pairs)
        a, b = tables[0], tables[1]
        hint = propose_join_path(schema, a.split(".")[-1], b.split(".")[-1])
        state.setdefault("evidence", {})["join_hint"] = hint
        # If the plan has no explicit join predicates, inject the hint
        if hint.get("joins") and not plan.get("joins"):
            plan["joins"] = [{"type":"inner","left":j.split("=")[0],"right":j.split("=")[1]} for j in hint["joins"]]
            state["plan"] = plan
    return state


def node_retrieve(state: QAState) -> QAState:
    state["retrieved"] = retrieve_metadata(state["question"])
    return state

def node_plan(state: QAState) -> QAState:
    state["plan"] = plan_sql(state["question"], state["retrieved"])
    return state

def node_generate(state: QAState) -> QAState:
    state["sql"] = generate_sql(state["plan"], "postgres")
    return state

def node_lint_sql(state: QAState) -> QAState:
    state["lint"] = lint_sql(state["sql"])
    return state

def route_after_lint_sql(state: QAState) -> str:
    return "policy" if state["lint"].get("ok") else "generate"

def node_policy(state: QAState) -> QAState:
    state["policy_ok"] = bool(policy_guard(state["sql"]).get("ok"))
    return state

def route_after_policy(state: QAState) -> str:
    return "explain" if state["policy_ok"] else "generate"

def node_explain_sql(state: QAState) -> QAState:
    state["explain"] = explain(state["sql"])
    return state

def node_cost_gate(state: QAState) -> QAState:
    state["gate"] = cost_gate(state["explain"])
    return state

def route_after_cost_gate(state: QAState) -> str:
    return "preview" if state["gate"].get("pass") else "generate"

def node_dry_run_preview(state: QAState) -> QAState:
    state["preview"] = dry_run_sample(state["sql"])
    # If preview failed due to e.g., missing column, loop back to regenerate
    return state

def route_after_dry_run_preview(state: QAState) -> str:
    return "execute" if state["preview"].get("ok") else "generate"

def node_execute(state: QAState) -> QAState:
    state["result"] = execute(state["sql"])
    return state

def node_summarize_answer(state: QAState) -> QAState:
    state["answer"] = summarize_result(state["question"], state["result"], state["retrieved"])
    return state

# ---- Graph ----

g = StateGraph(QAState)
g.add_node("retrieve", node_retrieve)
g.add_node("plan_sql", node_plan)
# g.add_node("join_hint", node_join_hint)
g.add_node("generate", node_generate)
g.add_node("lint_sql", node_lint_sql)
g.add_node("policy", node_policy)
g.add_node("explain_sql", node_explain_sql)
g.add_node("cost_gate", node_cost_gate)
g.add_node("dry_run_preview", node_dry_run_preview)
g.add_node("execute", node_execute)
g.add_node("summarize_answer", node_summarize_answer)

g.add_edge(START, "retrieve")
g.add_edge("retrieve", "plan_sql")
# g.add_edge("plan", "join_hint")
g.add_edge("plan_sql", "generate")
g.add_edge("generate", "lint_sql")
g.add_conditional_edges("lint_sql", route_after_lint_sql, {"policy": "policy", "generate": "generate"})
g.add_conditional_edges("policy", route_after_policy, {"explain": "explain_sql", "generate": "generate"})
g.add_edge("explain_sql", "cost_gate")
g.add_conditional_edges("cost_gate", route_after_cost_gate, {"preview": "dry_run_preview", "generate": "generate"})
g.add_conditional_edges("dry_run_preview", route_after_dry_run_preview, {"execute": "execute", "generate": "generate"})
g.add_edge("execute", "summarize_answer")
g.add_edge("summarize_answer", END)
# g.add_edge("execute", END)
APP = g.compile()

if __name__ == "__main__":
    print(APP.get_graph().draw_ascii())