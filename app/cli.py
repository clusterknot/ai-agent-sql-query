from __future__ import annotations
import sys
from app.ingestion.schema_ingest import ingest_schema_cards
from app.graph.app import APP
from app.graph.state import QAState

USAGE = """Usage:
  python -m app.cli ingest-schema --schemas public[,app] [--samples 3]
  python -m app.cli ask "What was monthly revenue in 2024 by product line?"
"""

def main(argv:list[str]) -> int:
    if len(argv) < 2:
        print(USAGE); return 1
    cmd = argv[1]

    if cmd == "ingest-schema":
        schemas=[]; samples=0
        args = argv[2:]
        for i,a in enumerate(args):
            if a=="--schemas" and i+1<len(args):
                schemas=[s.strip() for s in args[i+1].split(",") if s.strip()]
            if a=="--samples" and i+1<len(args):
                samples=int(args[i+1])
        n = ingest_schema_cards(schemas=schemas, per_table_samples=samples)
        print(f"Ingested {n} schema/metric cards into FAISS.")
        return 0

    if cmd == "ask":
        q = " ".join(argv[2:])
        state: QAState = {
            "question": q,
            "retrieved": [],
            "plan": None,
            "sql": None,
            "lint": None,
            "policy_ok": False,
            "explain": None,
            "gate": None,
            "preview": None,
            "result": None,
            "answer": None,
            "evidence": {},
        }
        out = APP.invoke(state)
        print(out.get("answer",""))
        return 0

    print(USAGE); return 1

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
