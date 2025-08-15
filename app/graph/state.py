from __future__ import annotations
from typing import List, Optional, Dict, Any
from typing_extensions import TypedDict

class QAState(TypedDict, total=False):
    question: str
    retrieved: List[str]
    plan: Dict[str, Any]
    sql: str
    lint: Dict[str, Any]
    policy_ok: bool
    explain: Dict[str, Any]
    gate: Dict[str, Any]
    preview: Dict[str, Any]
    result: Dict[str, Any]
    answer: str
    evidence: Dict[str, Any]
