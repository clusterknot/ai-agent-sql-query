from __future__ import annotations
from typing import Dict, Any, Optional
_memory: Dict[str, Dict[str, Any]] = {}

def cache_get(key: str) -> Optional[Dict[str, Any]]:
    return _memory.get(key)

def cache_put(key: str, payload: Dict[str, Any]) -> None:
    _memory[key] = payload
