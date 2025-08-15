from __future__ import annotations
from typing import List, Dict
from pathlib import Path
import json, math
import numpy as np, faiss
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.append(ROOT)
from app.llm.gemini import embed
from app.config import FAISS_INDEX_PATH, FAISS_META_PATH

def _normalize(v: List[float]) -> List[float]:
    n = math.sqrt(sum(x*x for x in v)) or 1.0
    return [x/n for x in v]

def _dim() -> int:
    try:
        return len(embed(["probe"])[0])
    except Exception as e:
        print(f"Error getting embedding dimension: {e}")
        print("This might be due to API timeout or network issues. Check your GOOGLE_API_KEY and internet connection.")
        raise

def _load_index(dim:int) -> faiss.Index:
    p = Path(FAISS_INDEX_PATH)
    if p.exists():
        return faiss.read_index(str(p))
    return faiss.IndexFlatIP(dim)

def _save_index(ix: faiss.Index) -> None:
    Path(FAISS_INDEX_PATH).parent.mkdir(parents=True, exist_ok=True)
    faiss.write_index(ix, str(FAISS_INDEX_PATH))

def _load_meta() -> List[Dict]:
    p = Path(FAISS_META_PATH)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return []

def _save_meta(meta: List[Dict]) -> None:
    Path(FAISS_META_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(FAISS_META_PATH).write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")

def add_texts(cards: List[str], sources: List[str]) -> int:
    dim = _dim()
    ix = _load_index(dim)
    meta = _load_meta()
    added = 0
    for i, text in enumerate(cards):
        try:
            vec = _normalize(embed([text])[0])
            ix.add(np.array([vec], dtype="float32"))
            meta.append({"source": sources[i], "content": text})
            added += 1
        except Exception as e:
            print(f"Failed to embed text {i+1}/{len(cards)} from source '{sources[i]}': {e}")
            print(f"Text preview: {text[:100]}...")
            continue  # Skip this text and continue with others
    _save_index(ix); _save_meta(meta)
    return added

def search(query: str, k: int) -> List[str]:
    meta = _load_meta()
    p = Path(FAISS_INDEX_PATH)
    if not meta or not p.exists():
        return []
    try:
        qv = _normalize(embed([query])[0])
        ix = faiss.read_index(str(p))
        D, I = ix.search(np.array([qv], dtype="float32"), min(k, len(meta)))
        return [meta[idx]["content"] for idx in I[0] if 0 <= idx < len(meta)]
    except Exception as e:
        print(f"Error during search: {e}")
        return []

if __name__ == "__main__":
    print(_dim())