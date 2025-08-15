from __future__ import annotations
from typing import List
import google.generativeai as genai
import os, sys
import time
from google.api_core import retry
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.append(ROOT)
from app.config import GOOGLE_API_KEY, GENERATION_MODEL, EMBEDDING_MODEL, API_TIMEOUT_SECONDS, API_MAX_RETRIES, API_RETRY_DELAY

if not GOOGLE_API_KEY:
    raise RuntimeError("Set GOOGLE_API_KEY in .env")

genai.configure(api_key=GOOGLE_API_KEY)

def embed(texts: List[str]) -> List[List[float]]:
    # Gemini supports a single content per call; keep it simple & reliable
    vecs: List[List[float]] = []
    for t in texts:
        for attempt in range(API_MAX_RETRIES):
            try:
                r = genai.embed_content(
                    model=EMBEDDING_MODEL, 
                    content=t
                )
                # shape: {"embedding":{"values":[...]}}
                # print(r)
                vecs.append(r["embedding"])
                break  # Success, exit retry loop
            except Exception as e:
                if attempt == API_MAX_RETRIES - 1:  # Last attempt
                    print(f"Failed to embed text after {API_MAX_RETRIES} attempts: {e}")
                    raise
                print(f"Embedding attempt {attempt + 1} failed: {e}. Retrying in {API_RETRY_DELAY} seconds...")
                time.sleep(API_RETRY_DELAY)
    return vecs

def generate(prompt: str) -> str:
    model = genai.GenerativeModel(GENERATION_MODEL)
    for attempt in range(API_MAX_RETRIES):
        try:
            out = model.generate_content(prompt)
            return (out.text or "").strip()
        except Exception as e:
            if attempt == API_MAX_RETRIES - 1:  # Last attempt
                print(f"Failed to generate content after {API_MAX_RETRIES} attempts: {e}")
                raise
            print(f"Generation attempt {attempt + 1} failed: {e}. Retrying in {API_RETRY_DELAY} seconds...")
            time.sleep(API_RETRY_DELAY)

if __name__ == "__main__":
    print(len(embed(["probe"])[0]))