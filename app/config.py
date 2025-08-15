from __future__ import annotations
import os
from dotenv import load_dotenv

load_dotenv()

# LLM
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GENERATION_MODEL = os.getenv("GENERATION_MODEL", "gemini-1.5-flash")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-004")

# API timeout and retry settings
API_TIMEOUT_SECONDS = int(os.getenv("API_TIMEOUT_SECONDS", "30"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "3"))
API_RETRY_DELAY = int(os.getenv("API_RETRY_DELAY", "2"))

# DB (Postgres)
PGHOST=os.getenv("PGHOST","localhost")
PGPORT=int(os.getenv("PGPORT","5432"))
PGDATABASE=os.getenv("PGDATABASE","postgres")
PGUSER=os.getenv("PGUSER","postgres")
PGPASSWORD=os.getenv("PGPASSWORD","postgres")
DSN=f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"

# Safety & retrieval
ALLOWED_SCHEMAS=[s.strip() for s in os.getenv("ALLOWED_SCHEMAS","public").split(",") if s.strip()]
MAX_SQL_ROWS=int(os.getenv("MAX_SQL_ROWS","200"))
MAX_EST_ROWS=int(os.getenv("MAX_EST_ROWS","1000000"))
MAX_EST_COST=float(os.getenv("MAX_EST_COST","1000000"))
TOP_K=int(os.getenv("TOP_K","6"))

# Index paths
FAISS_INDEX_PATH=os.getenv("FAISS_INDEX_PATH","./index/faiss.index")
FAISS_META_PATH=os.getenv("FAISS_META_PATH","./index/meta.json")


if __name__ == "__main__":
    print(GOOGLE_API_KEY)
    print(GENERATION_MODEL)
    print(EMBEDDING_MODEL)
    print(f"API_TIMEOUT_SECONDS: {API_TIMEOUT_SECONDS}")
    print(f"API_MAX_RETRIES: {API_MAX_RETRIES}")
    print(f"API_RETRY_DELAY: {API_RETRY_DELAY}")
    print(PGHOST)
    print(PGPORT)
    print(PGDATABASE)
    print(PGUSER)
    print(PGPASSWORD)
    print(DSN)
    print(TOP_K)
    print(FAISS_INDEX_PATH)
    print(FAISS_META_PATH)