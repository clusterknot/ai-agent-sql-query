#!/usr/bin/env python3
"""
Test script to verify embedding functionality with timeout and retry logic
"""

import sys
import os

# Add the project root to the path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from app.llm.gemini import embed
from app.config import API_TIMEOUT_SECONDS, API_MAX_RETRIES, API_RETRY_DELAY

def test_embedding():
    print("Testing embedding functionality...")
    print(f"Timeout: {API_TIMEOUT_SECONDS} seconds")
    print(f"Max retries: {API_MAX_RETRIES}")
    print(f"Retry delay: {API_RETRY_DELAY} seconds")
    print()
    
    try:
        # Test with a simple probe text
        test_texts = ["This is a test sentence for embedding."]
        print("Getting embeddings...")
        embeddings = embed(test_texts)
        
        print(f"Success! Got {len(embeddings)} embeddings")
        print(f"Embedding dimension: {len(embeddings[0])}")
        print(f"First few values: {embeddings[0][:5]}...")
        
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    success = test_embedding()
    sys.exit(0 if success else 1)
