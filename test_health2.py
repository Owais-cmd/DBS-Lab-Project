#!/usr/bin/env python
import requests

BASE_URL = "http://127.0.0.1:8000"

# Step 1: Check health
print("Testing health endpoint...")
try:
    resp = requests.get(f"{BASE_URL}/health", timeout=5)
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.json()}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
