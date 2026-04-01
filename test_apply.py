#!/usr/bin/env python
import requests
import json

BASE_URL = "http://127.0.0.1:8000"

# Step 1: Login to get token
login_data = {
    "email": "admin@test.com",
    "password": "admin123"
}

print("1. Attempting login...")
try:
    resp = requests.post(f"{BASE_URL}/login", json=login_data)
    print(f"   Status: {resp.status_code}")
    if resp.status_code == 200:
        token_data = resp.json()
        token = token_data.get("access_token")
        print(f"   Token: {token[:50]}...")
        
        # Step 2: Test apply endpoint
        print("\n2. Attempting to apply index...")
        apply_data = {
            "table": "products",
            "column": "name",
            "force": True,
            "user": "admin@test.com"
        }
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        resp = requests.post(f"{BASE_URL}/apply", json=apply_data, headers=headers)
        print(f"   Status: {resp.status_code}")
        print(f"   Response: {resp.text}")
    else:
        print(f"   Error: {resp.text}")
except Exception as e:
    print(f"   Error: {e}")
