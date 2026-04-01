#!/usr/bin/env python
import psycopg2

DATABASE_URL = "postgresql://demo:demo@localhost:5432/demo"

print("Testing database connection...")
try:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("SELECT 1")
    result = cur.fetchone()
    print(f"Connected! Result: {result}")
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
