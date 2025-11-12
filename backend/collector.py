# app/collector.py
import os
import time
import pandas as pd
import psycopg2
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

DATABASE_URL = "postgresql://demo:demo@localhost:5432/demo"
data_dir = os.path.join(os.path.dirname(__file__), "../data")
os.makedirs(data_dir, exist_ok=True)  # create if not exists
OUT_CSV = os.path.join(data_dir, f"pg_stats.csv") # mapped in docker-compose

def get_conn():
    # psycopg2 accepts a URL or keywords
    return psycopg2.connect(DATABASE_URL)

def collect():
    try:
     print("[collector] collecting pg_stat_statements")
     conn = get_conn()
     cur = conn.cursor()
     cur.execute("""
     SELECT query, calls, total_exec_time, mean_exec_time, rows
     FROM pg_stat_statements    
     WHERE query NOT ILIKE '%pg_stat_statements%'
     ORDER BY total_exec_time DESC
     LIMIT 1000;
     """)
     print(f"[collector] executed query")
     rows = cur.fetchall()
     df = pd.DataFrame(rows, columns=['query','calls','total_exec_time','mean_exec_time','rows'])
     os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
     df.to_csv(OUT_CSV, index=False)
     print(f"[collector] wrote {len(df)} rows to {OUT_CSV}")
     cur.close()
     conn.close()
    except Exception as e:
     print(f"[collector] error: {e}")
     return False
    return True

if __name__ == "__main__":
    # single-run if env var set, otherwise schedule
    run_once = os.getenv("RUN_ONCE", "false").lower() == "true"
    if run_once:
        collect()
    else:
        sched = BackgroundScheduler()
        sched.add_job(collect, 'interval', minutes=1, next_run_time=datetime.now())
        sched.start()
        print("[collector] scheduler started, collecting every 1 minute. Press Ctrl+C to exit.")
        try:
            while True:
                time.sleep(0.05)
        except KeyboardInterrupt:
            print("Shutting down scheduler")
            sched.shutdown()
