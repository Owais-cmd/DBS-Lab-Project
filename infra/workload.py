# infra/workload_runner.py
import time
import random
import psycopg2

DSN = "dbname=demo user=demo password=demo host=localhost port=5432"

QUERIES = [
    "SELECT * FROM users WHERE city = 'Delhi' LIMIT 50;",
    "SELECT * FROM users WHERE age > 30 AND city = 'Mumbai' LIMIT 50;",
    "SELECT count(*) FROM orders WHERE user_id = %s;",
    "SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id WHERE u.city = 'Pune' LIMIT 50;",
]

def run_loop():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    while True:
        q = random.choice(QUERIES)
        if '%s' in q:
            uid = random.randint(1, 5000)
            cur.execute(q, (uid,))
        else:
            cur.execute(q)
        # fetch a small result to ensure the query actually runs
        try:
            _ = cur.fetchmany(10)
        except:
            pass
        time.sleep(0.2)  # adjust to generate load
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_loop()
