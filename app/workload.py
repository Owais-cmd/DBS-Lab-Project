import psycopg2
import random
import time
import csv

# Connect to PostgreSQL
conn = psycopg2.connect(dbname='demo', user='demo', password='demo', host='localhost')
cur = conn.cursor()

cities = ["Delhi", "Mumbai", "Pune", "Chennai", "Kolkata"]
statuses = ["pending", "delivered", "cancelled"]
item_categories = ["electronics", "clothing", "books", "home", "toys"]

# Open CSV file to log queries
csv_file = open("../data/query_log.csv", "a", newline="")
csv_writer = csv.writer(csv_file)
csv_writer.writerow(["timestamp", "query", "params", "execution_time_ms", "rows_returned"])

print("🏃 Running workload... Press Ctrl+C to stop.")
try:
    while True:
        q_type = random.choice(["users", "orders_status", "orders_item", "orders_user_item", "join_items"])
        if q_type == "users":
            query = "SELECT COUNT(*) FROM users WHERE city = %s;"
            param = (random.choice(cities),)
        elif q_type == "orders_status":
            query = "SELECT COUNT(*) FROM orders WHERE status = %s;"
            param = (random.choice(statuses),)
        elif q_type == "orders_item":
            # pick a random item id (1..NUM_ITEMS) if present, otherwise random small id
            item_id = random.randint(1, 200)
            query = "SELECT COUNT(*) FROM orders WHERE item_id = %s;"
            param = (item_id,)
        elif q_type == "orders_user_item":
            user_id = random.randint(1, 10000)
            # correlate item to user to mimic seeder behavior
            item_id = ((user_id - 1) % 200) + 1
            query = "SELECT COUNT(*) FROM orders WHERE user_id = %s AND item_id = %s;"
            param = (user_id, item_id)
        else:
            # join: get top item in a category by order count
            category = random.choice(item_categories)
            query = ("SELECT i.id, i.name, COUNT(o.id) as orders_count "
                     "FROM items i JOIN orders o ON o.item_id = i.id "
                     "WHERE i.category = %s GROUP BY i.id, i.name ORDER BY orders_count DESC LIMIT 1;")
            param = (category,)

        # Measure execution time
        start_time = time.time()
        cur.execute(query, param)
        rows = cur.fetchone()
        conn.commit()
        end_time = time.time()

        execution_time_ms = (end_time - start_time) * 1000  # convert to milliseconds

        # Log to CSV
        csv_writer.writerow([
            time.strftime("%Y-%m-%d %H:%M:%S"),
            cur.mogrify(query, param).decode(),  # full query with parameters
            param,
            round(execution_time_ms, 3),
            rows[0] if rows else 0
        ])
        csv_file.flush()  # make sure data is written immediately

        time.sleep(0.1)

except KeyboardInterrupt:
    print("⏹ Workload stopped.")

finally:
    cur.close()
    conn.close()
    csv_file.close()
