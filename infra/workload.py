import psycopg2
import random
import time

# Connect to PostgreSQL
conn = psycopg2.connect(dbname='demo', user='demo', password='demo', host='localhost')
cur = conn.cursor()

cities = ["Delhi", "Mumbai", "Pune", "Bangalore", "Hyderabad"]
statuses = ["placed", "cancelled", "delivered"]
item_categories = ["electronics", "clothing", "books", "home", "toys"]

# Constants matching seed_db.py
NUM_USERS = 5000
NUM_ITEMS = 10
NUM_ORDERS = 20000

print("🏃 Running workload... Press Ctrl+C to stop.")
try:
    while True:
        q_type = random.choice(["users_city", "users_age", "orders_status", "orders_user", "order_items_join", "items_category"])
        
        if q_type == "users_city":
            query = "SELECT COUNT(*) FROM users WHERE city = %s;"
            param = (random.choice(cities),)
        elif q_type == "users_age":
            age = random.randint(18, 60)
            query = "SELECT COUNT(*) FROM users WHERE age = %s;"
            param = (age,)
        elif q_type == "orders_status":
            query = "SELECT COUNT(*) FROM orders WHERE status = %s;"
            param = (random.choice(statuses),)
        elif q_type == "orders_user":
            user_id = random.randint(1, NUM_USERS)
            query = "SELECT COUNT(*) FROM orders WHERE user_id = %s;"
            param = (user_id,)
        elif q_type == "order_items_join":
            # Join query: get order details with items
            order_id = random.randint(1, NUM_ORDERS)
            query = ("SELECT o.id, o.status, oi.item_id, oi.quantity, oi.price "
                     "FROM orders o JOIN order_items oi ON o.id = oi.order_id "
                     "WHERE o.id = %s;")
            param = (order_id,)
        else:
            # items by category
            category = random.choice(item_categories)
            query = "SELECT COUNT(*) FROM items WHERE category = %s;"
            param = (category,)

        # Execute query without recording
        cur.execute(query, param)
        cur.fetchall()
        conn.commit()

        time.sleep(0.1)

except KeyboardInterrupt:
    print("⏹ Workload stopped.")

finally:
    cur.close()
    conn.close()
