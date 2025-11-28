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
NUM_ITEMS = 200
NUM_ORDERS = 20000

print("🏃 Running workload... Press Ctrl+C to stop.")
try:
    while True:
        q_type = random.choice([
            "users_city", 
            "orders_status", 
            "orders_user", 
            "order_items_join", 
            "items_category",
            "expensive_orders",
            "user_total_spent"
        ])
        
        if q_type == "users_city":
            query = "SELECT COUNT(*) FROM users WHERE city = %s;"
            param = (random.choice(cities),)
        elif q_type == "orders_status":
            query = "SELECT COUNT(*) FROM orders WHERE status = %s;"
            param = (random.choice(statuses),)
        elif q_type == "orders_user":
            user_id = random.randint(1, NUM_USERS)
            query = "SELECT * FROM orders WHERE user_id = %s ORDER BY created_at DESC LIMIT 5;"
            param = (user_id,)
        elif q_type == "order_items_join":
            # Join query: get order details with items
            order_id = random.randint(1, NUM_ORDERS)
            query = ("SELECT o.id, o.status, o.total_amount, oi.item_id, oi.quantity, oi.price "
                     "FROM orders o JOIN order_items oi ON o.id = oi.order_id "
                     "WHERE o.id = %s;")
            param = (order_id,)
        elif q_type == "items_category":
            # items by category
            category = random.choice(item_categories)
            query = "SELECT * FROM items WHERE category = %s LIMIT 20;"
            param = (category,)
        elif q_type == "expensive_orders":
            # Get expensive orders
            query = "SELECT * FROM orders WHERE status != 'cart' ORDER BY total_amount DESC LIMIT 10;"
            param = ()
        else:
            # user_total_spent
            user_id = random.randint(1, NUM_USERS)
            query = "SELECT SUM(total_amount) FROM orders WHERE user_id = %s AND status = 'placed';"
            param = (user_id,)

        # Execute query without recording
        if param:
            cur.execute(query, param)
        else:
            cur.execute(query)
        cur.fetchall()
        conn.commit()

        time.sleep(0.1)

except KeyboardInterrupt:
    print("⏹ Workload stopped.")

finally:
    cur.close()
    conn.close()
