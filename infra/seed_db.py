import psycopg2
from random import choice, randint
import hashlib
import random

conn = psycopg2.connect(dbname='demo', user='demo', password='demo', host='localhost')
cur = conn.cursor()

# Drop existing tables if they exist (for clean reseeding)
#cur.execute("DROP TABLE IF EXISTS order_items CASCADE;")
#cur.execute("DROP TABLE IF EXISTS orders CASCADE;")
#cur.execute("DROP TABLE IF EXISTS items CASCADE;")
#cur.execute("DROP TABLE IF EXISTS users CASCADE;")

# Create users table
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    id serial PRIMARY KEY,
    email text UNIQUE NOT NULL,
    hashed_password text NOT NULL,
    name text NOT NULL,
    is_admin boolean DEFAULT false,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    city text NOT NULL
);
""")

# Create items table
cur.execute("""
CREATE TABLE IF NOT EXISTS items (
    id serial PRIMARY KEY,
    name text NOT NULL,
    description text,
    category text,
    price numeric DEFAULT 0,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);
""")

# Create orders table
cur.execute("""
CREATE TABLE IF NOT EXISTS orders (
    id serial PRIMARY KEY,
    user_id int NOT NULL REFERENCES users(id),
    status text DEFAULT 'cart',
    total_amount numeric DEFAULT 0,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);
""")

# Create order_items table (junction table)
cur.execute("""
CREATE TABLE IF NOT EXISTS order_items (
    id serial PRIMARY KEY,
    order_id int NOT NULL REFERENCES orders(id),
    item_id int NOT NULL REFERENCES items(id),
    quantity int DEFAULT 1,
    price numeric
);
""")

# Insert synthetic data
cities = ["Delhi", "Mumbai", "Pune", "Bangalore", "Hyderabad"]
statuses = ["placed", "cancelled", "delivered"]
item_categories = ["electronics", "clothing", "books", "home", "toys"]

# Insert users
NUM_USERS = 5000
for i in range(1, NUM_USERS + 1):
    email = f"user{i}@example.com"
    name = f"User {i}"
    # Simple hash for demo purposes
    hashed_password = hashlib.sha256(f"password{i}".encode()).hexdigest()
    cur.execute(
        "INSERT INTO users (email, hashed_password, name, city) VALUES (%s, %s, %s, %s)",
        (email, hashed_password, name, choice(cities))
    )

# Insert items
NUM_ITEMS = 200
for i in range(1, NUM_ITEMS + 1):
    cur.execute(
        "INSERT INTO items (name, description, category, price) VALUES (%s, %s, %s, %s)",
        (f"Item {i}", f"Description for item {i}", choice(item_categories), randint(50, 5000))
    )

# Insert orders and order_items
NUM_ORDERS = 20000
for i in range(1, NUM_ORDERS + 1):
    user_id = randint(1, NUM_USERS)
    order_status = choice(statuses)
    
    # Insert order with calculated total
    cur.execute(
        "INSERT INTO orders (user_id, status, total_amount) VALUES (%s, %s, %s) RETURNING id",
        (user_id, order_status, 0)  # Will update total_amount after adding items
    )
    order_id = cur.fetchone()[0]
    
    # Insert 1-3 unique items per order
    x = []
    num_items_in_order = randint(1, 3)
    total_amount = 0
    
    for _ in range(num_items_in_order):
        # Ensure unique items in each order
        item_id = randint(1, NUM_ITEMS)
        while item_id in x:
            item_id = randint(1, NUM_ITEMS)
        x.append(item_id)
        
        quantity = randint(1, 5)
        price = randint(50, 5000)
        total_amount += price * quantity
        
        cur.execute(
            "INSERT INTO order_items (order_id, item_id, quantity, price) VALUES (%s, %s, %s, %s)",
            (order_id, item_id, quantity, price)
        )
    
    # Update order total_amount
    cur.execute(
        "UPDATE orders SET total_amount = %s WHERE id = %s",
        (total_amount, order_id)
    )

conn.commit()
cur.close()
conn.close()
print("Database seeded ✅")