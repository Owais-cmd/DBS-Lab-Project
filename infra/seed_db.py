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
    is_admin boolean DEFAULT false,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    city text NOT NULL,
    age int NOT NULL
);
""")

# Create items table
cur.execute("""
CREATE TABLE IF NOT EXISTS items (
    id serial PRIMARY KEY,
    name text NOT NULL,
    category text,
    price numeric DEFAULT 0
);
""")

# Create orders table
cur.execute("""
CREATE TABLE IF NOT EXISTS orders (
    id serial PRIMARY KEY,
    user_id int NOT NULL REFERENCES users(id),
    status text DEFAULT 'placed',
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
    # Simple hash for demo purposes
    hashed_password = hashlib.sha256(f"password{i}".encode()).hexdigest()
    cur.execute(
        "INSERT INTO users (email, hashed_password, city, age) VALUES (%s, %s, %s, %s)",
        (email, hashed_password, choice(cities), randint(18, 60))
    )

# Insert items
NUM_ITEMS = 10
for i in range(1, NUM_ITEMS + 1):
    x=random.randint(1,10)
    cur.execute(
        "INSERT INTO items (name, category, price) VALUES (%s, %s, %s)",
        (f"item{x}", choice(item_categories), randint(50, 5000))
    )

# Insert orders and order_items
NUM_ORDERS = 20000
for i in range(1, NUM_ORDERS + 1):
    user_id = randint(1, NUM_USERS)
    order_status = choice(statuses)
    
    # Insert order
    cur.execute(
        "INSERT INTO orders (user_id, status) VALUES (%s, %s) RETURNING id",
        (user_id, order_status)
    )
    order_id = cur.fetchone()[0]
    x=[]
    # Insert 1-3 items per order
    num_items_in_order = randint(1, 3)
    for _ in range(num_items_in_order):
        item_id = randint(1, NUM_ITEMS)
        while item_id in x:
            item_id=randint(1,NUM_ITEMS)
        x.append(item_id)
        quantity = randint(1, 5)
        price = randint(50, 5000)
        
        cur.execute(
            "INSERT INTO order_items (order_id, item_id, quantity, price) VALUES (%s, %s, %s, %s)",
            (order_id, item_id, quantity, price)
        )

conn.commit()
cur.close()
conn.close()
print("Database seeded ✅")