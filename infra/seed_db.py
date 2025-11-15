import psycopg2
from random import choice, randint

conn = psycopg2.connect(dbname='demo', user='demo', password='demo', host='localhost')
cur = conn.cursor()

# Create users table
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    id serial PRIMARY KEY,
    name text,
    city text,
    age int
);
""")

# Create items table: products that users order (create items before orders so FK can reference it)
cur.execute("""
CREATE TABLE IF NOT EXISTS items (
    id serial PRIMARY KEY,
    name text,
    category text,
    price numeric
);
""")

# Create orders table with foreign keys to users and items
cur.execute("""
CREATE TABLE IF NOT EXISTS orders (
    id serial PRIMARY KEY,
    user_id int REFERENCES users(id),
    item_id int REFERENCES items(id),
    amount numeric,
    status text
);
""")

# Insert synthetic data
cities = ["Delhi", "Mumbai", "Pune", "Bangalore", "Hyderabad"]
statuses = ["pending", "shipped", "delivered", "cancelled"]

# Insert users
NUM_USERS = 10000
for i in range(1, NUM_USERS + 1):
    cur.execute("INSERT INTO users (name, city, age) VALUES (%s, %s, %s)",
                (f"name{i}", choice(cities), randint(18, 60)))

# Insert a simple catalog of items
item_categories = ["electronics", "clothing", "books", "home", "toys"]
NUM_ITEMS = 200
for i in range(1, NUM_ITEMS + 1):
    cur.execute("INSERT INTO items (name, category, price) VALUES (%s, %s, %s)",
                (f"item{i}", choice(item_categories), randint(50, 5000)))

# Insert orders and associate an item with each order (user_id and item_id are correlated randomly)
NUM_ORDERS = 20000
# Insert orders and associate an item with each order correlated to the user_id
for i in range(1, NUM_ORDERS + 1):
    user_id = randint(1, NUM_USERS)
    # correlate item choice with user_id so the same user tends to order the same items
    item_id = ((user_id - 1) % NUM_ITEMS) + 1
    cur.execute(
        "INSERT INTO orders (user_id, item_id, amount, status) VALUES (%s, %s, %s, %s)",
        (user_id, item_id, randint(100, 10000), choice(statuses))
    )

conn.commit()
cur.close()
conn.close()
print("Database seeded ✅")
