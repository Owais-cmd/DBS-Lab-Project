# Adaptive Ordering System Backend

A FastAPI backend with PostgreSQL for an e-commerce ordering system with adaptive database indexing capabilities.

## Features

✅ **Complete Authentication System**
- JWT token-based auth stored in HttpOnly cookies
- Bcrypt password hashing
- User signup, login, logout
- Role-based access control (admin/user)

✅ **Full E-commerce API**
- User management
- Item catalog with search and filtering
- Shopping cart system
- Order management (place, cancel, track)
- Admin order operations

✅ **Advanced Metrics & Analytics**
- Most ordered items
- High-value orders tracking
- Category statistics
- Top customers by spending
- User purchase statistics

✅ **Database Index Management**
- Dynamic index creation
- Index listing and auditing
- Query plan comparison (with/without indexes)
- Support for btree, hash, gin, gist indexes

## Tech Stack

- **Framework**: FastAPI
- **Database**: PostgreSQL
- **ORM**: SQLAlchemy (sync)
- **Migrations**: Alembic
- **Auth**: JWT (python-jose) + bcrypt (passlib)
- **Validation**: Pydantic v2

## Project Structure

```
backend/app/
├── main.py              # FastAPI app entry point
├── config.py            # Settings and environment variables
├── database.py          # Database connection and session
├── models.py            # SQLAlchemy models
├── schemas/             # Pydantic schemas
│   ├── auth.py
│   ├── user.py
│   ├── item.py
│   └── order.py
├── routers/             # API endpoints
│   ├── auth.py          # /auth/*
│   ├── users.py         # /users/*
│   ├── items.py         # /items/*
│   ├── orders.py        # /cart/*, /orders/*
│   ├── metrics.py       # /metrics/*
│   └── indexes.py       # /indexes/*
├── crud/                # Database operations
│   ├── auth.py
│   ├── users.py
│   ├── items.py
│   └── orders.py
├── services/            # Business logic
│   ├── index_manager.py
│   └── metrics_manager.py
└── utils/               # Utilities
    └── security.py      # JWT and password functions
```

## Database Models

### User
- `id`: Primary key
- `email`: Unique email address
- `hashed_password`: Bcrypt hashed password
- `name`: User's full name
- `is_admin`: Admin flag
- `city`: User's city
- `created_at`: Account creation timestamp

### Item
- `id`: Primary key
- `name`: Unique item name
- `description`: Item description
- `price`: Item price (numeric)
- `category`: Item category
- `created_at`: Item creation timestamp

### Order
- `id`: Primary key
- `user_id`: Foreign key to User
- `status`: Order status (cart, placed, cancelled, delivered)
- `total_amount`: Order total (computed)
- `created_at`: Order creation timestamp

### OrderItem (Junction Table)
- `id`: Primary key
- `order_id`: Foreign key to Order
- `item_id`: Foreign key to Item
- `quantity`: Item quantity
- `price`: Item price at purchase time

## API Endpoints

### Authentication (`/auth`)
- `POST /auth/signup` - Create new user account
- `POST /auth/login` - Login (sets HttpOnly cookie)
- `POST /auth/logout` - Logout (deletes cookie)

### Users (`/users`)
- `GET /users/me` - Get current user profile
- `GET /users/me/orders?limit=5` - Get user's order history

### Items (`/items`)
- `GET /items?search=&category=&limit=100&offset=0` - List items
- `GET /items/{id}` - Get specific item
- `POST /items` - Create item (admin only)
- `PATCH /items/{id}` - Update item (admin only)
- `DELETE /items/{id}` - Delete item (admin only)

### Orders & Cart
- `POST /cart/add` - Add item to cart
- `POST /cart/remove` - Remove item from cart
- `POST /orders/place` - Convert cart to order
- `DELETE /orders/{id}` - Cancel order
- `PATCH /orders/{id}/delivered` - Mark as delivered (admin only)

### Metrics (`/metrics`) - Admin Only
- `GET /metrics/most-ordered?limit=10` - Most ordered items
- `GET /metrics/expensive-orders?limit=10` - High-value orders
- `GET /metrics/category-stats` - Sales by category
- `GET /metrics/top-customers?limit=10` - Top spending customers
- `GET /metrics/user/{user_id}` - User statistics

### Indexes (`/indexes`) - Admin Only
- `POST /indexes/apply` - Create an index
- `GET /indexes/list` - List all indexes
- `POST /indexes/comparison/{index_name}` - Compare query plans
- `DELETE /indexes/drop/{index_name}` - Drop an index

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Copy `.env.example` to `.env` and update:

```bash
cd backend
cp .env.example .env
```

Edit `.env`:
```env
DATABASE_URL=postgresql://demo:demo@localhost:5432/demo
JWT_SECRET=your-very-secret-key-change-this
COOKIE_SECURE=False  # True in production with HTTPS
```

### 3. Start PostgreSQL

```bash
cd infra
docker compose up -d
```

### 4. Seed Database

```bash
cd infra
python seed_db.py
```

This creates:
- 5,000 users
- 200 items
- 20,000 orders with items

### 5. Run the API Server

```bash
cd backend/app
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

API will be available at: `http://localhost:8000`

Interactive docs: `http://localhost:8000/docs`

## Authentication Flow

### Signup
```bash
curl -X POST "http://localhost:8000/auth/signup" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "securepassword",
    "name": "John Doe",
    "city": "Mumbai"
  }'
```

### Login
```bash
curl -X POST "http://localhost:8000/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "securepassword"
  }' \
  -c cookies.txt
```

### Use Protected Endpoints
```bash
curl -X GET "http://localhost:8000/users/me" \
  -b cookies.txt
```

## Creating an Admin User

Run this in PostgreSQL:

```sql
UPDATE users 
SET is_admin = true 
WHERE email = 'admin@example.com';
```

Or create directly in Python:

```python
from backend.app.crud.auth import create_user
from backend.app.database import SessionLocal

db = SessionLocal()
admin = create_user(
    db=db,
    email="admin@example.com",
    password="adminpass",
    name="Admin User",
    city="Delhi",
    is_admin=True
)
db.close()
```

## Testing the API

### 1. Test Items Endpoint
```bash
curl "http://localhost:8000/items?category=electronics&limit=5"
```

### 2. Add to Cart (requires login)
```bash
curl -X POST "http://localhost:8000/cart/add" \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{
    "item_id": 1,
    "quantity": 2
  }'
```

### 3. Place Order
```bash
curl -X POST "http://localhost:8000/orders/place" \
  -b cookies.txt
```

### 4. View Order History
```bash
curl "http://localhost:8000/users/me/orders?limit=5" \
  -b cookies.txt
```

### 5. Admin: View Metrics
```bash
curl "http://localhost:8000/metrics/most-ordered?limit=10" \
  -b admin-cookies.txt
```

### 6. Admin: Create Index
```bash
curl -X POST "http://localhost:8000/indexes/apply" \
  -H "Content-Type: application/json" \
  -b admin-cookies.txt \
  -d '{
    "table_name": "orders",
    "column_name": "user_id",
    "index_type": "btree"
  }'
```

## Workload Generation

Run continuous queries for testing:

```bash
cd infra
python workload.py
```

This generates random queries including:
- User lookups by city
- Order status queries
- Order-Item joins
- Category searches
- Expensive order queries
- User spending calculations

## Security Features

✅ **Password Security**
- Bcrypt hashing with automatic salt
- Never stores plain passwords

✅ **JWT Security**
- Stored in HttpOnly cookies (XSS protection)
- Configurable expiration (default 60 min)
- Secure flag for HTTPS in production

✅ **Authorization**
- User-specific operations (own orders only)
- Admin-only endpoints for management
- Automatic token validation on protected routes

## Production Deployment Checklist

- [ ] Change `JWT_SECRET` to a strong random string
- [ ] Set `COOKIE_SECURE=True`
- [ ] Use strong database passwords
- [ ] Enable HTTPS/SSL
- [ ] Configure proper CORS origins
- [ ] Set up database backups
- [ ] Add rate limiting
- [ ] Configure logging
- [ ] Use environment-specific configs

## Troubleshooting

### Import Errors
Make sure you're running from the correct directory:
```bash
cd backend/app
python -m uvicorn main:app --reload
```

### Database Connection Issues
Check PostgreSQL is running:
```bash
docker ps
psql -h localhost -U demo -d demo
```

### Cookie Not Set
Ensure you're using `-c cookies.txt` (save) and `-b cookies.txt` (load) with curl, or use browser/Postman which handles cookies automatically.

## License

MIT License
