# DBS Lab Project - Backend Setup Guide

## Backend Requirements Update

Add these dependencies to your `requirements.txt`:

```
fastapi>=0.104.0
uvicorn>=0.24.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9
pydantic[email]>=2.0
python-jose[cryptography]>=3.3.0
passlib[bcrypt]>=1.7.4
python-multipart>=0.0.6
python-dotenv>=1.0.0
pandas>=1.5
psycopg2-binary>=2.9
scikit-learn>=1.2.0
sqlparse>=0.4.3
apscheduler>=3.10.0
joblib>=1.3.0
alembic>=1.12.0
```

## Running the Backend

### 1. Start PostgreSQL

```cmd
docker compose up -d
```

### 2. Install Dependencies

```cmd
pip install -r requirements.txt
```

### 3. Run the New Backend

Replace the old `main.py` with `main_new.py`:

```cmd
cd backend/app
# Backup old main.py
ren main.py main_old.py
ren main_new.py main.py

# Run backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## Database Schema

The backend automatically creates the following tables:

### users

```sql
- id (Primary Key)
- email (unique)
- hashed_password
- role (user/admin)
- created_at
```

### products

```sql
- id (Primary Key)
- name
- price
- description
- image_url (optional)
- created_at
```

### cart_items

```sql
- id (Primary Key)
- user_id (Foreign Key)
- product_id (Foreign Key)
- quantity
- added_at
```

### orders

```sql
- id (Primary Key)
- user_id (Foreign Key)
- total_price
- created_at
```

### order_items

```sql
- id (Primary Key)
- order_id (Foreign Key)
- product_id (Foreign Key)
- quantity
- price
```

### index_audit

```sql
- id (Primary Key)
- action
- index_name
- table_name
- column_name
- user_name
- ts (timestamp)
- details (JSON)
```

## API Endpoints

### Authentication

- **POST** `/signup` - Register new user

  - Body: `{email, password, role}`
  - Returns: `{access_token, token_type, user}`

- **POST** `/login` - Login user
  - Body: `{email, password}`
  - Returns: `{access_token, token_type, user}`

### Products (All users can view)

- **GET** `/products` - Get all products
- **POST** `/products` - Create product (admin only)
- **DELETE** `/products/{id}` - Delete product (admin only)

### Cart (Authenticated users)

- **GET** `/cart` - Get user's cart
- **POST** `/cart` - Add item to cart
  - Body: `{product_id, quantity}`
- **PUT** `/cart/{product_id}` - Update quantity
  - Body: `{quantity}`
- **DELETE** `/cart/{product_id}` - Remove item

### Analysis (Admin only)

- **GET** `/recommendations` - Get index recommendations
- **GET** `/indexes` - Get current indexes
- **POST** `/apply` - Apply index
  - Body: `{table, column, force, user}`

### Health

- **GET** `/health` - Health check

## Admin Account Setup

Create an admin account:

1. Signup with role "admin"
2. Use admin credentials to log in
3. Access admin dashboard at `/admin`

## CORS Configuration

The backend allows requests from:

- http://localhost:3000
- http://localhost:5173
- http://localhost:8000

Update `ALLOWED_ORIGINS` in `main.py` for production.

## Environment Variables

Create a `.env` file in the backend root:

```env
DATABASE_URL=postgresql://demo:demo@localhost:5432/demo
SECRET_KEY=your-secret-key-change-in-production
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
```

## Testing the API

Use the provided Postman collection or test with curl:

```bash
# Signup
curl -X POST "http://localhost:8000/signup" \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@example.com","password":"password123","role":"admin"}'

# Login
curl -X POST "http://localhost:8000/login" \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@example.com","password":"password123"}'

# Get products
curl "http://localhost:8000/products"

# Create product (requires auth token)
curl -X POST "http://localhost:8000/products" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"Product 1","price":29.99,"description":"Test product"}'
```

## Troubleshooting

### CORS Errors

- Ensure frontend URL is in `ALLOWED_ORIGINS`
- Check that backend is running on port 8000

### Database Connection Errors

- Verify PostgreSQL is running: `docker compose ps`
- Check DATABASE_URL in environment variables
- Ensure database credentials are correct

### Authentication Errors

- Verify JWT secret key is set
- Check token expiration time
- Ensure token is passed in Authorization header as "Bearer {token}"

## Performance Notes

The adaptive index advisor monitors queries using `pg_stat_statements`. To use:

1. Run the workload generator:

```bash
python infra/workload.py
```

2. Run the collector:

```bash
python backend/collector.py
```

3. Access recommendations via API:

```bash
curl "http://localhost:8000/recommendations" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

The system automatically enforces a 3-index limit, removing the oldest index when a new one is created.
