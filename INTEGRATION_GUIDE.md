# DBS Lab Project - Complete Integration Guide

## Project Overview

A full-stack e-commerce application with adaptive database index advisor, built with:

- **Frontend**: React 18 + TypeScript + Tailwind CSS + Shadcn/UI
- **Backend**: FastAPI + SQLAlchemy + PostgreSQL
- **Database**: PostgreSQL 15 with pg_stat_statements

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Browser                           │
│            (React 18 + Tailwind + Shadcn/UI)                │
│                  frontend-react:3000                         │
└────────────────────────┬────────────────────────────────────┘
                         │ HTTP/REST/JSON
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI Backend                           │
│              backend/app/main.py:8000                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Auth Routes  │  │ Product APIs │  │ Cart Routes  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│  ┌──────────────────────────────────────────────────┐       │
│  │      Query Analysis & Index Advisor             │       │
│  │ (recommendations, indexes, apply endpoints)     │       │
│  └──────────────────────────────────────────────────┘      │
└────────────────────────┬────────────────────────────────────┘
                         │ SQL/psycopg2
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                PostgreSQL Database                           │
│              localhost:5432 (Docker)                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │
│  │ users    │ │ products │ │ orders   │ │ cart     │      │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘      │
│  ┌──────────────────────────────────────────────────┐       │
│  │  index_audit + pg_stat_statements                │       │
│  │  (Query analysis for recommendations)            │       │
│  └──────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

## Setup Steps

### Step 1: Prerequisites

✅ Install Required Software:

- Node.js 16+ (https://nodejs.org)
- Python 3.8+ (https://python.org)
- Docker Desktop (https://docker.com)
- Git (https://git-scm.com)

### Step 2: Database Setup

```bash
# Navigate to project root
cd DBS-Lab-Project

# Start PostgreSQL in Docker
docker compose up -d

# Verify database is running
docker compose ps
```

### Step 3: Backend Setup

```bash
# Navigate to backend
cd backend

# Create Python virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start backend server
cd app
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Backend will be available at: http://localhost:8000

### Step 4: Frontend Setup

```bash
# In a new terminal, navigate to frontend
cd frontend-react

# Install dependencies
npm install

# Start development server
npm run dev
```

Frontend will be available at: http://localhost:5173

### Step 5: Verify Everything Works

1. **Backend Health Check**

   - Open http://localhost:8000/health in browser
   - Should see: `{"status":"ok"}`

2. **Frontend Access**

   - Open http://localhost:5173 in browser
   - Should see login/signup page

3. **Create Test Account**
   - Sign up with email: `admin@example.com`
   - Password: `password123`
   - Select role: `Admin`

## Usage Guide

### For Regular Users

1. **Sign Up / Login**

   - Go to http://localhost:5173
   - Create account or login
   - Select "Customer" role for regular user

2. **Browse Products**

   - View all products on Shopping page
   - Add items to cart with quantity selection
   - Wishlist items (UI feature)

3. **Manage Cart**

   - Click cart icon in header
   - View cart details
   - Update quantities or remove items
   - See order summary with tax

4. **Checkout**
   - Click "Proceed to Checkout"
   - Simulates order placement

### For Admin Users

1. **Admin Login**

   - Sign up/login with admin role
   - Access admin dashboard at `/admin`

2. **Product Management**

   - Navigate to "Products" tab
   - Click "Add Product" button
   - Fill in product details (name, price, description)
   - View all products in grid
   - Delete products as needed

3. **Query Analysis**
   - Navigate to "Analysis" tab
   - View current database indexes
   - View index recommendations
   - Click recommendation to see query details
   - Click "Apply Index" to create index
   - System automatically limits to 3 indexes

## Database Schema

### Users Table

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR UNIQUE NOT NULL,
    hashed_password VARCHAR NOT NULL,
    role VARCHAR DEFAULT 'user',
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Products Table

```sql
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    price FLOAT NOT NULL,
    description VARCHAR,
    image_url VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Cart Items Table

```sql
CREATE TABLE cart_items (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER DEFAULT 1,
    added_at TIMESTAMP DEFAULT NOW()
);
```

### Orders Table

```sql
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    total_price FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Order Items Table

```sql
CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER,
    price FLOAT
);
```

### Index Audit Table

```sql
CREATE TABLE index_audit (
    id SERIAL PRIMARY KEY,
    action VARCHAR,
    index_name VARCHAR,
    table_name VARCHAR,
    column_name VARCHAR,
    user_name VARCHAR,
    ts TIMESTAMP DEFAULT NOW(),
    details JSONB
);
```

## API Endpoints Reference

### Authentication

```
POST /signup
Body: {email, password, role}
Response: {access_token, token_type, user}

POST /login
Body: {email, password}
Response: {access_token, token_type, user}
```

### Products

```
GET /products
Response: [Product]

POST /products (Admin)
Header: Authorization: Bearer {token}
Body: {name, price, description, image_url?}
Response: Product

DELETE /products/{id} (Admin)
Header: Authorization: Bearer {token}
Response: {message}
```

### Cart

```
GET /cart
Header: Authorization: Bearer {token}
Response: {items}

POST /cart
Header: Authorization: Bearer {token}
Body: {product_id, quantity}
Response: {message}

PUT /cart/{product_id}
Header: Authorization: Bearer {token}
Body: {quantity}
Response: {message}

DELETE /cart/{product_id}
Header: Authorization: Bearer {token}
Response: {message}
```

### Query Analysis

```
GET /recommendations (Requires token)
Response: [Recommendation]

GET /indexes (Requires token)
Response: [Index]

POST /apply (Admin only)
Header: Authorization: Bearer {token}
Body: {table, column, force, user}
Response: {index, deleted_index?, message}
```

## Performance Monitoring

### Enable Query Monitoring

1. **Run Workload Generator**

   ```bash
   cd backend
   python infra/workload.py
   ```

2. **Run Collector**

   ```bash
   cd backend
   python backend/collector.py
   ```

3. **Generate Recommendations**

   ```bash
   cd backend
   python backend/recommender_rules.py
   ```

4. **View Recommendations in Admin Dashboard**
   - Login as admin
   - Go to Analysis tab
   - See recommended indexes

## Development Workflow

### Adding New Features

1. **Create Feature Branch**

   ```bash
   git checkout -b feature/my-feature
   ```

2. **Frontend Changes**

   ```bash
   cd frontend-react
   # Edit components/pages
   # Run: npm run dev
   # Test in browser
   ```

3. **Backend Changes**

   ```bash
   cd backend/app
   # Edit main.py or models.py
   # Changes auto-reload with --reload flag
   # Test with curl or Postman
   ```

4. **Database Changes**

   - Models are auto-created by SQLAlchemy
   - For migrations, use Alembic:

   ```bash
   alembic init migrations
   alembic revision --autogenerate -m "description"
   alembic upgrade head
   ```

5. **Commit and Push**
   ```bash
   git add .
   git commit -m "feat: description of changes"
   git push origin feature/my-feature
   ```

## Testing the Complete Flow

### 1. Test User Registration

```bash
# Sign up as regular user
POST http://localhost:8000/signup
{
  "email": "user@example.com",
  "password": "password123",
  "role": "user"
}
```

### 2. Test Product Management (Admin)

```bash
# Create product
POST http://localhost:8000/products
Headers: Authorization: Bearer {admin_token}
{
  "name": "Laptop",
  "price": 999.99,
  "description": "High-performance laptop"
}
```

### 3. Test Shopping Flow

```bash
# Add to cart
POST http://localhost:8000/cart
Headers: Authorization: Bearer {user_token}
{
  "product_id": 1,
  "quantity": 2
}

# View cart
GET http://localhost:8000/cart
Headers: Authorization: Bearer {user_token}
```

### 4. Test Admin Analysis

```bash
# Get recommendations
GET http://localhost:8000/recommendations
Headers: Authorization: Bearer {admin_token}

# Apply index
POST http://localhost:8000/apply
Headers: Authorization: Bearer {admin_token}
{
  "table": "orders",
  "column": "user_id",
  "force": true,
  "user": "admin@example.com"
}
```

## Troubleshooting

### Issue: Frontend can't connect to backend

**Solution**:

- Check backend is running: `http://localhost:8000/health`
- Verify CORS settings in backend
- Check browser console for errors
- Restart both frontend and backend

### Issue: Database connection error

**Solution**:

- Verify Docker container is running: `docker compose ps`
- Check PostgreSQL is accepting connections
- Verify DATABASE_URL environment variable
- Restart database: `docker compose restart`

### Issue: Authentication not working

**Solution**:

- Clear localStorage in browser
- Verify token is being saved
- Check backend logs for JWT errors
- Ensure SECRET_KEY is set in backend

### Issue: Products not appearing

**Solution**:

- Verify admin account created a product
- Check backend logs for SQL errors
- Verify product inserted into database
- Restart frontend to refresh

## Production Deployment

### Frontend Deployment (Vercel)

```bash
cd frontend-react
vercel
# Follow prompts to deploy
```

### Backend Deployment (Heroku/Railway)

```bash
# Build Docker image
docker build -t shophub-backend .

# Push to registry
docker tag shophub-backend myregistry/shophub-backend
docker push myregistry/shophub-backend

# Deploy to cloud
```

### Environment Variables (Production)

```
Backend:
- DATABASE_URL=postgresql://user:password@host:5432/dbname
- SECRET_KEY=your-secure-random-key
- ALGORITHM=HS256

Frontend:
- VITE_API_URL=https://your-api-domain.com
```

## Performance Tips

1. **Database Optimization**

   - Use adaptive indexes from recommendations
   - Monitor query performance in admin dashboard
   - Run workload generator periodically

2. **Frontend Optimization**

   - Build: `npm run build` for production
   - Enable caching in production
   - Use CDN for static assets

3. **Backend Optimization**
   - Enable connection pooling
   - Cache recommendations
   - Implement rate limiting

## Support & Documentation

- **Frontend Docs**: See `FRONTEND_SETUP.md`
- **Backend Docs**: See `BACKEND_SETUP.md`
- **API Docs**: Available at `http://localhost:8000/docs` (Swagger UI)
- **GitHub Issues**: Report bugs in GitHub

## License

MIT - Feel free to use and modify
