# 🛍️ ShopHub - DBS Lab Project

A full-stack e-commerce platform with **adaptive database index advisor**, featuring a modern React frontend and FastAPI backend. Real-time query optimization powered by PostgreSQL's `pg_stat_statements`.

## ✨ Features

### 🎨 Frontend (React 18 + TypeScript + Shadcn/UI)

- **Modern UI** - Sleek design with Tailwind CSS
- **Micro-interactions** - Smooth animations with Framer Motion
- **Authentication** - JWT-based user auth with role-based access (user/admin)
- **Shopping** - Browse products, add to cart, manage quantities
- **Admin Dashboard** - Product management + Query analysis interface

### ⚡ Backend (FastAPI + SQLAlchemy)

- **RESTful API** - Complete REST endpoints for all operations
- **Authentication** - JWT tokens with role-based access control
- **Database** - PostgreSQL with optimized schema
- **Query Analysis** - Adaptive index recommendations based on workload
- **CRUD Operations** - Full product and cart management

### 🔍 Database Intelligence

- **Query Monitoring** - Real-time query statistics with `pg_stat_statements`
- **Index Recommendations** - AI-powered suggestions based on query patterns
- **Index Management** - Create/delete indexes with one click
- **Performance Tracking** - Monitor query execution times and frequency

## 📊 Project Structure

```
DBS-Lab-Project/
├── 🎨 frontend-react/          ← React 18 UI (npm run dev)
│   ├── src/
│   │   ├── pages/              ← Landing, Shopping, Cart, Admin
│   │   ├── components/         ← Reusable UI components
│   │   ├── store/              ← Zustand state management
│   │   ├── lib/ui/             ← Shadcn/UI components
│   │   └── App.tsx             ← Main app with routing
│   └── package.json
├── ⚙️ backend/
│   ├── app/
│   │   ├── main.py             ← FastAPI server (UPDATED)
│   │   ├── models.py           ← SQLAlchemy models
│   │   ├── database.py         ← Database connection
│   │   └── main_new.py         ← New backend code
│   ├── collector.py            ← Query statistics collector
│   ├── recommender_rules.py    ← Index recommendation engine
│   └── requirements.txt
├── 🐘 infra/
│   ├── docker-compose.yml      ← PostgreSQL 15 setup
│   ├── seed_db.py              ← Sample data seeder
│   └── workload.py             ← Query workload generator
├── 📖 QUICKSTART.md            ← Get started in 5 minutes
├── 📖 INTEGRATION_GUIDE.md     ← Complete architecture guide
├── 📖 FRONTEND_SETUP.md        ← Frontend detailed setup
├── 📖 BACKEND_SETUP.md         ← Backend detailed setup
└── data/                       ← Query analysis results
```

## 🚀 Quick Start (5 Minutes)

### 1️⃣ **Prerequisites**

- Node.js 16+
- Python 3.8+
- Docker Desktop

### 2️⃣ **Start Everything**

**Terminal 1 - Database:**

```bash
cd DBS-Lab-Project
docker compose up -d
```

**Terminal 2 - Backend:**

```bash
cd backend
python -m venv venv

# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate

pip install -r requirements.txt
cd app
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

**Terminal 3 - Frontend:**

```bash
cd frontend-react
npm install
npm run dev
```

### 3️⃣ **Access the App**

- **Frontend**: http://localhost:5173
- **Backend API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs

### 4️⃣ **Create Test Account**

- Click Sign Up
- Email: `admin@example.com`
- Password: `password123`
- Role: `Admin` (or `Customer` for regular user)

🎉 **Done!** Start shopping or manage products!

## 📚 Full Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - Get started in 5 minutes
- **[INTEGRATION_GUIDE.md](INTEGRATION_GUIDE.md)** - Complete setup guide
- **[FRONTEND_SETUP.md](FRONTEND_SETUP.md)** - React frontend details
- **[BACKEND_SETUP.md](BACKEND_SETUP.md)** - FastAPI backend details

## 🎯 What You Can Do

### As a Customer

✅ Sign up and login
✅ Browse product catalog
✅ Add items to cart
✅ Manage cart quantities
✅ View order summary
✅ Checkout (simulated)

### As an Admin

✅ All customer features +
✅ Create new products
✅ Delete products
✅ View query statistics
✅ Get index recommendations
✅ Create database indexes
✅ Monitor index performance

## 🛠️ Tech Stack

| Layer        | Technology                                                   |
| ------------ | ------------------------------------------------------------ |
| **Frontend** | React 18, TypeScript, Tailwind CSS, Shadcn/UI, Framer Motion |
| **Backend**  | FastAPI, SQLAlchemy, Pydantic, JWT, passlib                  |
| **Database** | PostgreSQL 15, pg_stat_statements                            |
| **State**    | Zustand, React Router                                        |
| **Build**    | Vite, npm                                                    |

## 📊 Database Schema

- **users** - Authentication and profiles
- **products** - Product catalog
- **cart_items** - Shopping cart items
- **orders** - Order history
- **order_items** - Items in orders
- **index_audit** - Index creation tracking

## 🔐 Authentication

JWT-based authentication with:

- User registration/login
- Role-based access control (user/admin)
- Secure password hashing (bcrypt)
- Token expiration (30 minutes)

## 🎨 UI Highlights

### Design Features

- 🎯 Sleek, modern color scheme (red accent on slate)
- 📱 Fully responsive (mobile, tablet, desktop)
- ♿ Accessible components (ARIA labels, keyboard navigation)
- ⚡ Smooth animations and transitions
- 🌙 Dark mode ready (configured in Tailwind)

### Components

- Custom buttons with variants
- Form inputs with validation
- Modal dialogs
- Cards and badges
- Loading spinners
- Toast notifications (ready)
- Animated lists

## 📈 Query Optimization

The admin dashboard includes an **adaptive index advisor**:

1. **Real-time Monitoring** - Tracks all SQL queries
2. **Analysis** - Identifies slow/frequently called queries
3. **Recommendations** - Suggests indexes to create
4. **One-Click Apply** - Create indexes with single button
5. **Performance Tracking** - Monitor index effectiveness

## 🌐 API Endpoints

### Auth

```
POST /signup        - Register user
POST /login         - User login
```

### Products

```
GET  /products              - List all products
POST /products              - Create product (admin)
DELETE /products/{id}       - Delete product (admin)
```

### Shopping

```
GET  /cart                  - Get user's cart
POST /cart                  - Add to cart
PUT  /cart/{product_id}     - Update quantity
DELETE /cart/{product_id}   - Remove from cart
```

### Analysis

```
GET  /recommendations       - Get index recommendations
GET  /indexes              - Get current indexes
POST /apply                - Apply index (admin)
```

## 🚀 Next Steps

1. **First Time?** Start with [QUICKSTART.md](QUICKSTART.md)
2. **Full Setup?** Read [INTEGRATION_GUIDE.md](INTEGRATION_GUIDE.md)
3. **Frontend Dev?** Check [FRONTEND_SETUP.md](FRONTEND_SETUP.md)
4. **Backend Dev?** Check [BACKEND_SETUP.md](BACKEND_SETUP.md)
5. **Need API Docs?** Visit http://localhost:8000/docs (Swagger)

## 📝 Original Lab Features

Still includes all original DBS Lab features:

- PostgreSQL instance with `pg_stat_statements`
- Synthetic data generation (users, orders)
- Query workload generator
- Query statistics collector
- Recommendation engine

## 🤝 Contributing

Found a bug? Want a feature?

1. Create an issue in GitHub
2. Fork the repository
3. Create a feature branch
4. Submit a pull request

## 📄 License

MIT License - Feel free to use and modify!

---

## 📧 Support

For issues or questions:

1. Check the documentation files
2. Review API documentation at `/docs`
3. Check browser console for frontend errors
4. Check terminal logs for backend errors

**Happy Coding! 🚀**

---

### Quick Troubleshooting

| Issue                | Solution                                   |
| -------------------- | ------------------------------------------ |
| Frontend won't load  | Is backend running on 8000?                |
| Login fails          | Check email/password, verify DB is running |
| Products not showing | Admin needs to create products first       |
| Cart not working     | Verify user is authenticated               |
| Database error       | Restart: `docker compose restart`          |

For more troubleshooting, see [INTEGRATION_GUIDE.md](INTEGRATION_GUIDE.md)
