# 🚀 ShopHub Project - RUNNING

## ✅ Status: All Services Online

### Services Status

| Service         | URL                        | Status     | Port |
| --------------- | -------------------------- | ---------- | ---- |
| **Frontend**    | http://localhost:3000      | ✅ Running | 3000 |
| **Backend API** | http://localhost:8000      | ✅ Running | 8000 |
| **API Docs**    | http://localhost:8000/docs | ✅ Running | 8000 |
| **Database**    | PostgreSQL                 | ✅ Running | 5432 |

---

## 🎯 What You Can Do Now

### 1. **Create an Admin Account**

Go to http://localhost:3000 and:

- Click "Sign Up"
- Email: `admin@example.com`
- Password: `password123`
- Role: `Admin`
- Click "Sign Up"

### 2. **Test Admin Features**

After logging in as admin:

- Go to "Admin Dashboard"
- **Products Tab**: Add new products with name, price, description, image URL
- **Analysis Tab**: View query recommendations and database indexes

### 3. **Shop as User**

- Go to "Shopping"
- Browse products
- Add items to cart (heart icon for wishlist)
- Go to "Cart"
- View order summary with tax calculation
- Checkout (creates order in database)

### 4. **Test Authentication**

- Login with credentials
- Logout
- Create new user account
- Only admins can access Admin Dashboard
- Cart data persists to database

---

## 📁 Project Structure

```
DBS-Lab-Project/
├── docker-compose.yml          ← PostgreSQL database
├── backend/
│   ├── venv/                   ← Python virtual environment
│   ├── app/
│   │   ├── main_new.py         ← FastAPI backend (RUNNING)
│   │   ├── models.py           ← Database models
│   │   └── database.py         ← DB connection
│   └── requirements.txt        ← Python dependencies
├── frontend-react/
│   ├── node_modules/           ← npm packages
│   ├── src/
│   │   ├── pages/              ← All 4 pages
│   │   ├── store/              ← Zustand stores
│   │   └── lib/ui/             ← Shadcn components
│   ├── package.json            ← npm config
│   └── npm run dev             ← (RUNNING on port 3000)
└── data/
    └── recommendations.json    ← Query recommendations
```

---

## 🔧 Running Services

### Backend (Python)

```bash
cd backend
venv\Scripts\activate
cd app
uvicorn main_new:app --host 0.0.0.0 --port 8000
```

### Frontend (Node.js)

```bash
cd frontend-react
npm run dev
```

### Database (Docker)

```bash
docker compose up -d
```

All three are currently running!

---

## 🧪 API Endpoints

### Authentication

- `POST /signup` - Create new account
- `POST /login` - Login with email/password

### Products

- `GET /products` - List all products
- `POST /products` (admin) - Create product
- `DELETE /products/{id}` (admin) - Delete product

### Shopping Cart

- `GET /cart` - Get user's cart
- `POST /cart` - Add item to cart
- `PUT /cart/{id}` - Update item quantity
- `DELETE /cart/{id}` - Remove from cart

### Analysis (Admin)

- `GET /recommendations` - Get query recommendations
- `GET /indexes` - List database indexes
- `POST /apply` (admin) - Apply index recommendations

### Docs

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

---

## 💾 Database

PostgreSQL database with 7 tables:

- `users` - User accounts
- `products` - Product catalog
- `cart_items` - Shopping cart items
- `orders` - Customer orders
- `order_items` - Items in orders
- `index_audit` - Index change history

Auto-created on backend startup!

---

## 🎨 Frontend Features

### Pages

1. **Landing** - Login/Signup authentication
2. **Shopping** - Product grid with add to cart
3. **Cart** - Manage items, view total with tax
4. **Admin Dashboard** - Product management & query analysis

### UI Components

- ✅ Button (4 variants)
- ✅ Input (with validation)
- ✅ Card (containers)
- ✅ Label (form labels)
- ✅ Badge (status indicators)
- ✅ Textarea (descriptions)

### Animations

- 🎬 Page transitions (fade + slide)
- 🎬 Button hover effects
- 🎬 List animations (staggered)
- 🎬 Modal animations
- 🎬 Loading spinners

---

## 🔐 Authentication

JWT-based authentication:

- Tokens stored in browser localStorage
- Auto-included in API requests
- Token expires after 30 minutes
- Role-based access control (user vs admin)

---

## 🛠️ Tech Stack

**Frontend:**

- React 18 + TypeScript
- Vite (build tool)
- Tailwind CSS (styling)
- Shadcn/UI (components)
- Zustand (state management)
- React Router (navigation)
- Framer Motion (animations)
- Axios (HTTP client)

**Backend:**

- FastAPI (API framework)
- SQLAlchemy (ORM)
- PostgreSQL (database)
- Passlib + bcrypt (password hashing)
- Python-Jose (JWT tokens)

---

## ✨ Test Scenarios

### Scenario 1: Admin Workflow

1. Sign up as admin@example.com / Admin role
2. Go to Admin Dashboard
3. Products tab: Add "Laptop" for $999
4. Analysis tab: View recommendations
5. Products tab: Delete product

### Scenario 2: Shopping Workflow

1. Sign up as user@example.com / User role
2. Go to Shopping
3. Add "Laptop" to cart
4. Go to Cart
5. Adjust quantity
6. View total with tax
7. Checkout

### Scenario 3: Authentication

1. Signup → stored in database
2. Logout → clear localStorage
3. Login → get JWT token
4. Refresh page → stay logged in (token in storage)
5. Try accessing admin page as user → redirect

---

## 📊 Next Steps

1. ✅ **Backend Running** - API at http://localhost:8000
2. ✅ **Frontend Running** - App at http://localhost:3000
3. ✅ **Database Running** - PostgreSQL on port 5432
4. 👉 **Create Account** - Sign up to test
5. 👉 **Test Features** - Add products, shop, checkout
6. 👉 **Review Admin** - Test admin dashboard

---

## 🐛 Troubleshooting

### Frontend not loading?

- Check: http://localhost:3000 (should show landing page)
- Terminal: `npm run dev` should show "ready in XXX ms"
- Clear browser cache (Ctrl+Shift+Delete)

### Backend not responding?

- Check: http://localhost:8000/docs
- Should show Swagger documentation
- Terminal: Should show "Application startup complete"

### Database connection failed?

- Check: `docker ps` (should show running PostgreSQL)
- Restart: `docker compose restart`

### Login not working?

- First create account (Sign Up)
- Email must be valid format
- Password must be stored (check browser DevTools > Application > localStorage)

---

## 📝 Database Credentials

```
Username: demo
Password: demo
Database: demo
Host: localhost
Port: 5432
```

---

## ✅ Project Complete!

All components are:

- ✅ Built
- ✅ Configured
- ✅ Running
- ✅ Connected

**Go to http://localhost:3000 to start using ShopHub!**
