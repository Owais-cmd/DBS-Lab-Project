# 🚀 Quick Start Guide

Get the ShopHub project up and running in 5 minutes!

## Prerequisites

- Node.js 16+
- Python 3.8+
- Docker Desktop
- Git

## One-Command Setup (Recommended)

### On Windows (PowerShell as Administrator)

```powershell
# Clone or navigate to project
cd DBS-Lab-Project

# Start everything
docker compose up -d
cd backend && python -m venv venv && .\venv\Scripts\activate && pip install -r requirements.txt
cd ../frontend-react && npm install
```

Then in separate terminals:

**Terminal 1 - Backend:**

```bash
cd backend
.\venv\Scripts\activate
cd app
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

**Terminal 2 - Frontend:**

```bash
cd frontend-react
npm run dev
```

## Manual Setup (Step by Step)

### Step 1: Start Database (2 minutes)

```bash
cd DBS-Lab-Project
docker compose up -d
```

### Step 2: Setup Backend (1 minute)

```bash
cd backend

# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python -m venv venv
source venv/bin/activate

pip install -r requirements.txt

cd app
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Backend runs at: **http://localhost:8000**

### Step 3: Setup Frontend (1 minute)

```bash
# In new terminal
cd frontend-react
npm install
npm run dev
```

Frontend runs at: **http://localhost:5173**

### Step 4: Create Admin Account (1 minute)

1. Open http://localhost:5173 in browser
2. Click "Sign Up"
3. Enter:
   - Email: `admin@example.com`
   - Password: `password123`
   - Role: `Admin`
4. Click "Sign Up"

✅ You're ready!

## What to Try First

### 👨‍💼 Admin Features

1. Go to `/admin` after logging in as admin
2. **Products Tab**: Add a new product
3. **Analysis Tab**: View database query analysis

### 🛒 Customer Features

1. Sign up as customer (role: `user`)
2. Browse products on shopping page
3. Add items to cart
4. View cart and update quantities
5. Proceed to checkout

## URLs

| Component | URL                        | Default Credentials             |
| --------- | -------------------------- | ------------------------------- |
| Frontend  | http://localhost:5173      | admin@example.com / password123 |
| Backend   | http://localhost:8000      | (JWT auth)                      |
| API Docs  | http://localhost:8000/docs | (Swagger)                       |
| Database  | localhost:5432             | demo/demo                       |

## Project Structure

```
DBS-Lab-Project/
├── frontend-react/        ← React UI (npm run dev)
├── backend/
│   ├── app/
│   │   └── main.py       ← FastAPI server
│   └── requirements.txt
├── infra/
│   └── docker-compose.yml ← PostgreSQL config
├── data/                  ← Query analysis data
└── INTEGRATION_GUIDE.md   ← Detailed setup
```

## Common Issues & Solutions

### "Connection refused" - Backend not running?

```bash
# Check if backend is running
curl http://localhost:8000/health

# If not, ensure virtual environment is activated and run:
cd backend/app
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### "Port 5173 already in use"

```bash
# Use different port
cd frontend-react
npm run dev -- --port 5174
```

### "Database connection error"

```bash
# Check if PostgreSQL is running
docker compose ps

# If not, restart
docker compose down
docker compose up -d
```

### "npm: command not found"

- Install Node.js from https://nodejs.org

### "python: command not found"

- Install Python from https://python.org

## Next Steps

📖 **Read Full Documentation**

- `INTEGRATION_GUIDE.md` - Complete architecture guide
- `FRONTEND_SETUP.md` - Frontend detailed setup
- `BACKEND_SETUP.md` - Backend detailed setup

🎨 **Customize**

- Edit products in admin dashboard
- Modify Tailwind colors in `frontend-react/tailwind.config.js`
- Update API endpoints as needed

🚀 **Deploy**

- Deploy frontend to Vercel/Netlify
- Deploy backend to Heroku/Railway
- Use managed PostgreSQL database

## Getting Help

- Check logs in terminal for errors
- Visit http://localhost:8000/docs for API documentation
- Review code comments in source files
- Check browser console (F12) for frontend errors

---

**Congratulations!** 🎉 Your ShopHub project is running!

Need more details? See the full `INTEGRATION_GUIDE.md`
