# 🎯 Quick Start Guide

## Prerequisites Check
- [ ] PostgreSQL is running (`docker ps` should show postgres container)
- [ ] Python 3.8+ is installed
- [ ] pip is available

## Setup Steps (5 minutes)

### 1. Install Dependencies
```bash
cd /home/owais/DBS-Lab-Project
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
cd backend
cp .env.example .env
# Edit .env if needed (optional for testing)
```

### 3. Seed Database
```bash
cd ../infra
python seed_db.py
```

You should see:
```
Database seeded ✅
```

### 4. Start the API Server
```bash
cd ../backend/app
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Or use the startup script:
```bash
cd /home/owais/DBS-Lab-Project
./start.sh
```

### 5. Verify It's Running
Open in browser: http://localhost:8000/docs

You should see the Swagger UI with all endpoints.

## Quick Test Commands

### Test 1: Health Check
```bash
curl http://localhost:8000/
```

Expected: `{"status":"ok","app":"Adaptive Ordering System","version":"1.0.0"}`

### Test 2: List Items
```bash
curl http://localhost:8000/items?limit=5
```

Expected: JSON array of 5 items

### Test 3: Run Automated Tests
```bash
cd /home/owais/DBS-Lab-Project
python backend/test_api.py
```

Expected: All tests pass ✅

### Test 4: Create Admin User
```bash
cd /home/owais/DBS-Lab-Project
python backend/db_manager.py make-admin testuser@example.com
```

Expected: `✅ User testuser@example.com is now an admin`

### Test 5: View Database Stats
```bash
python backend/db_manager.py stats
```

Expected:
```
📊 Database Statistics
==================================================
👥 Users:       5,000
📦 Items:       200
🛒 Orders:      20,000
📋 Order Items: ~40,000
👑 Admins:      1
==================================================
```

## 🎓 Learning the API

### Signup & Login
```bash
# Signup
curl -X POST http://localhost:8000/auth/signup \
  -H "Content-Type: application/json" \
  -d '{"email":"demo@test.com","password":"demo123","name":"Demo User","city":"Mumbai"}'

# Login (saves cookie)
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"demo@test.com","password":"demo123"}' \
  -c cookies.txt

# Get profile
curl http://localhost:8000/users/me -b cookies.txt
```

### Shopping Flow
```bash
# Browse items
curl "http://localhost:8000/items?category=electronics&limit=10"

# Add to cart
curl -X POST http://localhost:8000/cart/add \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{"item_id":1,"quantity":2}'

# Place order
curl -X POST http://localhost:8000/orders/place \
  -b cookies.txt

# View orders
curl "http://localhost:8000/users/me/orders?limit=5" -b cookies.txt
```

### Admin Operations
```bash
# First, make user admin
python backend/db_manager.py make-admin demo@test.com

# Login as admin
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"demo@test.com","password":"demo123"}' \
  -c admin-cookies.txt

# View metrics
curl "http://localhost:8000/metrics/most-ordered?limit=10" \
  -b admin-cookies.txt

# Create index
curl -X POST http://localhost:8000/indexes/apply \
  -H "Content-Type: application/json" \
  -b admin-cookies.txt \
  -d '{"table_name":"orders","column_name":"user_id","index_type":"btree"}'
```

## 📚 Documentation Locations

- **API Docs**: http://localhost:8000/docs (Swagger UI)
- **Alternative Docs**: http://localhost:8000/redoc
- **Backend README**: `/home/owais/DBS-Lab-Project/backend/README.md`
- **Implementation Details**: `/home/owais/DBS-Lab-Project/IMPLEMENTATION_COMPLETE.md`

## 🛠️ Utility Scripts

### Database Manager
```bash
# Show help
python backend/db_manager.py

# Common commands
python backend/db_manager.py stats           # Show statistics
python backend/db_manager.py list 20         # List 20 users
python backend/db_manager.py create-admin    # Interactive admin creation
python backend/db_manager.py make-admin EMAIL # Make user admin
```

### API Tests
```bash
python backend/test_api.py
```

### Workload Generator
```bash
cd infra
python workload.py
# Press Ctrl+C to stop
```

## 🔧 Troubleshooting

### Error: "No module named 'app'"
**Solution**: Run from correct directory
```bash
cd backend/app
python -m uvicorn main:app --reload
```

### Error: Database connection refused
**Solution**: Start PostgreSQL
```bash
cd infra
docker compose up -d
```

### Error: "ModuleNotFoundError: No module named 'pydantic_settings'"
**Solution**: Install dependencies
```bash
pip install -r requirements.txt
```

### Error: "relation does not exist"
**Solution**: Seed the database
```bash
cd infra
python seed_db.py
```

## ✅ Success Checklist

- [ ] API server starts without errors
- [ ] http://localhost:8000/docs loads
- [ ] Can signup new user
- [ ] Can login and receive cookie
- [ ] Can view profile at `/users/me`
- [ ] Can list items at `/items`
- [ ] Can add to cart
- [ ] Can place order
- [ ] Can create admin user
- [ ] Admin can view metrics
- [ ] Admin can create indexes

## 🎉 You're Ready!

Your FastAPI backend is fully functional with:
- ✅ Complete authentication system
- ✅ E-commerce cart & orders
- ✅ Admin analytics dashboard
- ✅ Database index management
- ✅ 25,000+ records of test data

**Next Steps:**
1. Explore the API with Swagger UI
2. Try the automated test script
3. Create a frontend or integrate with your existing one
4. Experiment with database indexes using the indexes endpoints

**Have fun building! 🚀**
