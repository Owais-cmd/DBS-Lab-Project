#!/bin/bash
# Startup script for Adaptive Ordering System Backend

echo "🚀 Starting Adaptive Ordering System..."

# Check if PostgreSQL is running
if ! docker ps | grep -q postgres; then
    echo "📦 Starting PostgreSQL with Docker..."
    cd infra
    docker compose up -d
    cd ..
    echo "⏳ Waiting for PostgreSQL to be ready..."
    sleep 5
fi

# Check if database is seeded
echo "🔍 Checking database..."
ROWS=$(psql -h localhost -U demo -d demo -t -c "SELECT COUNT(*) FROM users;" 2>/dev/null)

if [ -z "$ROWS" ] || [ "$ROWS" -lt 100 ]; then
    echo "🌱 Seeding database..."
    cd infra
    python seed_db.py
    cd ..
fi

# Install dependencies if needed
if [ ! -d "venv" ]; then
    echo "📚 Creating virtual environment..."
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

# Check if .env exists
if [ ! -f "backend/.env" ]; then
    echo "⚙️  Creating .env file..."
    cp backend/.env.example backend/.env
    echo "⚠️  Please update backend/.env with your settings!"
fi

# Start the server
echo "✅ Starting FastAPI server..."
cd backend/app
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
