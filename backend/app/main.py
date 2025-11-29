# app/main.py
import os
import json
from fastapi import FastAPI, HTTPException, Depends, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from .database import get_db, init_db
from .models import User, Item, Order, OrderItem
from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy import text
import time

# Import all routers
from .routers import (
    auth_router,
    users_router,
    items_router,
    orders_router,
    metrics_router,
    indexes_router
)
from .config import settings


DATABASE_URL = settings.DATABASE_URL
AUDIT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS index_audit (
  id serial PRIMARY KEY,
  action text,
  index_name text,
  table_name text,
  column_name text,
  user_name text,
  ts timestamptz default now(),
  details jsonb
);
"""

app = FastAPI(
    title=settings.APP_NAME,
    description="Adaptive Ordering System with PostgreSQL Optimization",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8501"],  # Adjust for your frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include all routers
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(items_router)
app.include_router(orders_router)
app.include_router(metrics_router)
app.include_router(indexes_router)


def get_conn():
    return psycopg2.connect(DATABASE_URL)


@app.on_event("startup")
async def startup():
    init_db()
    # ensure audit table exists
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(AUDIT_TABLE_SQL)
    conn.commit()
    cur.close()
    conn.close()


@app.get("/")
def root():
    """Health check endpoint."""
    return {
        "status": "ok",
        "app": settings.APP_NAME,
        "version": "1.0.0"
    }
