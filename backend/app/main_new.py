# app/main.py
import os
import json
from datetime import datetime, timedelta
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Depends, Form, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy import func, text
from passlib.context import CryptContext
from jose import JWTError, jwt
import psycopg2

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://demo:demo@localhost:5432/demo")
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# CORS
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5173",
    "http://localhost:8000",
]

# Database setup
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Security
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# Recommendation file path
RECS_FILE = os.path.join(os.path.dirname(__file__), "../../data/recommendations.json")

# ===== Database Models =====
class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    role = Column(String, default="user")  # user or admin
    created_at = Column(DateTime, default=datetime.utcnow)
    
    orders = relationship("Order", back_populates="user")
    cart_items = relationship("CartItem", back_populates="user")

class Product(Base):
    __tablename__ = "products"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    price = Column(Float)
    description = Column(String)
    image_url = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    cart_items = relationship("CartItem", back_populates="product")
    order_items = relationship("OrderItem", back_populates="product")

class CartItem(Base):
    __tablename__ = "cart_items"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    product_id = Column(Integer, ForeignKey("products.id"))
    quantity = Column(Integer, default=1)
    added_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", back_populates="cart_items")
    product = relationship("Product", back_populates="cart_items")

class Order(Base):
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    total_price = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", back_populates="orders")
    items = relationship("OrderItem", back_populates="order")

class OrderItem(Base):
    __tablename__ = "order_items"
    
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("orders.id"))
    product_id = Column(Integer, ForeignKey("products.id"))
    quantity = Column(Integer)
    price = Column(Float)
    
    order = relationship("Order", back_populates="items")
    product = relationship("Product", back_populates="order_items")

# Index audit table for query analysis
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

# ===== Pydantic Models =====
class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    user: dict

class UserSignup(BaseModel):
    email: EmailStr
    password: str
    role: str = "user"

class UserLogin(BaseModel):
    email: str
    password: str

class UserResponse(BaseModel):
    id: int
    email: str
    role: str
    
    class Config:
        from_attributes = True

class ProductCreate(BaseModel):
    name: str
    price: float
    description: str
    image_url: Optional[str] = None

class ProductResponse(ProductCreate):
    id: int
    
    class Config:
        from_attributes = True

class CartItemRequest(BaseModel):
    product_id: int
    quantity: int

class CartItemResponse(BaseModel):
    id: int
    product: ProductResponse
    quantity: int
    
    class Config:
        from_attributes = True

class CartResponse(BaseModel):
    items: List[dict]

class ApplyIndexRequest(BaseModel):
    table: str
    column: str
    force: bool = False
    user: str = "admin"

# ===== FastAPI App =====
app = FastAPI(title="ShopHub API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== Database Setup =====
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
async def startup():
    Base.metadata.create_all(bind=engine)
    
    # Create audit table
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    try:
        cur.execute(AUDIT_TABLE_SQL)
        conn.commit()
    except Exception as e:
        print(f"Audit table error: {e}")
    finally:
        cur.close()
        conn.close()

# ===== Security Functions =====
def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    user = db.query(User).filter(User.email == email).first()
    if user is None:
        raise credentials_exception
    return user

# ===== Auth Endpoints =====
@app.post("/signup", response_model=TokenResponse)
def signup(user_data: UserSignup, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.email == user_data.email).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    hashed_password = hash_password(user_data.password)
    db_user = User(email=user_data.email, hashed_password=hashed_password, role=user_data.role)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    
    access_token = create_access_token(data={"sub": db_user.email})
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": db_user.id,
            "email": db_user.email,
            "role": db_user.role
        }
    }

@app.post("/login", response_model=TokenResponse)
def login(user_data: UserLogin, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.email == user_data.email).first()
    if not db_user or not verify_password(user_data.password, db_user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    access_token = create_access_token(data={"sub": db_user.email})
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": db_user.id,
            "email": db_user.email,
            "role": db_user.role
        }
    }

# ===== Product Endpoints =====
@app.get("/products", response_model=List[ProductResponse])
def get_products(db: Session = Depends(get_db)):
    products = db.query(Product).all()
    return products

@app.post("/products", response_model=ProductResponse)
def create_product(
    product: ProductCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")
    
    db_product = Product(**product.dict())
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    return db_product

@app.delete("/products/{product_id}")
def delete_product(
    product_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")
    
    db_product = db.query(Product).filter(Product.id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    db.delete(db_product)
    db.commit()
    return {"message": "Product deleted"}

# ===== Cart Endpoints =====
@app.get("/cart")
def get_cart(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    cart_items = db.query(CartItem).filter(CartItem.user_id == current_user.id).all()
    items = []
    for item in cart_items:
        items.append({
            "id": item.product.id,
            "name": item.product.name,
            "price": item.product.price,
            "description": item.product.description,
            "quantity": item.quantity
        })
    return {"items": items}

@app.post("/cart")
def add_to_cart(
    request: CartItemRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    # Check if product exists
    product = db.query(Product).filter(Product.id == request.product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Check if item already in cart
    cart_item = db.query(CartItem).filter(
        CartItem.user_id == current_user.id,
        CartItem.product_id == request.product_id
    ).first()
    
    if cart_item:
        cart_item.quantity += request.quantity
    else:
        cart_item = CartItem(
            user_id=current_user.id,
            product_id=request.product_id,
            quantity=request.quantity
        )
        db.add(cart_item)
    
    db.commit()
    return {"message": "Item added to cart"}

@app.put("/cart/{product_id}")
def update_cart_quantity(
    product_id: int,
    quantity: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    cart_item = db.query(CartItem).filter(
        CartItem.user_id == current_user.id,
        CartItem.product_id == product_id
    ).first()
    
    if not cart_item:
        raise HTTPException(status_code=404, detail="Item not in cart")
    
    cart_item.quantity = quantity
    db.commit()
    return {"message": "Quantity updated"}

@app.delete("/cart/{product_id}")
def remove_from_cart(
    product_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    cart_item = db.query(CartItem).filter(
        CartItem.user_id == current_user.id,
        CartItem.product_id == product_id
    ).first()
    
    if not cart_item:
        raise HTTPException(status_code=404, detail="Item not in cart")
    
    db.delete(cart_item)
    db.commit()
    return {"message": "Item removed from cart"}

# ===== Analysis Endpoints =====
@app.get("/recommendations")
def get_recommendations(current_user: User = Depends(get_current_user)):
    if not os.path.exists(RECS_FILE):
        return []
    with open(RECS_FILE, 'r') as f:
        recs = json.load(f)
    return recs

@app.get("/indexes")
def get_indexes(current_user: User = Depends(get_current_user)):
    """Get current list of indexes created by the system"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT 
                a.index_name,
                a.table_name,
                a.column_name,
                a.ts as created_at,
                a.user_name,
                COALESCE(pg_size_pretty(pg_relation_size(i.indexname::regclass)), 'N/A') as size
            FROM (
                SELECT DISTINCT ON (index_name)
                    index_name, table_name, column_name, ts, user_name
                FROM index_audit
                WHERE action = 'create'
                ORDER BY index_name, ts DESC
            ) a
            JOIN pg_indexes i ON i.indexname = a.index_name
            WHERE i.schemaname = 'public'
            ORDER BY a.ts DESC;
        """)
        indexes = []
        for row in cur.fetchall():
            indexes.append({
                "index_name": row[0],
                "table_name": row[1],
                "column_name": row[2],
                "created_at": row[3].isoformat() if row[3] else None,
                "user_name": row[4],
                "size": row[5] if row[5] else "N/A"
            })
        return indexes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

@app.post("/apply")
def apply_index(
    request: ApplyIndexRequest,
    current_user: User = Depends(get_current_user)
):
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")
    
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    try:
        index_name = f"idx_{request.table}_{request.column}"
        
        # Create index
        cur.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {request.table}({request.column})")
        conn.commit()
        
        # Log to audit table
        cur.execute("""
            INSERT INTO index_audit (action, index_name, table_name, column_name, user_name, details)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, ('create', index_name, request.table, request.column, request.user, {}))
        conn.commit()
        
        # Check max indexes (keep only 3)
        cur.execute("""
            SELECT index_name FROM index_audit 
            WHERE action = 'create' 
            ORDER BY ts DESC LIMIT 1 OFFSET 3
        """)
        result = cur.fetchone()
        deleted_index = None
        
        if result:
            old_index = result[0]
            cur.execute(f"DROP INDEX IF EXISTS {old_index}")
            cur.execute("""
                INSERT INTO index_audit (action, index_name, table_name, column_name, user_name, details)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, ('delete', old_index, request.table, request.column, request.user, {}))
            deleted_index = old_index
            conn.commit()
        
        return {
            "index": index_name,
            "deleted_index": deleted_index,
            "message": "Index applied successfully"
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

# Health check
@app.get("/health")
def health_check():
    return {"status": "ok"}
