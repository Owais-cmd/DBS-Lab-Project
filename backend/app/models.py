# backend/app/models.py
from sqlalchemy import (
    Column, Integer, String, Boolean, Numeric,
    ForeignKey, DateTime, Text 
)
from sqlalchemy.orm import relationship, declarative_base
import datetime

Base = declarative_base()

# -------------------------
# USER TABLE
# -------------------------
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(Text, unique=True, index=True, nullable=False)
    hashed_password = Column(Text, nullable=False)
    is_admin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    city = Column(Text, nullable=False)
    age = Column(Integer, nullable=False)

    # relationship: ONE user → MANY orders
    orders = relationship("Order", back_populates="user")

# -------------------------
# ITEM TABLE
# -------------------------
class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(Text, nullable=False)
    price = Column(Numeric, default=0)
    category = Column(Text)
    # one item appears in many order-items
    order_items = relationship("OrderItem", back_populates="item")

# -------------------------
# ORDER TABLE
# -------------------------
class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    status = Column(Text, default="placed")  # placed, canceled, delivered
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    # relation back to user
    user = relationship("User", back_populates="orders")

    # one order → many order-items
    items = relationship("OrderItem", back_populates="order")

# -------------------------
# ORDER-ITEM TABLE (line items)
# -------------------------
class OrderItem(Base):
    __tablename__ = "order_items"

    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False)

    quantity = Column(Integer, default=1)
    price = Column(Numeric)  # price at time of purchase

    # relation back to parent order
    order = relationship("Order", back_populates="items")

    # relation to actual item
    item = relationship("Item", back_populates="order_items")
