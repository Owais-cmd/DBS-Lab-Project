# backend/app/models.py
from sqlalchemy import (
    Column, Integer, String, Boolean, Numeric,
    ForeignKey, DateTime, Text, Enum, func
)
from sqlalchemy.orm import relationship, declarative_base
import datetime
import enum

Base = declarative_base()

# -------------------------
# ENUMS
# -------------------------
class OrderStatus(enum.Enum):
    cart = "cart"
    placed = "placed"
    cancelled = "cancelled"
    delivered = "delivered"

# -------------------------
# USER TABLE
# -------------------------
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(Text, unique=True, index=True, nullable=False)
    hashed_password = Column(Text, nullable=False)
    name = Column(Text, nullable=False)
    is_admin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    city = Column(Text, nullable=False)

    # relationship: ONE user → MANY orders
    orders = relationship("Order", back_populates="user")

# -------------------------
# ITEM TABLE
# -------------------------
class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(Text, nullable=False)
    description = Column(Text)
    price = Column(Numeric, default=0)
    category = Column(Text)
    image_url = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow,server_default=func.now())
    # one item appears in many order-items
    order_items = relationship("OrderItem", back_populates="item", passive_deletes=True)

# -------------------------
# ORDER TABLE
# -------------------------
class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    status = Column(Text, default="cart")  # cart, placed, cancelled, delivered
    total_amount = Column(Numeric, default=0)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    # relation back to user
    user = relationship("User", back_populates="orders")

    # one order → many order-items
    items = relationship("OrderItem", back_populates="order", passive_deletes=True)

# -------------------------
# ORDER-ITEM TABLE (line items)
# -------------------------
class OrderItem(Base):
    __tablename__ = "order_items"

    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    item_id = Column(
    Integer,
    ForeignKey("items.id", ondelete="CASCADE"),
    nullable=False
)

    quantity = Column(Integer, default=1)
    price = Column(Numeric)  # price at time of purchase

    # relation back to parent order
    order = relationship("Order", back_populates="items")

    # relation to actual item
    item = relationship("Item", back_populates="order_items")
