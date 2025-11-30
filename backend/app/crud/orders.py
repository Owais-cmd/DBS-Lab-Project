from sqlalchemy.orm import Session
from sqlalchemy import desc
from typing import Optional, List
from ..models import Order, OrderItem, Item


def get_or_create_cart(db: Session, user_id: int) -> Order:
    """Get existing cart or create new one for user."""
    cart = db.query(Order).filter(
        Order.user_id == user_id,
        Order.status == "cart"
    ).first()
    
    if not cart:
        cart = Order(user_id=user_id, status="cart", total_amount=0)
        db.add(cart)
        db.commit()
        db.refresh(cart)
    
    return cart


def add_item_to_cart(db: Session, user_id: int, item_id: int, quantity: int) -> Order:
    """Add item to user's cart."""
    cart = get_or_create_cart(db, user_id)
    
    # Check if item already in cart
    existing = db.query(OrderItem).filter(
        OrderItem.order_id == cart.id,
        OrderItem.item_id == item_id
    ).first()
    
    if existing:
        # Update quantity
        existing.quantity += quantity
    else:
        # Add new item
        # Get item price
        item = db.query(Item).filter(Item.id == item_id).first()
        if not item:
            raise ValueError("Item not found")
        
        order_item = OrderItem(
            order_id=cart.id,
            item_id=item_id,
            quantity=quantity,
            price=item.price
        )
        db.add(order_item)
    
    db.commit()
    db.refresh(cart)
    return cart


def remove_item_from_cart(db: Session, user_id: int, item_id: int) -> Optional[Order]:
    """Remove item from user's cart."""
    cart = get_or_create_cart(db, user_id)
    
    order_item = db.query(OrderItem).filter(
        OrderItem.order_id == cart.id,
        OrderItem.item_id == item_id
    ).first()
    
    if order_item:
        db.delete(order_item)
        db.commit()
        db.refresh(cart)
    
    return cart


def update_cart_item(db: Session, user_id: int, item_id: int, quantity: int) -> Optional[Order]:
    """Update quantity of item in cart."""
    cart = get_or_create_cart(db, user_id)
    
    order_item = db.query(OrderItem).filter(
        OrderItem.order_id == cart.id,
        OrderItem.item_id == item_id
    ).first()
    
    if order_item:
        if quantity <= 0:
            db.delete(order_item)
        else:
            order_item.quantity = quantity
        db.commit()
        db.refresh(cart)
    
    return cart


def place_order(db: Session, user_id: int) -> Optional[Order]:
    """Convert cart to placed order."""
    cart = db.query(Order).filter(
        Order.user_id == user_id,
        Order.status == "cart"
    ).first()
    
    if not cart or not cart.items:
        return None
    
    # Calculate total amount
    total = sum(float(item.price * item.quantity) for item in cart.items)
    
    # Update order
    cart.status = "placed"
    cart.total_amount = total
    
    db.commit()
    db.refresh(cart)
    return cart


def get_order(db: Session, order_id: int) -> Optional[Order]:
    """Get order by ID."""
    return db.query(Order).filter(Order.id == order_id).first()


def get_user_orders(db: Session, user_id: int, limit: int = 5) -> List[Order]:
    """Get user's orders sorted by created_at DESC."""
    return db.query(Order).filter(
        Order.user_id == user_id,
        Order.status != "cart"  # Exclude cart
    ).order_by(desc(Order.created_at)).limit(limit).all()


def cancel_order(db: Session, order_id: int, user_id: int) -> Optional[Order]:
    """Cancel an order."""
    order = db.query(Order).filter(
        Order.id == order_id,
        Order.user_id == user_id
    ).first()
    
    if not order or order.status == "delivered":
        return None
    
    order.status = "cancelled"
    db.commit()
    db.refresh(order)
    return order


def mark_order_delivered(db: Session, order_id: int) -> Optional[Order]:
    """Mark order as delivered (admin only)."""
    order = get_order(db, order_id)
    if not order:
        return None
    
    order.status = "delivered"
    db.commit()
    db.refresh(order)
    return order


def get_most_ordered_items(db: Session, limit: int = 10):
    """Get most ordered items with total quantity."""
    from sqlalchemy import func
    
    results = db.query(
        Item.id,
        Item.name,
        func.sum(OrderItem.quantity).label("total_quantity")
    ).join(OrderItem, Item.id == OrderItem.item_id
    ).join(Order, OrderItem.order_id == Order.id
    ).filter(Order.status == "placed"
    ).group_by(Item.id, Item.name
    ).order_by(desc("total_quantity")
    ).limit(limit).all()
    
    return [
        {"item_id": r.id, "name": r.name, "total_quantity": int(r.total_quantity)}
        for r in results
    ]


def get_expensive_orders(db: Session, limit: int = 10) -> List[Order]:
    """Get orders sorted by total_amount DESC."""
    return db.query(Order).filter(
        Order.status != "cart"
    ).order_by(desc(Order.total_amount)).limit(limit).all()
