from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List, Dict
from ..database import get_db
from ..models import User, Order, Item
from ..utils.security import require_admin
from ..services.metrics_manager import MetricsManager
from sqlalchemy import func, desc

router = APIRouter(prefix="/metrics", tags=["Metrics"])


@router.get("", response_model=Dict)
def get_all_metrics(
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Get comprehensive metrics dashboard data (admin only).
    """
    metrics = MetricsManager(db)
    
    # Get basic counts
    total_users = db.query(func.count(User.id)).scalar()
    total_items = db.query(func.count(Item.id)).scalar()
    total_orders = db.query(func.count(Order.id)).filter(Order.status != "cart").scalar()
    total_revenue = db.query(func.sum(Order.total_amount)).filter(Order.status == "placed").scalar() or 0
    
    # Get top customers
    top_customers = db.query(
        User.id.label('user_id'),
        User.name.label('user_name'),
        User.email.label('user_email'),
        func.sum(Order.total_amount).label('total_spent'),
        func.count(Order.id).label('order_count')
    ).join(Order, User.id == Order.user_id
    ).filter(Order.status == "placed"
    ).group_by(User.id, User.name, User.email
    ).order_by(desc('total_spent')
    ).limit(10).all()
    
    # Get recent orders
    recent_orders = db.query(
        Order.id.label('order_id'),
        User.name.label('user_name'),
        User.email.label('user_email'),
        Order.total_amount,
        Order.status,
        Order.created_at
    ).join(User, Order.user_id == User.id
    ).filter(Order.status != "cart"
    ).order_by(desc(Order.created_at)
    ).limit(10).all()
    
    return {
        "total_users": total_users,
        "total_items": total_items,
        "total_orders": total_orders,
        "total_revenue": float(total_revenue),
        "top_customers": [
            {
                "user_id": c.user_id,
                "user_name": c.user_name,
                "user_email": c.user_email,
                "total_spent": float(c.total_spent),
                "order_count": c.order_count
            }
            for c in top_customers
        ],
        "recent_orders": [
            {
                "order_id": o.order_id,
                "user_name": o.user_name,
                "user_email": o.user_email,
                "total_amount": float(o.total_amount),
                "status": o.status,
                "created_at": o.created_at.isoformat() if o.created_at else None
            }
            for o in recent_orders
        ]
    }


@router.get("/most-ordered", response_model=List[Dict])
def get_most_ordered_items(
    limit: int = 10,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Get most ordered items with total quantity (admin only).
    
    Returns:
        List of items with item_id, name, category, total_quantity, order_count
    """
    metrics = MetricsManager(db)
    return metrics.get_most_ordered_items(limit=limit)


@router.get("/expensive-orders", response_model=List[Dict])
def get_expensive_orders(
    limit: int = 10,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Get orders sorted by total_amount DESC (admin only).
    
    Returns:
        List of high-value orders
    """
    metrics = MetricsManager(db)
    return metrics.get_expensive_orders(limit=limit)


@router.get("/category-stats", response_model=List[Dict])
def get_category_statistics(
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Get sales statistics by category (admin only).
    """
    metrics = MetricsManager(db)
    return metrics.get_category_statistics()


@router.get("/top-customers", response_model=List[Dict])
def get_top_customers(
    limit: int = 10,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Get top customers by total spent (admin only).
    """
    metrics = MetricsManager(db)
    return metrics.get_top_customers(limit=limit)


@router.get("/user/{user_id}", response_model=Dict)
def get_user_statistics(
    user_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Get statistics for a specific user (admin only).
    """
    metrics = MetricsManager(db)
    return metrics.get_user_statistics(user_id)
