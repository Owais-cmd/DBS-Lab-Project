from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List, Dict
from ..database import get_db
from ..models import User
from ..utils.security import require_admin
from ..services.metrics_manager import MetricsManager

router = APIRouter(prefix="/metrics", tags=["Metrics"])


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
