from sqlalchemy.orm import Session
from sqlalchemy import text, func, desc
from typing import List, Dict
from ..models import Order, OrderItem, Item, User


class MetricsManager:
    """Manage business metrics and analytics."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_most_ordered_items(self, limit: int = 10) -> List[Dict]:
        """
        Get most ordered items with total quantity.
        
        Returns:
            List of items with order counts
        """
        results = self.db.query(
            Item.id,
            Item.name,
            Item.category,
            func.sum(OrderItem.quantity).label("total_quantity"),
            func.count(OrderItem.id).label("order_count")
        ).join(
            OrderItem, Item.id == OrderItem.item_id
        ).join(
            Order, OrderItem.order_id == Order.id
        ).filter(
            Order.status == "placed"
        ).group_by(
            Item.id, Item.name, Item.category
        ).order_by(
            desc("total_quantity")
        ).limit(limit).all()
        
        return [
            {
                "item_id": r.id,
                "name": r.name,
                "category": r.category,
                "total_quantity": int(r.total_quantity),
                "order_count": int(r.order_count)
            }
            for r in results
        ]
    
    def get_expensive_orders(self, limit: int = 10) -> List[Dict]:
        """
        Get orders sorted by total amount (descending).
        
        Returns:
            List of high-value orders
        """
        results = self.db.query(Order).filter(
            Order.status != "cart"
        ).order_by(desc(Order.total_amount)).limit(limit).all()
        
        return [
            {
                "order_id": order.id,
                "user_id": order.user_id,
                "status": order.status,
                "total_amount": float(order.total_amount),
                "created_at": order.created_at.isoformat() if order.created_at else None,
                "item_count": len(order.items)
            }
            for order in results
        ]
    
    def get_user_statistics(self, user_id: int) -> Dict:
        """Get statistics for a specific user."""
        # Total orders
        total_orders = self.db.query(func.count(Order.id)).filter(
            Order.user_id == user_id,
            Order.status != "cart"
        ).scalar()
        
        # Total spent
        total_spent = self.db.query(func.sum(Order.total_amount)).filter(
            Order.user_id == user_id,
            Order.status == "placed" or Order.status == "delivered"
        ).scalar() or 0
        
        # Most ordered category
        category_results = self.db.query(
            Item.category,
            func.count(OrderItem.id).label("count")
        ).join(
            OrderItem, Item.id == OrderItem.item_id
        ).join(
            Order, OrderItem.order_id == Order.id
        ).filter(
            Order.user_id == user_id,
            Order.status == "placed" or Order.status == "delivered"
        ).group_by(
            Item.category
        ).order_by(
            desc("count")
        ).first()
        
        favorite_category = category_results.category if category_results else None
        
        return {
            "user_id": user_id,
            "total_orders": total_orders,
            "total_spent": float(total_spent),
            "favorite_category": favorite_category
        }
    
    def get_category_statistics(self) -> List[Dict]:
        """Get sales statistics by category."""
        results = self.db.query(
            Item.category,
            func.count(OrderItem.id).label("total_orders"),
            func.sum(OrderItem.quantity).label("total_quantity"),
            func.sum(OrderItem.price * OrderItem.quantity).label("total_revenue")
        ).join(
            OrderItem, Item.id == OrderItem.item_id
        ).join(
            Order, OrderItem.order_id == Order.id
        ).filter(
            Order.status == "placed"
        ).group_by(
            Item.category
        ).order_by(
            desc("total_revenue")
        ).all()
        
        return [
            {
                "category": r.category or "Uncategorized",
                "total_orders": int(r.total_orders),
                "total_quantity": int(r.total_quantity),
                "total_revenue": float(r.total_revenue)
            }
            for r in results
        ]
    
    def get_top_customers(self, limit: int = 10) -> List[Dict]:
        """Get top customers by total spent."""
        results = self.db.query(
            User.id,
            User.name,
            User.email,
            User.city,
            func.count(Order.id).label("order_count"),
            func.sum(Order.total_amount).label("total_spent")
        ).join(
            Order, User.id == Order.user_id
        ).filter(
            Order.status == "placed"
        ).group_by(
            User.id, User.name, User.email, User.city
        ).order_by(
            desc("total_spent")
        ).limit(limit).all()
        
        return [
            {
                "user_id": r.id,
                "name": r.name,
                "email": r.email,
                "city": r.city,
                "order_count": int(r.order_count),
                "total_spent": float(r.total_spent)
            }
            for r in results
        ]
