from sqlalchemy.orm import Session
from typing import Optional, List
from ..models import Item
from sqlalchemy import func


def create_item(db: Session, name: str, description: Optional[str], price: float, category: Optional[str]) -> Item:
    """Create a new item."""
    db_item = Item(
        name=name,
        description=description,
        price=price,
        category=category
    )
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item


def get_item(db: Session, item_id: int) -> Optional[Item]:
    """Get item by ID."""
    return db.query(Item).filter(Item.id == item_id).first()


def get_items(
    db: Session, 
    search: Optional[str] = None,
    category: Optional[str] = None,
    skip: int = 0, 
    limit: int = 100
) -> List[Item]:
    """Get list of items with optional filters."""
    query = db.query(Item)
    
    if search:
        query = query.filter(Item.name.ilike(f"%{search}%"))
    
    if category:
        query = query.filter(Item.category == category)

    query = query.order_by(func.random())
    
    return query.offset(skip).limit(limit).all()


def update_item(db: Session, item_id: int, **kwargs) -> Optional[Item]:
    """Update item fields."""
    item = get_item(db, item_id)
    if not item:
        return None
    
    for key, value in kwargs.items():
        if hasattr(item, key) and value is not None:
            setattr(item, key, value)
    
    db.commit()
    db.refresh(item)
    return item


def delete_item(db: Session, item_id: int) -> bool:
    """Delete an item."""
    item = get_item(db, item_id)
    if not item:
        return False
    
    db.delete(item)
    db.commit()
    return True
