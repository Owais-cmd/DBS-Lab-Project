from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from ..database import get_db
from ..schemas.item import ItemCreate, ItemResponse, ItemUpdate
from ..models import User
from ..utils.security import require_admin, get_current_user
from ..crud import items as crud_items

router = APIRouter(prefix="/items", tags=["Items"])


@router.get("", response_model=List[ItemResponse])
def list_items(
    search: Optional[str] = Query(None, description="Search items by name"),
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Get list of items with optional filters.
    """
    items = crud_items.get_items(
        db=db,
        search=search,
        category=category,
        skip=offset,
        limit=limit
    )
    return items


@router.get("/{item_id}", response_model=ItemResponse)
def get_item(item_id: int, db: Session = Depends(get_db)):
    """
    Get a specific item by ID.
    """
    item = crud_items.get_item(db, item_id)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found"
        )
    return item


@router.post("", response_model=ItemResponse, status_code=status.HTTP_201_CREATED)
def create_item(
    item: ItemCreate,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Create a new item (admin only).
    """
    db_item = crud_items.create_item(
        db=db,
        name=item.name,
        description=item.description,
        price=item.price,
        category=item.category
    )
    return db_item


@router.patch("/{item_id}", response_model=ItemResponse)
def update_item(
    item_id: int,
    item_update: ItemUpdate,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Update item fields (admin only).
    """
    updated_item = crud_items.update_item(
        db=db,
        item_id=item_id,
        name=item_update.name,
        description=item_update.description,
        price=item_update.price,
        category=item_update.category
    )
    
    if not updated_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found"
        )
    
    return updated_item


@router.delete("/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_item(
    item_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Delete an item (admin only).
    """
    success = crud_items.delete_item(db, item_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found"
        )
    return None
