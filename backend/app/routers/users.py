from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from ..database import get_db
from ..schemas.user import UserResponse
from ..schemas.order import OrderResponse
from ..models import User
from ..utils.security import get_current_user
from ..crud import orders as crud_orders

router = APIRouter(prefix="/users", tags=["Users"])


@router.get("/me", response_model=UserResponse)
def get_current_user_profile(current_user: User = Depends(get_current_user)):
    """
    Get current user profile.
    """
    return current_user


@router.get("/me/orders", response_model=List[OrderResponse])
def get_my_orders(
    limit: int = 5,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get current user's orders (last N orders sorted by created_at DESC).
    """
    orders = crud_orders.get_user_orders(db, current_user.id, limit=limit)
    return orders
