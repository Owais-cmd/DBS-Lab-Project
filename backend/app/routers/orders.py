from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from ..database import get_db
from ..schemas.order import AddToCartRequest, RemoveFromCartRequest, UpdateCartRequest, OrderResponse
from ..models import User
from ..utils.security import get_current_user, require_admin
from ..crud import orders as crud_orders

router = APIRouter(tags=["Orders"])


@router.get("/cart", response_model=OrderResponse)
def get_cart(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get current user's cart.
    """
    cart = crud_orders.get_or_create_cart(db=db, user_id=current_user.id)
    return cart


@router.post("/cart/update", response_model=OrderResponse)
def update_cart(
    request: UpdateCartRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Update item quantity in cart.
    """
    cart = crud_orders.update_cart_item(
        db=db,
        user_id=current_user.id,
        item_id=request.item_id,
        quantity=request.quantity
    )
    return cart


@router.post("/cart/add", response_model=OrderResponse)
def add_to_cart(
    request: AddToCartRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Add item to cart. If no cart exists, creates one.
    If item already in cart, updates quantity.
    """
    try:
        cart = crud_orders.add_item_to_cart(
            db=db,
            user_id=current_user.id,
            item_id=request.item_id,
            quantity=request.quantity
        )
        return cart
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.post("/cart/remove", response_model=OrderResponse)
def remove_from_cart(
    request: RemoveFromCartRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Remove item from cart.
    """
    cart = crud_orders.remove_item_from_cart(
        db=db,
        user_id=current_user.id,
        item_id=request.item_id
    )
    return cart


@router.post("/orders/place", response_model=OrderResponse)
def place_order(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Convert cart to placed order.
    - Calculates total_amount
    - Sets each OrderItem.price from current Item.price
    - Sets status to 'placed'
    """
    order = crud_orders.place_order(db=db, user_id=current_user.id)
    
    if not order:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No items in cart or cart not found"
        )
    
    return order


@router.delete("/orders/{order_id}", status_code=status.HTTP_200_OK)
def cancel_order(
    order_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Cancel an order (set status to cancelled).
    Only allowed if:
    - User is the owner
    - Order status is not 'delivered'
    """
    order = crud_orders.cancel_order(db=db, order_id=order_id, user_id=current_user.id)
    
    if not order:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot cancel this order (not found, not yours, or already delivered)"
        )
    
    return {"msg": "Order cancelled", "order_id": order.id}


@router.patch("/orders/{order_id}/delivered", response_model=OrderResponse)
def mark_delivered(
    order_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Mark an order as delivered (admin only).
    """
    order = crud_orders.mark_order_delivered(db=db, order_id=order_id)
    
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found"
        )
    
    return order
