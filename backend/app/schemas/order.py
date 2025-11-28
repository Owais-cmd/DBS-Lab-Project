from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class OrderItemBase(BaseModel):
    item_id: int
    quantity: int
    price: Optional[float] = None


class OrderItemResponse(OrderItemBase):
    id: int
    order_id: int
    
    class Config:
        from_attributes = True


class AddToCartRequest(BaseModel):
    item_id: int
    quantity: int


class RemoveFromCartRequest(BaseModel):
    item_id: int


class OrderBase(BaseModel):
    status: str
    total_amount: float


class OrderResponse(OrderBase):
    id: int
    user_id: int
    created_at: datetime
    items: List[OrderItemResponse] = []
    
    class Config:
        from_attributes = True


class OrderCreate(BaseModel):
    items: List[OrderItemBase]
