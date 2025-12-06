from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class ItemBase(BaseModel):
    name: str
    description: Optional[str] = None
    price: float
    category: Optional[str] = None
    image_url: Optional[str] = None


class ItemCreate(ItemBase):
    pass


class ItemUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    price: Optional[float] = None
    category: Optional[str] = None
    image_url: Optional[str] = None


class ItemResponse(ItemBase):
    id: int
    created_at: Optional[datetime]
    name: str
    description: Optional[str]
    price: float
    category: Optional[str]
    
    class Config:
        from_attributes = True
