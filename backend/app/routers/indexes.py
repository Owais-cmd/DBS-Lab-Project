from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import Session
from typing import List, Dict
from pydantic import BaseModel
from ..database import get_db
from ..models import User
from ..utils.security import require_admin
from ..services.index_manager import IndexManager

router = APIRouter(prefix="/indexes", tags=["Indexes"])


class CreateIndexRequest(BaseModel):
    table_name: str
    column_name: str
    index_type: str = "btree"


class CompareQueryRequest(BaseModel):
    query: str


@router.post("/apply", response_model=Dict)
def apply_index(
    request: CreateIndexRequest,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Create an index on a table column (admin only).
    
    Args:
        table_name: Name of the table
        column_name: Name of the column
        index_type: Type of index (btree, hash, gin, gist, etc.)
    
    Returns:
        Status and message
    """
    manager = IndexManager(db)
    result = manager.create_index(
        table_name=request.table_name,
        column_name=request.column_name,
        index_type=request.index_type
    )
    return result


@router.get("/list", response_model=List[Dict])
def list_indexes(
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    List all indexes in the database (admin only).
    
    Returns:
        List of indexes with schema, table, index_name, and definition
    """
    manager = IndexManager(db)
    return manager.list_indexes()


@router.post("/comparison/{index_name}", response_model=Dict)
def compare_query_plans(
    index_name: str,
    request: CompareQueryRequest,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Compare query execution plans with and without an index (admin only).
    
    Args:
        index_name: Name of the index to test
        query: SQL query to analyze
    
    Returns:
        Execution plans with and without the index
    """
    manager = IndexManager(db)
    result = manager.compare_query_plans(
        index_name=index_name,
        query=request.query
    )
    return result


@router.delete("/drop/{index_name}", response_model=Dict)
def drop_index(
    index_name: str,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Drop an index (admin only).
    """
    manager = IndexManager(db)
    result = manager.drop_index(index_name)
    return result
