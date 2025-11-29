from sqlalchemy.orm import Session
from typing import Optional
from ..models import User
from ..utils.security import hash_password, verify_password


def create_user(db: Session, email: str, password: str, name: str, city: str, is_admin: bool = False) -> User:
    """Create a new user."""
    hashed_password = hash_password(password)
    db_user = User(
        email=email,
        hashed_password=hashed_password,
        name=name,
        city=city,
        is_admin=is_admin
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


def get_user_by_email(db: Session, email: str) -> Optional[User]:
    """Get user by email."""
    return db.query(User).filter(User.email == email).first()


def get_user_by_id(db: Session, user_id: int) -> Optional[User]:
    """Get user by ID."""
    return db.query(User).filter(User.id == user_id).first()


def authenticate_user(db: Session, email: str, password: str) -> Optional[User]:
    """Authenticate a user by email and password."""
    user = get_user_by_email(db, email)
    if not user:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user
