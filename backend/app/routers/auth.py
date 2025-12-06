from fastapi import APIRouter, Depends, HTTPException, status, Response
from sqlalchemy.orm import Session
from ..database import get_db
from ..schemas.auth import SignupRequest, LoginRequest
from ..crud import auth as crud_auth
from ..utils.security import create_access_token
from ..config import settings

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/signup")
def signup(request: SignupRequest, db: Session = Depends(get_db)):
    """
    Create a new user account.
    """
    # Check if user already exists
    existing_user = crud_auth.get_user_by_email(db, request.email)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    # Create user
    user = crud_auth.create_user(
        db=db,
        email=request.email,
        password=request.password,
        name=request.name,
        city=request.city
    )
    
    return {"msg": "User created successfully", "user_id": user.id}


@router.post("/login")
def login(request: LoginRequest, response: Response, db: Session = Depends(get_db)):
    """
    Login and receive JWT token in HttpOnly cookie.
    """
    # Authenticate user
    user = crud_auth.authenticate_user(db, request.email, request.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )
    
    # Create access token
    access_token = create_access_token(data={"sub": str(user.id)})
    
    # Set cookie
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        max_age=settings.JWT_EXPIRATION_MINUTES * 60
    )
    print("response cookies after login:", response.headers.getlist('set-cookie'))  # Debug print
    
    return {"msg": "ok"}


@router.post("/logout")
def logout(response: Response):
    """
    Logout by deleting the token cookie.
    """
    response.delete_cookie(key="access_token")
    return {"msg": "Logged out successfully"}
