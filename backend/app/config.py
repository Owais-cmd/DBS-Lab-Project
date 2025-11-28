from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql://demo:demo@localhost/demo"
    
    # JWT
    JWT_SECRET: str = "your-secret-key-change-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_MINUTES: int = 60
    
    # App
    APP_NAME: str = "Adaptive Ordering System"
    DEBUG: bool = True
    
    # Cookie
    COOKIE_SECURE: bool = False  # Set True in production
    COOKIE_SAMESITE: str = "lax"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
