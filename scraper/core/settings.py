"""Core settings for the scraper."""

import os
from pydantic import BaseModel, Field, field_validator, computed_field


class BaseSettings(BaseModel):
    """Base settings class."""

    class Config:
        arbitrary_types_allowed = True


class DatabaseSettings(BaseModel):
    """Database connection settings."""

    enabled: bool = Field(default=True)
    type: str = Field(default="postgresql")
    host: str = Field(default=os.getenv("DB_HOST", "db"))
    port: int = Field(default=int(os.getenv("DB_PORT", "5432")))
    name: str = Field(default=os.getenv("DB_NAME", "business_scraper"))
    user: str = Field(default=os.getenv("DB_USER", "postgres"))
    password: str = Field(default=os.getenv("DB_PASSWORD", "devpassword123"))
    pool_size: int = Field(default=int(os.getenv("DB_POOL_SIZE", "5")))
    max_overflow: int = Field(default=10)
    timeout: int = Field(default=30)

    @field_validator("type")
    @classmethod
    def validate_db_type(cls, v: str) -> str:
        """Validate database type."""
        valid_types = ["postgresql", "mysql"]
        if v not in valid_types:
            raise ValueError(f"Database type must be one of {valid_types}")
        return v

    @computed_field
    def connection_string(self) -> str:
        """Get database connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"
