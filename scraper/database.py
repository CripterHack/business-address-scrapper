"""Database module for the scraper."""

from typing import Any, Dict, List, Optional, Type, TypeVar, cast
import logging
from contextlib import contextmanager

from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    JSON,
    text,
    inspect,
)
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from .exceptions import DatabaseError

logger = logging.getLogger(__name__)

# Configuración base de SQLAlchemy
Base = declarative_base()
metadata = MetaData()


class DatabaseSettings:
    """Database connection settings."""

    def __init__(
        self,
        host: str = "db",
        port: int = 5432,
        name: str = "scraper",
        user: str = "postgres",
        password: str = "postgres",
        pool_size: int = 5,
        max_overflow: int = 10,
        timeout: int = 30,
    ):
        """Initialize database settings."""
        self.host = host
        self.port = port
        self.name = name
        self.user = user
        self.password = password
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.timeout = timeout

    @property
    def connection_string(self) -> str:
        """Get database connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class Database:
    """Database connection manager."""

    def __init__(self, settings: DatabaseSettings):
        """Initialize database connection."""
        self.settings = settings
        self.engine = None
        self.Session = None
        self._initialize_connection()

    def _initialize_connection(self) -> None:
        """Initialize database connection with retry logic."""
        try:
            self.engine = self._create_engine()
            self.Session = sessionmaker(bind=self.engine)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except OperationalError as e:
            logger.error(f"Failed to connect to database: {e}")
            raise DatabaseError(f"Database connection failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error connecting to database: {e}")
            raise DatabaseError(f"Database initialization failed: {e}")

    def _create_engine(self) -> Engine:
        """Create database engine."""
        try:
            return create_engine(
                self.settings.connection_string,
                poolclass=QueuePool,
                pool_size=self.settings.pool_size,
                max_overflow=self.settings.max_overflow,
                pool_timeout=self.settings.timeout,
                pool_pre_ping=True,
            )
        except Exception as e:
            raise DatabaseError(f"Error creating database engine: {e}")

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        if not self.Session:
            raise DatabaseError("Database session not initialized")

        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise DatabaseError(f"Database session error: {e}")
        finally:
            session.close()

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results."""
        with self.session_scope() as session:
            try:
                result = session.execute(text(query), params or {})
                return [dict(row._mapping) for row in result]
            except SQLAlchemyError as e:
                raise DatabaseError(f"Error executing query: {e}")

    def fetch_all(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch all results from a SQL query."""
        return self.execute(query, params)

    def fetch_one(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single result from a SQL query."""
        results = self.execute(query, params)
        return results[0] if results else None

    def create_tables(self) -> None:
        """Create all database tables."""
        try:
            Base.metadata.create_all(self.engine)
        except SQLAlchemyError as e:
            raise DatabaseError(f"Error creating database tables: {e}")

    def drop_tables(self) -> None:
        """Drop all database tables."""
        try:
            Base.metadata.drop_all(self.engine)
        except SQLAlchemyError as e:
            raise DatabaseError(f"Error dropping database tables: {e}")

    def get_table_names(self) -> List[str]:
        """Get all table names in the database."""
        try:
            inspector = inspect(self.engine)
            return inspector.get_table_names()
        except SQLAlchemyError as e:
            raise DatabaseError(f"Error getting table names: {e}")

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        return table_name in self.get_table_names()

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get the schema of a table."""
        if not self.table_exists(table_name):
            raise DatabaseError(f"Table {table_name} does not exist")

        try:
            table = Table(table_name, metadata, autoload_with=self.engine)
            return {
                "columns": {
                    col.name: {
                        "type": str(col.type),
                        "nullable": col.nullable,
                        "primary_key": col.primary_key,
                    }
                    for col in table.columns
                }
            }
        except SQLAlchemyError as e:
            raise DatabaseError(f"Error getting table schema: {e}")

    def vacuum_analyze(self, table_name: Optional[str] = None) -> None:
        """Run VACUUM ANALYZE on the database or a specific table."""
        if not self.engine:
            raise DatabaseError("Database engine not initialized")

        with self.engine.connect() as conn:
            try:
                if conn.in_transaction():
                    conn.execute(text("COMMIT"))
                if table_name:
                    conn.execute(text(f"VACUUM ANALYZE {table_name}"))
                else:
                    conn.execute(text("VACUUM ANALYZE"))
            except SQLAlchemyError as e:
                raise DatabaseError(f"Error running VACUUM ANALYZE: {e}")

    def get_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        if not self.table_exists(table_name):
            raise DatabaseError(f"Table {table_name} does not exist")

        try:
            with self.session_scope() as session:
                result = session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
                return cast(int, count) if count is not None else 0
        except SQLAlchemyError as e:
            raise DatabaseError(f"Error getting row count: {e}")

    def get_database_size(self) -> Dict[str, Any]:
        """Get the size of the database and its tables."""
        try:
            with self.session_scope() as session:
                # Get total database size
                total_size_query = text(
                    """
                SELECT pg_size_pretty(pg_database_size(current_database())) as size,
                       pg_database_size(current_database()) as bytes
                """
                )
                total_size = dict(session.execute(total_size_query).fetchone()._mapping)

                # Get size of each table
                table_size_query = text(
                    """
                SELECT relname as table,
                       pg_size_pretty(pg_total_relation_size(C.oid)) as size,
                       pg_total_relation_size(C.oid) as bytes
                FROM pg_class C
                LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
                WHERE nspname NOT IN ('pg_catalog', 'information_schema')
                AND C.relkind <> 'i'
                AND nspname !~ '^pg_toast'
                ORDER BY pg_total_relation_size(C.oid) DESC
                """
                )
                tables = [dict(row._mapping) for row in session.execute(table_size_query)]

                return {"total": total_size, "tables": tables}
        except SQLAlchemyError as e:
            raise DatabaseError(f"Error getting database size: {e}")


T = TypeVar("T", bound=Base)


def get_or_create(
    session: Any, model: Type[T], defaults: Optional[Dict[str, Any]] = None, **kwargs
) -> tuple[T, bool]:
    """Get an instance of a model or create it if it doesn't exist."""
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        params = dict((k, v) for k, v in kwargs.items())
        params.update(defaults or {})
        instance = model(**params)
        try:
            session.add(instance)
            session.commit()
            return instance, True
        except Exception:
            session.rollback()
            raise
