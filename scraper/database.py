"""Database operations module."""

import logging
import time
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional, Tuple
from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extensions import connection as Connection

from .exceptions import DatabaseError, ValidationError
from .settings import DatabaseSettings

logger = logging.getLogger(__name__)

class QueryBuilder:
    """SQL query builder with parameter binding."""
    
    def __init__(self):
        """Initialize query builder."""
        self.query_parts = []
        self.params = []

    def add(self, part: str, param: Any = None):
        """Add a query part and optional parameter."""
        self.query_parts.append(part)
        if param is not None:
            self.params.append(param)
        return self

    def build(self) -> Tuple[str, tuple]:
        """Build final query and parameters."""
        return " ".join(self.query_parts), tuple(self.params)

class Database:
    """Database connection and operations handler."""
    
    MAX_RETRIES = 3
    RETRY_DELAY = 1  # seconds
    
    def __init__(self, settings: DatabaseSettings):
        """Initialize database connection pool."""
        self.settings = settings
        self.pool = None
        self._create_connection_pool()
        self._initialize_database()

    def _create_connection_pool(self) -> None:
        """Create database connection pool with retry logic."""
        retries = 0
        while retries < self.MAX_RETRIES:
            try:
                self.pool = SimpleConnectionPool(
                    minconn=1,
                    maxconn=self.settings.pool_size,
                    host=self.settings.host,
                    port=self.settings.port,
                    dbname=self.settings.name,
                    user=self.settings.user,
                    password=self.settings.password,
                    cursor_factory=DictCursor,
                    # Configuración adicional para mejor rendimiento
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5
                )
                return
            except psycopg2.Error as e:
                retries += 1
                if retries == self.MAX_RETRIES:
                    raise DatabaseError(
                        "Failed to create database connection pool after multiple attempts",
                        details={'error': str(e), 'attempts': retries}
                    )
                logger.warning(f"Connection attempt {retries} failed, retrying in {self.RETRY_DELAY}s...")
                time.sleep(self.RETRY_DELAY)

    def _initialize_database(self) -> None:
        """Initialize database schema and indexes."""
        self.create_tables()
        self._create_additional_indexes()

    def _create_additional_indexes(self) -> None:
        """Create additional indexes for better query performance."""
        indexes = [
            """
            CREATE INDEX IF NOT EXISTS idx_businesses_dates_composite 
            ON businesses(nsl_published_date, nsl_effective_date, remediated_date);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_businesses_name_trgm 
            ON businesses USING gin(business_name gin_trgm_ops);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_businesses_address_trgm 
            ON businesses USING gin(address gin_trgm_ops);
            """
        ]
        
        for index in indexes:
            try:
                self.execute(index)
            except Exception as e:
                logger.warning(f"Failed to create index: {e}")

    def _validate_business_data(self, business_data: Dict[str, Any]) -> None:
        """Validate business data before database operations."""
        required_fields = {
            'business_name', 'address', 'city', 'state', 'zip_code',
            'violation_type', 'nsl_published_date', 'nsl_effective_date'
        }
        
        # Validar campos requeridos
        missing_fields = required_fields - business_data.keys()
        if missing_fields:
            raise ValidationError(
                "Missing required business data fields",
                details={'missing_fields': list(missing_fields)}
            )
        
        # Validar formato de estado
        if not isinstance(business_data['state'], str) or len(business_data['state']) != 2:
            raise ValidationError(
                "Invalid state format",
                details={'state': business_data['state']}
            )
        
        # Validar código postal
        if not isinstance(business_data['zip_code'], str) or not business_data['zip_code'].isdigit():
            raise ValidationError(
                "Invalid zip code format",
                details={'zip_code': business_data['zip_code']}
            )
        
        # Validar fechas
        date_fields = ['nsl_published_date', 'nsl_effective_date', 'remediated_date']
        for field in date_fields:
            if field in business_data and business_data[field]:
                try:
                    if isinstance(business_data[field], str):
                        datetime.strptime(business_data[field], '%Y-%m-%d')
                except ValueError as e:
                    raise ValidationError(
                        f"Invalid date format for {field}",
                        details={'field': field, 'value': business_data[field]}
                    )

    @contextmanager
    def get_connection(self) -> Generator[Connection, None, None]:
        """Get a connection from the pool with automatic cleanup."""
        conn = None
        try:
            conn = self.pool.getconn()
            yield conn
            conn.commit()
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            raise DatabaseError(
                "Database operation failed",
                details={'error': str(e)}
            )
        finally:
            if conn:
                self.pool.putconn(conn)

    def execute(self, query: str, params: Optional[tuple] = None, retries: int = MAX_RETRIES) -> None:
        """Execute a query without returning results, with retry logic."""
        last_error = None
        for attempt in range(retries):
            try:
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(query, params)
                return
            except psycopg2.Error as e:
                last_error = e
                if attempt < retries - 1:
                    logger.warning(f"Query attempt {attempt + 1} failed, retrying in {self.RETRY_DELAY}s...")
                    time.sleep(self.RETRY_DELAY)
        
        raise DatabaseError(
            "Query execution failed after multiple attempts",
            details={'error': str(last_error), 'attempts': retries}
        )

    def fetch_one(self, query: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
        """Fetch a single row from the database."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                return dict(row) if row else None

    def fetch_all(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Fetch all rows from the database."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                return [dict(row) for row in rows]

    def insert_business(self, business_data: Dict[str, Any]) -> int:
        """Insert business data and return the ID."""
        self._validate_business_data(business_data)
        
        query = QueryBuilder()
        query.add("""
            INSERT INTO businesses (
                business_name, address, city, state, zip_code,
                violation_type, nsl_published_date, nsl_effective_date,
                remediated_date, verified, created_at
            ) VALUES (
                %(business_name)s, %(address)s, %(city)s, %(state)s, %(zip_code)s,
                %(violation_type)s, %(nsl_published_date)s, %(nsl_effective_date)s,
                %(remediated_date)s, %(verified)s, NOW()
            ) ON CONFLICT (business_name, address) 
            DO UPDATE SET 
                updated_at = NOW(),
                violation_type = EXCLUDED.violation_type,
                nsl_published_date = EXCLUDED.nsl_published_date,
                nsl_effective_date = EXCLUDED.nsl_effective_date,
                remediated_date = EXCLUDED.remediated_date,
                verified = EXCLUDED.verified
            RETURNING id;
        """)
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(*query.build(), business_data)
                    return cur.fetchone()[0]
        except psycopg2.IntegrityError as e:
            raise DatabaseError(
                "Duplicate business entry",
                details={'error': str(e), 'business': business_data}
            )

    def update_business(self, business_id: int, business_data: Dict[str, Any]) -> None:
        """Update business data."""
        self._validate_business_data(business_data)
        
        query = QueryBuilder()
        query.add("""
            UPDATE businesses SET
                business_name = %(business_name)s,
                address = %(address)s,
                city = %(city)s,
                state = %(state)s,
                zip_code = %(zip_code)s,
                violation_type = %(violation_type)s,
                nsl_published_date = %(nsl_published_date)s,
                nsl_effective_date = %(nsl_effective_date)s,
                remediated_date = %(remediated_date)s,
                verified = %(verified)s,
                updated_at = NOW()
            WHERE id = %(id)s;
        """)
        
        business_data['id'] = business_id
        self.execute(*query.build(), business_data)

    def get_business_by_id(self, business_id: int) -> Optional[Dict[str, Any]]:
        """Get business by ID."""
        query = QueryBuilder()
        query.add("SELECT * FROM businesses WHERE id = %s;", business_id)
        return self.fetch_one(*query.build())

    def get_businesses_by_state(self, state: str) -> List[Dict[str, Any]]:
        """Get all businesses in a state."""
        query = QueryBuilder()
        query.add(
            "SELECT * FROM businesses WHERE state = %s ORDER BY business_name;",
            state
        )
        return self.fetch_all(*query.build())

    def get_unverified_businesses(self) -> List[Dict[str, Any]]:
        """Get all unverified businesses."""
        query = QueryBuilder()
        query.add(
            "SELECT * FROM businesses WHERE NOT verified ORDER BY nsl_published_date DESC;"
        )
        return self.fetch_all(*query.build())

    def get_businesses_with_violations(
        self,
        violation_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get businesses with violations matching criteria."""
        query = QueryBuilder()
        query.add("SELECT * FROM businesses WHERE 1=1")
        
        if violation_type:
            query.add("AND violation_type = %s", violation_type)
        
        if start_date:
            query.add("AND nsl_published_date >= %s", start_date)
        
        if end_date:
            query.add("AND nsl_published_date <= %s", end_date)
        
        query.add("ORDER BY nsl_published_date DESC;")
        
        return self.fetch_all(*query.build())

    def create_tables(self) -> None:
        """Create database tables if they don't exist."""
        queries = [
            """
            CREATE EXTENSION IF NOT EXISTS pg_trgm;
            """,
            """
            CREATE TABLE IF NOT EXISTS businesses (
                id SERIAL PRIMARY KEY,
                business_name VARCHAR(200) NOT NULL,
                address VARCHAR(200) NOT NULL,
                city VARCHAR(100) NOT NULL,
                state CHAR(2) NOT NULL,
                zip_code VARCHAR(10) NOT NULL,
                violation_type VARCHAR(100) NOT NULL,
                nsl_published_date DATE NOT NULL,
                nsl_effective_date DATE NOT NULL,
                remediated_date DATE,
                verified BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP,
                UNIQUE(business_name, address)
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_businesses_state 
            ON businesses(state);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_businesses_verified 
            ON businesses(verified);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_businesses_dates 
            ON businesses(nsl_published_date, nsl_effective_date);
            """
        ]
        
        for query in queries:
            self.execute(query)

    def close(self) -> None:
        """Close all database connections."""
        if self.pool:
            self.pool.closeall() 