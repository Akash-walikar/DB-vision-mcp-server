"""
Database connection implementations.

This package provides database connection abstractions and implementations
for various database systems (MySQL, PostgreSQL, SQLite, etc.).
"""

from .base import (
    DatabaseConnection,
    DatabaseType,
    QueryResult,
    TableSchema,
    DatabaseConnectionError,
    DatabaseQueryError,
    DatabaseTimeoutError,
    DatabaseConfigError
)

from .mysql import MySQLConnection, create_mysql_connection

# List what's available when someone imports from this package
__all__ = [
    'DatabaseConnection',
    'DatabaseType',
    'QueryResult',
    'TableSchema',
    'DatabaseConnectionError',
    'DatabaseQueryError',
    'DatabaseTimeoutError',
    'DatabaseConfigError',
    'MySQLConnection',
    'create_mysql_connection'
]