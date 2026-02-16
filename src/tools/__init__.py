"""
MCP Tools for Database Explorer.

This package contains all MCP tools for interacting with databases
through natural language queries.
"""

# Import tools as we create them
from .query_tools import natural_language_query, execute_sql_query
from .schema_tools import get_schema, list_tables, get_table_info
from .connection_tools import (
    list_connections, 
    connect,
    disconnect,
    test_connection,
    get_database_info
)

__all__ = [
    'natural_language_query',
    'execute_sql_query',
    'get_schema',
    'list_tables',
    'get_table_info',
    'list_connections',
    'connect',
    'disconnect',
    'test_connection',
    'get_database_info'
]