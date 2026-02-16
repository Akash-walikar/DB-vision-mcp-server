"""
Schema Tools for Database Explorer MCP Server.

Tools for exploring database schema and structure.
"""

from typing import Dict, Any, Optional, List
import json
import logging
from ..database.mysql import MySQLConnection
from ..database.base import TableSchema

logger = logging.getLogger(__name__)


def get_connection(connection_name: str) -> Optional[MySQLConnection]:
    """Get an active database connection by name."""
    # Import from query_tools (circular import workaround)
    from .query_tools import _active_connections
    return _active_connections.get(connection_name)


def get_schema(
    connection_name: str,
    table_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get database schema information.
    
    Args:
        connection_name: Name of the database connection
        table_name: Optional specific table name
        
    Returns:
        Dictionary with schema information
    """
    try:
        db = get_connection(connection_name)
        if not db:
            return {
                "success": False,
                "error": f"No active connection named '{connection_name}'"
            }
        
        # Get schema from database
        schema = db.get_schema(table_name)
        
        # Format for better readability
        formatted_tables = []
        for table in schema.get("tables", []):
            formatted_table = {
                "name": table["name"],
                "column_count": len(table["columns"]),
                "primary_keys": table.get("primary_keys", []),
                "sample_columns": [
                    {"name": col["name"], "type": col["type"]}
                    for col in table["columns"][:5]  # First 5 columns
                ]
            }
            if len(table["columns"]) > 5:
                formatted_table["note"] = f"... and {len(table['columns']) - 5} more columns"
            formatted_tables.append(formatted_table)
        
        return {
            "success": True,
            "database": schema.get("database"),
            "db_type": schema.get("db_type"),
            "mysql_version": schema.get("mysql_version"),
            "tables": formatted_tables,
            "table_count": len(formatted_tables),
            "note": f"Showing schema for {table_name if table_name else 'all tables'}"
        }
        
    except Exception as e:
        logger.error(f"Schema retrieval failed: {e}")
        return {
            "success": False,
            "error": f"Schema retrieval failed: {str(e)}"
        }


def list_tables(connection_name: str) -> Dict[str, Any]:
    """
    List all tables in the database.
    
    Args:
        connection_name: Name of the database connection
        
    Returns:
        Dictionary with table list
    """
    try:
        db = get_connection(connection_name)
        if not db:
            return {
                "success": False,
                "error": f"No active connection named '{connection_name}'"
            }
        
        # Get schema (we only need table names)
        schema = db.get_schema()
        
        tables = []
        for table in schema.get("tables", []):
            table_info = {
                "name": table["name"],
                "columns": len(table["columns"]),
                "primary_keys": table.get("primary_keys", []),
                "has_foreign_keys": len(table.get("foreign_keys", [])) > 0
            }
            tables.append(table_info)
        
        return {
            "success": True,
            "tables": tables,
            "count": len(tables),
            "database": schema.get("database")
        }
        
    except Exception as e:
        logger.error(f"Table listing failed: {e}")
        return {
            "success": False,
            "error": f"Table listing failed: {str(e)}"
        }


def get_table_info(
    connection_name: str,
    table_name: str
) -> Dict[str, Any]:
    """
    Get detailed information about a specific table.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        
    Returns:
        Dictionary with table details
    """
    try:
        db = get_connection(connection_name)
        if not db:
            return {
                "success": False,
                "error": f"No active connection named '{connection_name}'"
            }
        
        # Get detailed table schema
        table_schema = db.get_table_schema(table_name)
        if not table_schema:
            return {
                "success": False,
                "error": f"Table '{table_name}' not found"
            }
        
        # Format columns
        formatted_columns = []
        for col in table_schema.columns:
            column_info = {
                "name": col["name"],
                "type": col["type"],
                "nullable": col.get("nullable", False),
                "default": col.get("default"),
                "is_primary": col["name"] in table_schema.primary_keys,
                "extra": col.get("extra")
            }
            formatted_columns.append(column_info)
        
        # Format foreign keys
        formatted_foreign_keys = []
        for fk in table_schema.foreign_keys:
            fk_info = {
                "column": fk.get("column"),
                "references": f"{fk.get('referenced_table')}.{fk.get('referenced_column')}",
                "constraint": fk.get("constraint_name")
            }
            formatted_foreign_keys.append(fk_info)
        
        # Format indexes
        formatted_indexes = []
        for idx in table_schema.indexes:
            idx_info = {
                "name": idx.get("name"),
                "unique": idx.get("unique", False),
                "columns": idx.get("columns", []),
                "type": idx.get("type")
            }
            formatted_indexes.append(idx_info)
        
        return {
            "success": True,
            "table_name": table_schema.name,
            "row_count": table_schema.row_count,
            "columns": formatted_columns,
            "primary_keys": table_schema.primary_keys,
            "foreign_keys": formatted_foreign_keys,
            "indexes": formatted_indexes,
            "has_create_statement": table_schema.create_statement is not None,
            "note": "Use execute_sql_query() to query this table"
        }
        
    except Exception as e:
        logger.error(f"Table info retrieval failed: {e}")
        return {
            "success": False,
            "error": f"Table info retrieval failed: {str(e)}"
        }