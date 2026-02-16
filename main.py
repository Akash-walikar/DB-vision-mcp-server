#!/usr/bin/env python3
"""
Database Explorer MCP Server

A Model Context Protocol server for exploring and querying databases
using natural language.
"""

from fastmcp import FastMCP
import logging
from typing import Dict, Any, Optional
import json

# Import our tools
from src.tools.connection_tools import (
    list_connections,
    connect,
    disconnect,
    test_connection,
    get_database_info
)
from src.tools.query_tools import (
    natural_language_query,
    execute_sql_query
)
from src.tools.schema_tools import (
    get_schema,
    list_tables,
    get_table_info
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create MCP server
mcp = FastMCP(
    name="database-explorer",
    version="0.1.0"
)

# ------------------------------------------------------------
# Connection Management Tools
# ------------------------------------------------------------

@mcp.tool()
def list_connections_tool() -> Dict[str, Any]:
    """
    List all available database connections.
    
    Returns information about configured database connections,
    including which ones are currently active.
    """
    logger.info("Tool called: list_connections")
    return list_connections()


@mcp.tool()
def connect_tool(
    connection_name: str,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Connect to a database.
    
    Args:
        connection_name: Name for this connection
        config: Optional connection configuration dict. If not provided, loads from 
                src/config/connections/{connection_name}.json
                
    Example with config file:
        connect_tool("docker_mysql")
        
    Example with inline config:
        connect_tool("my_db", config={
            "type": "mysql",
            "host": "localhost",
            "user": "your_user",
            "password": "env:DB_PASSWORD",
            "database": "your_database"
        })
    """
    logger.info(f"Tool called: connect - connection: {connection_name}")
    return connect(connection_name, config=config)


@mcp.tool()
def disconnect_tool(connection_name: str) -> Dict[str, Any]:
    """
    Disconnect from a database.
    
    Args:
        connection_name: Name of the connection to disconnect
    """
    logger.info(f"Tool called: disconnect - connection: {connection_name}")
    return disconnect(connection_name)


@mcp.tool()
def test_connection_tool(connection_name: str) -> Dict[str, Any]:
    """
    Test if a database connection is alive.
    
    Args:
        connection_name: Name of the connection to test
    """
    logger.info(f"Tool called: test_connection - connection: {connection_name}")
    return test_connection(connection_name)


@mcp.tool()
def get_database_info_tool(connection_name: str) -> Dict[str, Any]:
    """
    Get metadata about a database.
    
    Args:
        connection_name: Name of the connection
    
    Returns information like database version, size, character set, etc.
    """
    logger.info(f"Tool called: get_database_info - connection: {connection_name}")
    return get_database_info(connection_name)

# ------------------------------------------------------------
# Schema Exploration Tools
# ------------------------------------------------------------

@mcp.tool()
def get_schema_tool(
    connection_name: str,
    table_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get database schema information.
    
    Args:
        connection_name: Name of the database connection
        table_name: Optional specific table name (if not provided, returns all tables)
    
    Returns table structures, columns, data types, and relationships.
    """
    logger.info(f"Tool called: get_schema - connection: {connection_name}, table: {table_name}")
    return get_schema(connection_name, table_name)


@mcp.tool()
def list_tables_tool(connection_name: str) -> Dict[str, Any]:
    """
    List all tables in a database.
    
    Args:
        connection_name: Name of the database connection
    
    Returns a list of tables with basic information.
    """
    logger.info(f"Tool called: list_tables - connection: {connection_name}")
    return list_tables(connection_name)


@mcp.tool()
def get_table_info_tool(
    connection_name: str,
    table_name: str
) -> Dict[str, Any]:
    """
    Get detailed information about a specific table.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table to inspect
    
    Returns column details, primary keys, foreign keys, indexes, etc.
    """
    logger.info(f"Tool called: get_table_info - connection: {connection_name}, table: {table_name}")
    return get_table_info(connection_name, table_name)

# ------------------------------------------------------------
# Query Execution Tools
# ------------------------------------------------------------

@mcp.tool()
def natural_language_query_tool(
    connection_name: str,
    question: str,
    max_rows: int = 100
) -> Dict[str, Any]:
    """
    Convert natural language to SQL using client's AI.
    
    This tool provides database schema context to the client's AI for SQL generation.
    The server extracts the schema, formats it, and returns a prompt that the client
    can use with their AI/LLM to generate SQL. The client then calls execute_sql_query_tool
    with the generated SQL.
    
    Args:
        connection_name: Name of the database connection
        question: Natural language question (e.g., "Show me customers from New York")
        max_rows: Maximum rows to return (default: 100)
    
    Returns:
        Dictionary containing:
        - schema_context: Database schema information
        - prompt: Formatted prompt for client AI to generate SQL
        - instructions: Step-by-step guide for using the prompt
        - example_usage: Example of how to use the generated prompt
    
    Workflow:
        1. Server extracts schema and creates prompt
        2. Client uses prompt with their AI to generate SQL
        3. Client calls execute_sql_query_tool() with generated SQL
    """
    logger.info(f"Tool called: natural_language_query - connection: {connection_name}, question: {question[:50]}...")
    return natural_language_query(connection_name, question, max_rows)


@mcp.tool()
def execute_sql_query_tool(
    connection_name: str,
    sql_query: str,
    max_rows: int = 1000,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Execute a SQL query on the database.
    
    Args:
        connection_name: Name of the database connection
        sql_query: SQL query to execute
        max_rows: Maximum rows to return (default: 1000)
        timeout: Query timeout in seconds (default: 30)
    
    Returns query results with data, columns, and execution metadata.
    """
    logger.info(f"Tool called: execute_sql_query - connection: {connection_name}, query: {sql_query[:100]}...")
    return execute_sql_query(connection_name, sql_query, max_rows, timeout)

# ------------------------------------------------------------
# MCP Resources (for schema context)
# ------------------------------------------------------------

@mcp.resource("schema://{connection_name}")
def get_schema_resource(connection_name: str) -> str:
    """
    Expose database schema as MCP resource.
    
    This allows clients to cache schema information for LLM context.
    Returns schema for all tables in the database.
    """
    logger.info(f"Resource requested: schema://{connection_name}")
    
    result = get_schema(connection_name)
    return json.dumps(result, indent=2)


@mcp.resource("schema://{connection_name}/{table_name}")
def get_table_schema_resource(connection_name: str, table_name: str) -> str:
    """
    Expose specific table schema as MCP resource.
    
    Returns schema for a specific table.
    """
    logger.info(f"Resource requested: schema://{connection_name}/{table_name}")
    
    result = get_schema(connection_name, table_name)
    return json.dumps(result, indent=2)


@mcp.resource("connections://list")
def list_connections_resource() -> str:
    """
    Expose connection list as MCP resource.
    """
    logger.info("Resource requested: connections://list")
    
    result = list_connections()
    return json.dumps(result, indent=2)

# ------------------------------------------------------------
# Server Startup
# ------------------------------------------------------------

def main():
    """Main entry point for the MCP server."""
    # Note: Print statements are sent to stderr to avoid interfering with MCP stdio protocol
    import sys
    print("Starting Database Explorer MCP Server...", file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    print("Available Tools:", file=sys.stderr)
    print("1. list_connections_tool() - List database connections", file=sys.stderr)
    print("2. connect_tool(name) - Connect to a database", file=sys.stderr)
    print("3. disconnect_tool(name) - Disconnect from a database", file=sys.stderr)
    print("4. test_connection_tool(name) - Test connection", file=sys.stderr)
    print("5. get_database_info_tool(name) - Get database metadata", file=sys.stderr)
    print("6. get_schema_tool(name, table) - Get schema", file=sys.stderr)
    print("7. list_tables_tool(name) - List tables", file=sys.stderr)
    print("8. get_table_info_tool(name, table) - Get table details", file=sys.stderr)
    print("9. natural_language_query_tool(name, question) - NL to SQL", file=sys.stderr)
    print("10. execute_sql_query_tool(name, sql) - Execute SQL", file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    print("Server running on stdio (for MCP clients)", file=sys.stderr)
    
    # Run the MCP server on stdio (for Claude Desktop, etc.)
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()