"""
Query Tools for Database Explorer MCP Server.

Tools for executing natural language and SQL queries.
"""

from typing import Dict, Any, Optional
import json
import logging
from ..database.mysql import MySQLConnection

logger = logging.getLogger(__name__)

# Import shared connections from connection_tools
from .connection_tools import _active_connections


def get_connection(connection_name: str) -> Optional[MySQLConnection]:
    """Get an active database connection by name."""
    return _active_connections.get(connection_name)


def natural_language_query(
    connection_name: str,
    question: str,
    max_rows: int = 100
) -> Dict[str, Any]:
    """
    Convert natural language to SQL using client's AI.
    
    This function provides schema context to the client's AI for SQL generation.
    The client's AI will generate the SQL, then execute_sql_query should be called.
    
    Args:
        connection_name: Name of the database connection
        question: Natural language question (e.g., "Show me customers from New York")
        max_rows: Maximum rows to return
        
    Returns:
        Dictionary with schema context and prompt for client AI to generate SQL
    """
    try:
        # Get the database connection
        db = get_connection(connection_name)
        if not db:
            return {
                "success": False,
                "error": f"No active connection named '{connection_name}'",
                "suggested_action": "Use list_connections() to see available connections"
            }
        
        # Get database schema for context
        schema = db.get_schema()
        
        # Format schema for LLM context
        schema_context = {
            "database": schema.get("database"),
            "db_type": schema.get("db_type"),
            "mysql_version": schema.get("mysql_version"),
            "tables": []
        }
        
        # Format each table with essential information
        for table in schema.get("tables", []):
            table_info = {
                "name": table.get("name"),
                "columns": [
                    {
                        "name": col.get("name"),
                        "type": col.get("type"),
                        "nullable": col.get("nullable", False),
                        "is_primary": col.get("name") in table.get("primary_keys", [])
                    }
                    for col in table.get("columns", [])
                ],
                "primary_keys": table.get("primary_keys", []),
                "foreign_keys": [
                    {
                        "column": fk.get("column"),
                        "references": f"{fk.get('referenced_table')}.{fk.get('referenced_column')}"
                    }
                    for fk in table.get("foreign_keys", [])
                ]
            }
            schema_context["tables"].append(table_info)
        
        # Create prompt for client AI
        prompt = f"""You are a SQL query generator. Convert the following natural language question into a valid SQL query.

Database Information:
- Database: {schema_context['database']}
- Type: {schema_context['db_type']}
- Version: {schema_context.get('mysql_version', 'Unknown')}

Schema:
{json.dumps(schema_context['tables'], indent=2)}

Natural Language Question: {question}

Instructions:
1. Generate a valid SQL query that answers the question
2. Use proper table and column names from the schema above
3. Only use SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)
4. Include appropriate WHERE, JOIN, GROUP BY, ORDER BY clauses as needed
5. Limit results to {max_rows} rows if using LIMIT
6. Return ONLY the SQL query, no explanations

SQL Query:"""

        return {
            "success": True,
            "question": question,
            "schema_context": schema_context,
            "prompt": prompt,
            "max_rows": max_rows,
            "instructions": {
                "step_1": "Use the provided prompt with your AI/LLM to generate SQL",
                "step_2": "The AI should return only the SQL query",
                "step_3": "Call execute_sql_query_tool() with the generated SQL",
                "note": "The server provides schema context, client AI generates SQL"
            },
            "example_usage": {
                "client_ai_prompt": prompt,
                "then_call": "execute_sql_query_tool(connection_name, generated_sql, max_rows)"
            }
        }
        
    except Exception as e:
        logger.error(f"Natural language query failed: {e}")
        return {
            "success": False,
            "error": f"Query failed: {str(e)}"
        }


def execute_sql_query(
    connection_name: str,
    sql_query: str,
    max_rows: int = 1000,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Execute a SQL query on the database.
    
    Args:
        connection_name: Name of the database connection
        sql_query: SQL query string
        max_rows: Maximum rows to return
        timeout: Query timeout in seconds
        
    Returns:
        Dictionary with query results
    """
    try:
        # Get the database connection
        db = get_connection(connection_name)
        if not db:
            return {
                "success": False,
                "error": f"No active connection named '{connection_name}'",
                "available_connections": list(_active_connections.keys())
            }
        
        # Execute the query
        result = db.execute_query(
            query=sql_query,
            max_rows=max_rows,
            timeout=timeout
        )
        
        # Convert to JSON-serializable format
        return {
            "success": result.success,
            "data": result.data,
            "columns": result.columns,
            "row_count": result.row_count,
            "execution_time": result.execution_time,
            "sql_query": result.sql_query,
            "note": f"Returned {len(result.data)} of {result.row_count} rows"
        }
        
    except Exception as e:
        logger.error(f"SQL query execution failed: {e}")
        return {
            "success": False,
            "error": f"Query execution failed: {str(e)}",
            "sql_query": sql_query
        }