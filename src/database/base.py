"""
Base Database Connection Interface

This module defines the abstract base class that all database connectors
must implement. It establishes a consistent interface for connecting to,
querying, and managing different types of databases (MySQL, PostgreSQL, etc.).

The abstract class ensures:
1. All database implementations have the same methods
2. Consistent error handling across databases
3. Uniform configuration format
4. Standardized query results format
5. Full async/await support for non-blocking operations
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple
import json
from dataclasses import dataclass
from enum import Enum


class DatabaseType(Enum):
    """Supported database types."""
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    UNKNOWN = "unknown"


@dataclass
class QueryResult:
    """
    Standardized query result container.
    
    Attributes:
        success: Whether the query executed successfully
        data: List of rows (each row is a dict of column->value)
        columns: List of column names in order
        row_count: Number of rows returned/affected
        execution_time: Time taken in seconds
        error_message: Error description if success=False
        sql_query: The original SQL query executed

        [{"id": 1, "name": "Alice", "age": 10},{"id": 2, "name": "Bob", "age": 12}]

    """
    success: bool
    data: List[Dict[str, Any]] 
    columns: List[str]
    row_count: int
    execution_time: float
    error_message: Optional[str] = None
    sql_query: Optional[str] = None


@dataclass  
class TableSchema:
    """
    Schema information for a database table.
    
    Attributes:
        name: Table name
        columns: List of column information dictionaries
        primary_keys: List of primary key column names
        foreign_keys: List of foreign key information
        indexes: List of index information
        row_count: Approximate number of rows
        create_statement: SQL CREATE statement for the table
    """
    name: str
    columns: List[Dict[str, Any]]
    primary_keys: List[str]
    foreign_keys: List[Dict[str, Any]]
    indexes: List[Dict[str, Any]]
    row_count: int
    create_statement: Optional[str] = None


class DatabaseConnection(ABC):
    """
    Abstract Base Class for Database Connections.
    
    This class defines the contract that all concrete database implementations
    must follow. It provides a unified interface for:
    - Connection management (connect/disconnect)
    - Query execution with standardized results
    - Schema inspection
    - Connection testing
    - Configuration management
    
    Implementation Pattern:
    1. Each database type (MySQL, PostgreSQL) creates a subclass
    2. Subclasses implement all abstract methods
    3. All methods return standardized types (QueryResult, TableSchema)
    4. Error handling is consistent across implementations
    
    Example Usage:
        ```python
        # In mysql.py
        class MySQLConnection(DatabaseConnection):
            def connect(self, config):
                # MySQL-specific implementation
                pass
        ```
    """
    
    def __init__(self, connection_name: str):
        """
        Initialize a database connection.
        
        Args:
            connection_name: Unique identifier for this connection
                             (e.g., "mysql_prod", "postgres_dev")
        """
        self.connection_name = connection_name
        self.config: Dict[str, Any] = {}
        self._connection = None
        self._is_connected = False
    
    @abstractmethod
    def connect(self, config: Dict[str, Any]) -> bool:
        """
        Establish a connection to the database using the provided configuration.
        
        Args:
            config: Dictionary containing connection parameters.
                    Expected keys vary by database type but typically include:
                    - host: Database server hostname
                    - port: Database server port
                    - database: Database name
                    - username: Authentication username
                    - password: Authentication password (or env var name)
                    - ssl: Whether to use SSL
                    - timeout: Connection timeout in seconds
                    
        Returns:
            bool: True if connection succeeded, False otherwise
            
        Raises:
            ConnectionError: If connection cannot be established
            ValueError: If config is invalid or missing required keys
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """
        Gracefully close the database connection.
        
        This method should:
        1. Close any open cursors/connections
        2. Release connection pool resources
        3. Set internal state to disconnected
        4. Handle any cleanup operations
        
        Note: Should be idempotent (safe to call multiple times)
        """
        pass
    
    # -----------------------------------------------------------------
    # ASYNC Methods (Non-blocking operations)
    # -----------------------------------------------------------------
    
    @abstractmethod
    async def async_connect(self, config: Dict[str, Any]) -> bool:
        """
        Asynchronously establish a connection to the database.
        Non-blocking version of connect().
        
        Args:
            config: Dictionary containing connection parameters
                    
        Returns:
            bool: True if connection succeeded
            
        Raises:
            DatabaseConnectionError: If connection fails
        """
        pass
    
    @abstractmethod
    async def async_disconnect(self) -> None:
        """
        Asynchronously close the database connection.
        Non-blocking version of disconnect().
        """
        pass
    
    @abstractmethod
    async def async_execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None,
        max_rows: int = 1000,
        timeout: int = 30
    ) -> QueryResult:
        """
        Asynchronously execute a SQL query.
        Non-blocking version of execute_query().
        
        Args:
            query: SQL query string
            params: Optional query parameters
            max_rows: Maximum rows to return
            timeout: Query timeout in seconds
            
        Returns:
            QueryResult: Standardized query result
        """
        pass
    
    @abstractmethod
    async def async_get_schema(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Asynchronously retrieve schema information.
        Non-blocking version of get_schema().
        
        Args:
            table_name: Optional specific table name
            
        Returns:
            Dictionary with schema information
        """
        pass
    
    @abstractmethod
    async def async_get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        """
        Asynchronously get detailed schema for a specific table.
        Non-blocking version of get_table_schema().
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableSchema object or None if not found
        """
        pass
    
    @abstractmethod
    async def async_test_connection(self) -> Tuple[bool, str]:
        """
        Asynchronously test if connection is alive.
        Non-blocking version of test_connection().
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        pass
    
    @abstractmethod
    async def async_get_database_info(self) -> Dict[str, Any]:
        """
        Asynchronously get database metadata.
        Non-blocking version of get_database_info().
        
        Returns:
            Dictionary with database information
        """
        pass
    
    @abstractmethod
    def execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None,
        max_rows: int = 1000,
        timeout: int = 30
    ) -> QueryResult:
        """
        Execute a SQL query and return standardized results.
        
        Args:
            query: SQL query string
            params: Optional dictionary of query parameters for parameterized queries
                   (prevents SQL injection)
            max_rows: Maximum number of rows to return (safety limit)
            timeout: Query timeout in seconds
            
        Returns:
            QueryResult: Standardized result container with:
                - success: Boolean indicating query success
                - data: List of rows (each as dict)
                - columns: Column names
                - row_count: Number of rows returned
                - execution_time: Query duration
                - error_message: If failed, description of error
            
        Important:
            - MUST use parameterized queries when params are provided
            - MUST enforce max_rows limit
            - MUST enforce timeout
            - MUST handle exceptions gracefully (don't crash server)
        """
        pass
    
    @abstractmethod
    def get_schema(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve schema information from the database.
        
        Args:
            table_name: Optional specific table name. If None, returns all tables.
            
        Returns:
            Dictionary containing schema information:
            {
                "tables": [
                    {
                        "name": "users",
                        "columns": [
                            {"name": "id", "type": "int", "nullable": False},
                            {"name": "email", "type": "varchar(255)", "nullable": False}
                        ],
                        "primary_keys": ["id"],
                        "row_count": 1000
                    }
                ],
                "database": "mydb",
                "db_type": "mysql"
            }
            
        Note: This is used to provide context to LLM for NL-to-SQL conversion.
        """
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        """
        Get detailed schema information for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableSchema object with detailed metadata, or None if table doesn't exist
        """
        pass
    
    @abstractmethod
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test if the database connection is alive and working.
        
        Returns:
            Tuple of (success: bool, message: str)
            - success: True if connection test passes
            - message: Descriptive message (error details or success confirmation)
            
        Typical implementation executes a simple query like "SELECT 1"
        """
        pass
    
    @abstractmethod
    def get_database_info(self) -> Dict[str, Any]:
        """
        Get metadata about the connected database.
        
        Returns:
            Dictionary with database information:
            {
                "name": "database_name",
                "type": "mysql",
                "version": "8.0.33",
                "size_mb": 250.5,
                "character_set": "utf8mb4",
                "collation": "utf8mb4_general_ci"
            }
        """
        pass
    
    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if connection is currently active.
        
        Returns:
            bool: True if connection is established and responsive
        """
        pass
    
    @property
    @abstractmethod
    def db_type(self) -> DatabaseType:
        """
        Return the type of database.
        
        Returns:
            DatabaseType enum value (MYSQL, POSTGRESQL, etc.)
        """
        pass
    
    def __enter__(self):
        """Support context manager protocol."""
        if not self.is_connected and self.config:
            self.connect(self.config)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup on context manager exit."""
        self.disconnect()
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"{self.__class__.__name__}(name='{self.connection_name}', type={self.db_type}, connected={self.is_connected})"


# Exception classes for database operations
class DatabaseConnectionError(Exception):
    """Raised when database connection fails."""
    pass

class DatabaseQueryError(Exception):
    """Raised when a query execution fails."""
    pass

class DatabaseConfigError(Exception):
    """Raised when configuration is invalid."""
    pass

class DatabaseTimeoutError(DatabaseQueryError):
    """Raised when a query times out."""
    pass


# DatabaseType (Enum)
# A list of named constants representing different database systems our MCP server supports (MySQL, PostgreSQL, SQLite), ensuring type safety by using DatabaseType.MYSQL instead of error-prone strings like "mysql" throughout the codebase.

# QueryResult (Data Class)
# A standardized container that guarantees all database queries return results in exactly the same format regardless of database type, including success status, data rows, column names, row count, execution time, and optional error messages for consistent tool processing.

# TableSchema (Data Class)
# A uniform structure for storing table metadata including column definitions, primary/foreign keys, indexes, and row counts, providing LLMs with consistent schema context for natural language to SQL conversion across different database systems.

# DatabaseConnection (Abstract Base Class)
# The core contract/blueprint that every specific database implementation (MySQL, PostgreSQL, etc.) must follow, defining mandatory methods for connection management, query execution with safety limits, schema inspection, and standardized error handling to ensure polymorphic behavior.

# DatabaseConnectionError (Exception)
# Specialized exception raised when establishing a database connection fails, allowing for targeted error handling and clear user feedback about network, authentication, or configuration issues.

# DatabaseQueryError (Exception)
# Exception thrown when SQL query execution fails due to syntax errors, missing tables, or permission issues, distinct from connection errors for precise troubleshooting and error reporting.

# DatabaseConfigError (Exception)
# Exception for invalid configuration scenarios like missing required parameters or incorrect format, enabling validation failures to be caught and reported separately from runtime errors.

# DatabaseTimeoutError (Exception)
# A specialized query error for operations exceeding time limits, crucial for preventing LLM-generated queries from hanging the MCP server when they accidentally create expensive operations.

# __init__(self, connection_name: str)
# What it does: Creates a new database connection object with a name.
# Why it exists: Every connection needs a unique label like "customer_db" or "inventory_db" so you can manage multiple databases.
# Simple analogy: Like creating a new contact in your phone - you give it a name first before adding details.

# connect(self, config: Dict[str, Any]) -> bool
# What it does: Actually connects to the database using settings (hostname, password, etc.).
# Why it exists: Without connecting, you can't run any queries. This is like dialing a phone number.

# get_schema(self, table_name=None) -> Dict[str, Any]
# What it does: Gets information about database structure.
# Why it exists: LLM needs to know what tables and columns exist to write correct SQL.

# get_table_schema(self, table_name: str) -> Optional[TableSchema]
# What it does: Gets VERY detailed info about one specific table.
# Why it exists: More detailed than get_schema() - includes foreign keys, indexes, etc.

# test_connection(self) -> Tuple[bool, str]
# What it does: Checks if database is reachable with a simple test.
# Why it exists: Before running real queries, verify connection works.

