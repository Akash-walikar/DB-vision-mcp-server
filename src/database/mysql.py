"""
MySQL Database Connection Implementation

This module provides the concrete implementation of DatabaseConnection
for MySQL databases using the PyMySQL library for sync operations
and aiomysql for async operations.

Key Features:
- Real MySQL connections using PyMySQL (sync) and aiomysql (async)
- MySQL-specific schema extraction from INFORMATION_SCHEMA
- Parameterized queries to prevent SQL injection
- Safety limits (row limits, timeouts)
- Connection pooling support
- Full async/await support for non-blocking operations
"""

import pymysql
import pymysql.cursors
import aiomysql
from typing import Dict, List, Any, Optional, Sequence, Tuple, Mapping
import time
import logging
import json
import os
import asyncio

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

# Set up logging
logger = logging.getLogger(__name__)


class MySQLConnection(DatabaseConnection):
    """
    Concrete MySQL database connection implementation.
    
    This class implements all abstract methods from DatabaseConnection
    specifically for MySQL databases using both PyMySQL (sync) and aiomysql (async).
    """
    
    def __init__(self, connection_name: str):
        """
        Initialize MySQL connection.
        
        Args:
            connection_name: Unique name for this connection
        """
        super().__init__(connection_name)
        self._connection: Optional[pymysql.connections.Connection] = None
        self._cursor: Optional[pymysql.cursors.Cursor] = None
        self._pool = None
        self._mysql_version: Optional[str] = None
        
        # Async components
        self._async_connection: Optional[aiomysql.Connection] = None
        self._async_pool: Optional[aiomysql.Pool] = None
        
    # -----------------------------------------------------------------
    # Connection Management Methods
    # -----------------------------------------------------------------
    
    def connect(self, config: Dict[str, Any]) -> bool:
        """
        Establish connection to MySQL database.
        
        Args:
            config: Dictionary with connection parameters:
                Required: host, user, password, database
                Optional: port (default 3306), charset, ssl, etc.
                
        Returns:
            bool: True if connection successful
            
        Raises:
            DatabaseConnectionError: If connection fails
            DatabaseConfigError: If config is invalid
        """
        try:
            # Validate required config
            required_keys = ['host', 'user', 'database']
            for key in required_keys:
                if key not in config:
                    raise DatabaseConfigError(f"Missing required config key: {key}")
            
            # Get password (could be direct or from env var)
            password = self._get_password(config)
            
            # Build connection parameters
            conn_params = {
                'host': config['host'],
                'user': config['user'],
                'password': password,
                'database': config['database'],
                'port': config.get('port', 3306),
                'charset': config.get('charset', 'utf8mb4'),
                'cursorclass': pymysql.cursors.DictCursor,
                'autocommit': config.get('autocommit', True),
            }
            
            # Add SSL if configured
            if config.get('ssl', False):
                conn_params['ssl'] = {'ssl': True}
            
            # Add connection timeout
            if 'connect_timeout' in config:
                conn_params['connect_timeout'] = config['connect_timeout']
            
            # Store config for reconnection
            self.config = config
            
            # Establish connection
            logger.info(f"Connecting to MySQL: {config['host']}:{conn_params['port']}/{config['database']}")
            self._connection = pymysql.connect(**conn_params)
            
            # Test connection with a simple query
            self._connection.ping(reconnect=True)
            
            # Get MySQL version
            with self._connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("SELECT VERSION() as version")
                result = cursor.fetchone()
                # Fixed: Use .get() method which is compatible with Row/dict types
                self._mysql_version = result.get('version') if result else "Unknown"
            
            self._is_connected = True
            logger.info(f"Successfully connected to MySQL (version: {self._mysql_version})")
            return True
            
        except pymysql.Error as e:
            error_msg = f"MySQL connection failed: {str(e)}"
            logger.error(error_msg)
            self._is_connected = False
            raise DatabaseConnectionError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected connection error: {str(e)}"
            logger.error(error_msg)
            self._is_connected = False
            raise DatabaseConnectionError(error_msg) from e
    
    def disconnect(self) -> None:
        """Close MySQL connection gracefully."""
        try:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            
            if self._connection:
                self._connection.close()
                self._connection = None
            
            self._is_connected = False
            logger.info(f"Disconnected from MySQL: {self.connection_name}")
            
        except Exception as e:
            logger.warning(f"Error during disconnect: {str(e)}")
        finally:
            self._is_connected = False
    
    # -----------------------------------------------------------------
    # ASYNC Connection Management Methods
    # -----------------------------------------------------------------
    
    async def async_connect(self, config: Dict[str, Any]) -> bool:
        """
        Asynchronously establish connection to MySQL database.
        
        Args:
            config: Dictionary with connection parameters
                    
        Returns:
            bool: True if connection successful
            
        Raises:
            DatabaseConnectionError: If connection fails
        """
        try:
            required_keys = ['host', 'user', 'database']
            for key in required_keys:
                if key not in config:
                    raise DatabaseConfigError(f"Missing required config key: {key}")
            
            password = self._get_password(config)
            
            # Create async pool
            self._async_pool = await aiomysql.create_pool(
                host=config['host'],
                port=config.get('port', 3306),
                user=config['user'],
                password=password,
                db=config['database'],
                charset=config.get('charset', 'utf8mb4'),
                autocommit=config.get('autocommit', True),
                minsize=1,
                maxsize=10,
            )
            
            self.config = config
            self._is_connected = True
            logger.info(f"Async connected to MySQL: {config['host']}:{config.get('port', 3306)}/{config['database']}")
            return True
            
        except Exception as e:
            error_msg = f"Async MySQL connection failed: {str(e)}"
            logger.error(error_msg)
            self._is_connected = False
            raise DatabaseConnectionError(error_msg) from e
    
    async def async_disconnect(self) -> None:
        """Asynchronously close MySQL connection."""
        try:
            if self._async_pool:
                self._async_pool.close()
                await self._async_pool.wait_closed()
                self._async_pool = None
            
            self._is_connected = False
            logger.info(f"Async disconnected from MySQL: {self.connection_name}")
            
        except Exception as e:
            logger.warning(f"Error during async disconnect: {str(e)}")
        finally:
            self._is_connected = False
    
    # -----------------------------------------------------------------
    # Query Execution Methods
    # -----------------------------------------------------------------
    
    def execute_query(
        self, 
        query: str, 
        params: Optional[Mapping[str, Any] | Sequence[Any]] = None,
        max_rows: int = 1000,
        timeout: int = 30
    ) -> QueryResult:
        """
        Execute SQL query on MySQL with safety limits.
        
        Args:
            query: SQL query string
            params: Dictionary of parameters for parameterized query
            max_rows: Maximum rows to return (safety limit)
            timeout: Query timeout in seconds
            
        Returns:
            QueryResult: Standardized result container
        """
        start_time = time.time()
        query_lower = query.strip().lower()
        
        # Check if connection exists before proceeding
        if self._connection is None:
            raise DatabaseConnectionError("Not connected to database")
        
        cursor: Optional[pymysql.cursors.Cursor] = None
        try:
            # Create cursor
            cursor = self._connection.cursor()
            
            # Type assertion: cursor() never returns None, it raises on error
            assert cursor is not None, "Failed to create cursor"
            
            # Set timeout if supported (MySQL 5.7.4+)
            # Check if version is 5.7.4 or higher, or 8.0+
            if self._mysql_version and self._mysql_version != "Unknown":
                try:
                    # Parse version string (format: "X.Y.Z" or "X.Y.Z-extra")
                    version_parts = self._mysql_version.split('-')[0].split('.')
                    if len(version_parts) >= 3:
                        major = int(version_parts[0])
                        minor = int(version_parts[1])
                        patch = int(version_parts[2])
                        # MySQL 5.7.4+ or MySQL 8.0+ supports max_execution_time
                        if (major > 5) or (major == 5 and minor > 7) or (major == 5 and minor == 7 and patch >= 4):
                            cursor.execute(f"SET SESSION max_execution_time = {timeout * 1000}")
                except (ValueError, IndexError):
                    # If version parsing fails, skip timeout setting
                    logger.warning(f"Could not parse MySQL version: {self._mysql_version}")
            
            # Execute query with timing
            execute_start = time.time()
            
            if params:
                # Use parameterized query to prevent SQL injection
                # PyMySQL supports dict params with named placeholders %(name)s
                # or tuple/list params with positional placeholders %s
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            execution_time = time.time() - execute_start
            
            # Handle different query types
            if query_lower.startswith(('select', 'show', 'describe', 'explain')):
                # Fetch results with row limit
                rows = cursor.fetchmany(max_rows)
                
                # Check if we might have more rows (rowcount is approximate for SELECT)
                # Note: For SELECT queries, rowcount may be -1 or inaccurate until all rows fetched
                # Since we use fetchmany(max_rows), we limit results but can't know exact total
                # We'll add a note if we got exactly max_rows (might be more)
                warning = None
                if len(rows) == max_rows:
                    # We got exactly max_rows, there might be more
                    warning = f"Results may be truncated (returned {max_rows} rows, max_rows limit)"
                    logger.info(warning)
                
                # Convert rows to list of dicts
                data = []
                if rows:
                    for row in rows:
                        if isinstance(row, dict):
                            data.append(row)
                        else:
                            # Convert tuple to dict using column names
                            if cursor.description:
                                columns = [desc[0] for desc in cursor.description]
                                data.append(dict(zip(columns, row)))
                
                # Get column names
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # Row count
                row_count = len(data)
                
                result = QueryResult(
                    success=True,
                    data=data,
                    columns=columns,
                    row_count=row_count,
                    execution_time=execution_time,
                    sql_query=query
                )
                
            else:
                # For INSERT, UPDATE, DELETE - get affected rows
                affected_rows = cursor.rowcount
                
                result = QueryResult(
                    success=True,
                    data=[{"affected_rows": affected_rows}],
                    columns=["affected_rows"],
                    row_count=1,
                    execution_time=execution_time,
                    sql_query=query
                )
            
            # Commit if not in autocommit mode
            if not self._connection.autocommit:
                self._connection.commit()
            
            total_time = time.time() - start_time
            logger.debug(f"Query executed in {total_time:.3f}s: {query[:100]}...")
            
            return result
            
        except pymysql.OperationalError as e:
            if "max_execution_time" in str(e).lower():
                raise DatabaseTimeoutError(f"Query exceeded {timeout} second timeout") from e
            raise DatabaseQueryError(f"MySQL operational error: {str(e)}") from e
        except pymysql.Error as e:
            raise DatabaseQueryError(f"MySQL error: {str(e)}") from e
        except Exception as e:
            raise DatabaseQueryError(f"Unexpected error: {str(e)}") from e
        finally:
            if cursor:
                cursor.close()
    
    # -----------------------------------------------------------------
    # ASYNC Query Execution Methods
    # -----------------------------------------------------------------
    
    async def async_execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None,
        max_rows: int = 1000,
        timeout: int = 30
    ) -> QueryResult:
        """
        Asynchronously execute SQL query on MySQL with safety limits.
        
        Args:
            query: SQL query string
            params: Dictionary of parameters for parameterized query
            max_rows: Maximum rows to return (safety limit)
            timeout: Query timeout in seconds
            
        Returns:
            QueryResult: Standardized result container
        """
        start_time = time.time()
        query_lower = query.strip().lower()
        
        if not self._async_pool:
            raise DatabaseConnectionError("Not connected to database (async pool not initialized)")
        
        async with self._async_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    # Execute query with timeout
                    await asyncio.wait_for(cursor.execute(query, params or ()), timeout=timeout)
                    
                    # Handle different query types
                    if query_lower.startswith(('select', 'show', 'describe', 'explain')):
                        # Fetch results with row limit
                        rows = await cursor.fetchmany(max_rows)
                        
                        data = list(rows) if rows else []
                        columns = [desc[0] for desc in cursor.description] if cursor.description else []
                        row_count = len(data)
                        
                        warning = None
                        if len(data) == max_rows:
                            warning = f"Results may be truncated (returned {max_rows} rows, max_rows limit)"
                            logger.info(warning)
                        
                        execution_time = time.time() - start_time
                        
                        result = QueryResult(
                            success=True,
                            data=data,
                            columns=columns,
                            row_count=row_count,
                            execution_time=execution_time,
                            sql_query=query
                        )
                    else:
                        # For INSERT, UPDATE, DELETE
                        affected_rows = cursor.rowcount
                        execution_time = time.time() - start_time
                        
                        result = QueryResult(
                            success=True,
                            data=[{"affected_rows": affected_rows}],
                            columns=["affected_rows"],
                            row_count=1,
                            execution_time=execution_time,
                            sql_query=query
                        )
                    
                    return result
                    
                except asyncio.TimeoutError:
                    execution_time = time.time() - start_time
                    error_msg = f"Query timeout after {timeout} seconds"
                    logger.error(f"Query timed out: {query[:100]}")
                    
                    raise DatabaseTimeoutError(error_msg)
                    
                except Exception as e:
                    execution_time = time.time() - start_time
                    error_msg = f"Query execution failed: {str(e)}"
                    logger.error(f"Async query error: {error_msg}\nQuery: {query[:200]}")
                    
                    return QueryResult(
                        success=False,
                        data=[],
                        columns=[],
                        row_count=0,
                        execution_time=execution_time,
                        error_message=error_msg,
                        sql_query=query
                    )
    
    # -----------------------------------------------------------------
    # Schema Methods
    # -----------------------------------------------------------------
    
    def get_schema(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get MySQL database schema from INFORMATION_SCHEMA.
        
        Args:
            table_name: Optional specific table name
            
        Returns:
            Dictionary with schema information
        """
        try:
            if not self.is_connected:
                raise DatabaseConnectionError("Not connected to database")
            
            schema_info = {
                "database": self.config.get('database', 'unknown'),
                "db_type": "mysql",
                "mysql_version": self._mysql_version,
                "tables": []
            }
            
            # Build query based on whether we want specific table or all tables
            if table_name:
                table_query = """
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = %(database)s 
                    AND TABLE_NAME = %(table)s
                    AND TABLE_TYPE = 'BASE TABLE'
                """
                table_params = {"database": self.config['database'], "table": table_name}
            else:
                table_query = """
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = %(database)s 
                    AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME
                """
                table_params = {"database": self.config['database']}
            
            # Get tables
            tables_result = self.execute_query(table_query, table_params)
            
            for table_row in tables_result.data:
                table_name_val = table_row.get('TABLE_NAME')
                if table_name_val:  # Ensure it's not None
                    table_info = self._get_table_details(table_name_val)
                    schema_info["tables"].append(table_info)
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Error getting schema: {str(e)}")
            raise DatabaseQueryError(f"Failed to get schema: {str(e)}") from e
    
    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        """
        Get detailed schema for a specific MySQL table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableSchema object or None if table doesn't exist
        """
        try:
            # First check if table exists
            check_query = """
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = %(database)s 
                AND TABLE_NAME = %(table)s
            """
            check_result = self.execute_query(
                check_query, 
                {"database": self.config['database'], "table": table_name}
            )
            
            if not check_result.data:
                return None
            
            # Get table details
            table_details = self._get_table_details(table_name)
            
            # Get row count
            row_count_result = self.execute_query(
                f"SELECT COUNT(*) as count FROM `{table_name}`",
                max_rows=1
            )
            row_count = row_count_result.data[0].get('count', 0) if row_count_result.data else 0
            
            # Get CREATE TABLE statement
            create_result = self.execute_query(
                f"SHOW CREATE TABLE `{table_name}`",
                max_rows=1
            )
            create_statement = create_result.data[0].get('Create Table') if create_result.data else None
            
            # Build TableSchema object
            return TableSchema(
                name=table_name,
                columns=table_details['columns'],
                primary_keys=table_details['primary_keys'],
                foreign_keys=table_details['foreign_keys'],
                indexes=table_details['indexes'],
                row_count=row_count,
                create_statement=create_statement
            )
            
        except Exception as e:
            logger.error(f"Error getting table schema for {table_name}: {str(e)}")
            raise DatabaseQueryError(f"Failed to get table schema: {str(e)}") from e
    
    # -----------------------------------------------------------------
    # Connection Testing & Info Methods
    # -----------------------------------------------------------------
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test MySQL connection with a simple query.
        
        Returns:
            Tuple of (success, message)
        """
        try:
            if not self.is_connected:
                return False, "Not connected to database"
            
            result = self.execute_query("SELECT 1 as test", max_rows=1)
            
            if result.success and result.data and result.data[0].get('test') == 1:
                return True, f"Connection successful (MySQL {self._mysql_version})"
            else:
                return False, "Connection test query failed"
                
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"
    
    def get_database_info(self) -> Dict[str, Any]:
        """
        Get MySQL database metadata.
        
        Returns:
            Dictionary with database information
        """
        try:
            if not self.is_connected:
                raise DatabaseConnectionError("Not connected to database")
            
            info = {
                "name": self.config.get('database', 'unknown'),
                "type": "mysql",
                "version": self._mysql_version,
                "character_set": None,
                "collation": None,
                "size_mb": 0,
                "tables_count": 0,
                "connection": {
                    "host": self.config.get('host'),
                    "port": self.config.get('port', 3306),
                    "user": self.config.get('user')
                }
            }
            
            # Get character set and collation
            charset_result = self.execute_query(
                "SELECT @@character_set_database as charset, @@collation_database as collation",
                max_rows=1
            )
            if charset_result.data:
                info['character_set'] = charset_result.data[0].get('charset')
                info['collation'] = charset_result.data[0].get('collation')
            
            # Get database size
            size_query = """
                SELECT 
                    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) as size_mb
                FROM information_schema.TABLES 
                WHERE table_schema = %(database)s
            """
            size_result = self.execute_query(size_query, {"database": self.config['database']})
            if size_result.data:
                info['size_mb'] = size_result.data[0].get('size_mb', 0)
            
            # Get table count
            count_query = """
                SELECT COUNT(*) as table_count
                FROM information_schema.TABLES 
                WHERE table_schema = %(database)s AND TABLE_TYPE = 'BASE TABLE'
            """
            count_result = self.execute_query(count_query, {"database": self.config['database']})
            if count_result.data:
                info['tables_count'] = count_result.data[0].get('table_count', 0)
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting database info: {str(e)}")
            raise DatabaseQueryError(f"Failed to get database info: {str(e)}") from e
    
    # -----------------------------------------------------------------
    # ASYNC Schema & Info Methods
    # -----------------------------------------------------------------
    
    async def async_get_schema(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Asynchronously retrieve schema information from MySQL.
        
        Args:
            table_name: Optional specific table name
            
        Returns:
            Dictionary with schema information
        """
        try:
            schema_info = {
                "database": self.config.get('database', 'unknown'),
                "db_type": "mysql",
                "tables": []
            }
            
            if table_name:
                table_query = """
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = %(database)s 
                    AND TABLE_NAME = %(table)s
                    AND TABLE_TYPE = 'BASE TABLE'
                """
                table_params = {"database": self.config['database'], "table": table_name}
            else:
                table_query = """
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = %(database)s 
                    AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME
                """
                table_params = {"database": self.config['database']}
            
            tables_result = await self.async_execute_query(table_query, table_params)
            
            for table_row in tables_result.data:
                table_name_val = table_row.get('TABLE_NAME')
                if table_name_val:
                    table_info = self._get_table_details(table_name_val)
                    schema_info["tables"].append(table_info)
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Error async getting schema: {str(e)}")
            raise DatabaseQueryError(f"Failed to async get schema: {str(e)}") from e
    
    async def async_get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        """
        Asynchronously get detailed schema for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableSchema object or None if not found
        """
        try:
            check_query = """
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = %(database)s 
                AND TABLE_NAME = %(table)s
            """
            check_result = await self.async_execute_query(
                check_query, 
                {"database": self.config['database'], "table": table_name}
            )
            
            if not check_result.data:
                return None
            
            table_info = self._get_table_details(table_name)
            
            return TableSchema(
                name=table_name,
                columns=table_info.get('columns', []),
                primary_keys=table_info.get('primary_keys', []),
                foreign_keys=table_info.get('foreign_keys', []),
                indexes=table_info.get('indexes', []),
                row_count=table_info.get('row_count', 0)
            )
            
        except Exception as e:
            logger.error(f"Error async getting table schema: {str(e)}")
            raise DatabaseQueryError(f"Failed to async get table schema: {str(e)}") from e
    
    async def async_test_connection(self) -> Tuple[bool, str]:
        """
        Asynchronously test MySQL connection.
        
        Returns:
            Tuple of (success, message)
        """
        try:
            if not self.is_connected:
                return False, "Not connected to database"
            
            result = await self.async_execute_query("SELECT 1 as test", max_rows=1)
            
            if result.success and result.data and result.data[0].get('test') == 1:
                return True, f"Async connection successful (MySQL {self._mysql_version})"
            else:
                return False, "Async connection test query failed"
                
        except Exception as e:
            return False, f"Async connection test failed: {str(e)}"
    
    async def async_get_database_info(self) -> Dict[str, Any]:
        """
        Asynchronously get MySQL database metadata.
        
        Returns:
            Dictionary with database information
        """
        try:
            info = {
                "name": self.config.get('database', 'unknown'),
                "type": "mysql",
                "version": self._mysql_version or "Unknown"
            }
            
            charset_query = "SELECT @@character_set_database as charset, @@collation_database as collation"
            charset_result = await self.async_execute_query(charset_query)
            if charset_result.data:
                info['character_set'] = charset_result.data[0].get('charset')
                info['collation'] = charset_result.data[0].get('collation')
            
            size_query = """
                SELECT 
                    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) as size_mb
                FROM information_schema.TABLES 
                WHERE table_schema = %(database)s
            """
            size_result = await self.async_execute_query(size_query, {"database": self.config['database']})
            if size_result.data:
                info['size_mb'] = size_result.data[0].get('size_mb', 0)
            
            count_query = """
                SELECT COUNT(*) as table_count
                FROM information_schema.TABLES 
                WHERE table_schema = %(database)s AND TABLE_TYPE = 'BASE TABLE'
            """
            count_result = await self.async_execute_query(count_query, {"database": self.config['database']})
            if count_result.data:
                info['tables_count'] = count_result.data[0].get('table_count', 0)
            
            return info
            
        except Exception as e:
            logger.error(f"Error async getting database info: {str(e)}")
            raise DatabaseQueryError(f"Failed to async get database info: {str(e)}") from e
    
    # -----------------------------------------------------------------
    # Properties
    # -----------------------------------------------------------------
    
    @property
    def is_connected(self) -> bool:
        """
        Check if MySQL connection is active.
        
        Returns:
            bool: True if connected and responsive
        """
        if self._connection is None:
            return False
        
        try:
            # Try to ping the connection
            self._connection.ping(reconnect=False)
            return True
        except:
            self._is_connected = False
            return False
    
    @property
    def db_type(self) -> DatabaseType:
        """Return MySQL database type."""
        return DatabaseType.MYSQL
    
    @property
    def mysql_version(self) -> Optional[str]:
        """Get MySQL version string."""
        return self._mysql_version
    
    # -----------------------------------------------------------------
    # Helper Methods (Private)
    # -----------------------------------------------------------------
    
    def _get_password(self, config: Dict[str, Any]) -> str:
        """
        Extract password from config, handling environment variables.
        
        Args:
            config: Connection configuration
            
        Returns:
            Password string
        """
        password = config.get('password', '')
        
        # Check if password is an environment variable reference
        if isinstance(password, str) and password.startswith('env:'):
            env_var = password[4:]  # Remove 'env:' prefix
            password = os.getenv(env_var, '')
            if not password:
                raise DatabaseConfigError(f"Environment variable {env_var} not set")
        
        return password
    
    def _get_table_details(self, table_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table details
        """
        table_details = {
            "name": table_name,
            "columns": [],
            "primary_keys": [],
            "foreign_keys": [],
            "indexes": []
        }
        
        # Get columns
        columns_query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                COLUMN_TYPE,
                COLUMN_KEY,
                EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = %(database)s AND TABLE_NAME = %(table)s
            ORDER BY ORDINAL_POSITION
        """
        columns_result = self.execute_query(
            columns_query, 
            {"database": self.config['database'], "table": table_name}
        )
        
        for col in columns_result.data:
            column_info = {
                "name": col.get('COLUMN_NAME', ''),
                "type": col.get('DATA_TYPE', ''),
                "max_length": col.get('CHARACTER_MAXIMUM_LENGTH'),
                "nullable": col.get('IS_NULLABLE') == 'YES',
                "default": col.get('COLUMN_DEFAULT'),
                "column_type": col.get('COLUMN_TYPE', ''),
                "key": col.get('COLUMN_KEY', ''),
                "extra": col.get('EXTRA', '')
            }
            
            # Check if this column is a primary key
            if col.get('COLUMN_KEY') == 'PRI':
                pk_name = col.get('COLUMN_NAME')
                if pk_name:
                    table_details['primary_keys'].append(pk_name)
            
            table_details['columns'].append(column_info)
        
        # Get foreign keys
        fk_query = """
            SELECT 
                COLUMN_NAME,
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME,
                CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %(database)s 
            AND TABLE_NAME = %(table)s
            AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        fk_result = self.execute_query(
            fk_query,
            {"database": self.config['database'], "table": table_name}
        )
        
        for fk in fk_result.data:
            fk_info = {
                "column": fk.get('COLUMN_NAME'),
                "referenced_table": fk.get('REFERENCED_TABLE_NAME'),
                "referenced_column": fk.get('REFERENCED_COLUMN_NAME'),
                "constraint_name": fk.get('CONSTRAINT_NAME')
            }
            # Only add if we have all required fields
            if all(fk_info.values()):
                table_details['foreign_keys'].append(fk_info)
        
        # Get indexes (non-primary)
        index_query = """
            SELECT 
                INDEX_NAME,
                COLUMN_NAME,
                NON_UNIQUE,
                INDEX_TYPE
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = %(database)s 
            AND TABLE_NAME = %(table)s
            AND INDEX_NAME != 'PRIMARY'
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """
        index_result = self.execute_query(
            index_query,
            {"database": self.config['database'], "table": table_name}
        )
        
        current_index = None
        for idx in index_result.data:
            idx_name = idx.get('INDEX_NAME')
            if idx_name and idx_name != current_index:
                current_index = idx_name
                index_info = {
                    "name": idx_name,
                    "unique": idx.get('NON_UNIQUE', 1) == 0,
                    "type": idx.get('INDEX_TYPE', ''),
                    "columns": [idx.get('COLUMN_NAME', '')]
                }
                table_details['indexes'].append(index_info)
            elif current_index and idx.get('COLUMN_NAME'):
                # Add column to existing index
                table_details['indexes'][-1]['columns'].append(idx.get('COLUMN_NAME', ''))
        
        return table_details
    
    def __str__(self) -> str:
        """User-friendly string representation."""
        host = self.config.get('host', 'unknown')
        db = self.config.get('database', 'unknown')
        return f"MySQL Connection: {self.connection_name} ({host}/{db})"


# Convenience function to create MySQL connection
def create_mysql_connection(connection_name: str, config: Dict[str, Any]) -> MySQLConnection:
    """
    Create and connect a MySQL connection in one step.
    
    Args:
        connection_name: Unique name for the connection
        config: MySQL connection configuration
        
    Returns:
        Connected MySQLConnection instance
    """
    conn = MySQLConnection(connection_name)
    conn.connect(config)
    return conn

# Overview 
# This code implements a complete MySQL database connection system that follows the abstract blueprint defined in base.py. It provides real, working methods to connect to MySQL databases, execute queries with safety limits, extract schema information, and handle errors gracefully using the PyMySQL library.

#  Core Components
# Connection Management: Establishes and closes MySQL connections with proper error handling and configuration validation

# import pymysql
# import pymysql.cursors
# What: We're using the pymysql library - it's the tool that actually talks to MySQL.

# pymysql.cursors contains different cursor classes. Cursors are objects that let you execute SQL commands and fetch results. Importing it separately gives us access to DictCursor which returns results as dictionaries instead of tuples.

# Type Hint Imports
# from typing import Dict, List, Any, Optional, Tuple, cast
# Dict: Type hint for dictionaries, e.g., Dict[str, int] means dict with string keys and integer values

# Any: Means "any type" - used when we don't know or don't care about the specific type

# Tuple: Type hint for tuples, e.g., Tuple[int, str] means tuple with int then string

# .base means import from base.py in the same folder

# logger = logging.getLogger(__name__)
# This logger will be used throughout the module to record events at different levels

# class MySQLConnection(DatabaseConnection): defines a new class that inherits from DatabaseConnection

# def __init__(self, connection_name: str):
# super().__init__(connection_name) calls the parent class (DatabaseConnection) constructor
# First, follow the BASIC instructions from Book 1, THEN add our special MySQL features.

# None - connection is not there, When you want to ask the database a question, you use the cursor - None = The robot's mouth is CLOSED

# Connect Method Signature
# def connect(self, config: Dict[str, Any]) -> bool:
# Establish connection to MySQL database.

# password = self._get_password(config)
# Calls the private helper method _get_password to extract the password
# This method handles both direct passwords and environment variable references

# Build Connection Parameters
#conn_params - Creates a dictionary with all connection parameters PyMySQL needs
# pymysql.cursors.DictCursor tells PyMySQL to return results as dictionaries

# Optional SSL and Timeout
# SSL encrypts the data sent between

# self.config = config saves the config for potential reconnection
# pymysql.connect(**conn_params) is the actual connection call
# **conn_params unpacks the dictionary as keyword arguments: connect(host=..., user=..., etc.)

# Get MySQL version
# with self._connection.cursor(pymysql.cursors.DictCursor) as cursor:
# Hey robot, get ready to ask a question! Use your special 'DictCursor' mouth 🗣️ (which means: give me answers as dictionaries, not boring lists).
# Why with?:
# with is like saying: "Only use this mouth TEMPORARILY, then close it automatically!"

# Your robot asks the database:
# cursor.execute("SELECT VERSION() as version")

# result = cursor.fetchone()
#  The robot reaches out and GRABS THE ANSWER with its hand! ✋
# fetch = "Grab/get"
# one = "Just one answer"

# self._mysql_version = result.get('version') if result else "Unknown"

# def disconnect(self) -> None:

# Execute Query Method Signature
# execute_query


    # def execute_query(
    #     self, 
    #     query: str, 
    #     params: Optional[Dict[str, Any]] = None,
    #     max_rows: int = 1000,
    #     timeout: int = 30
    # ) -> QueryResult:
# query: str,
# query = The QUESTION you want to ask the database

# : str = MUST BE A STRING (text, like "SELECT * FROM users")

# Dict[str, Any]
# Dict = DICTIONARY (a collection of key-value pairs)
# [str, Any] = Keys are strings (str), Values can be anything (Any)

# max_rows: int = 1000,
# max_rows = MAXIMUM ROWS to return

# assert cursor is not None, "Failed to create cursor"
# "I SWEAR this thing should be TRUE! If it's not, SOMETHING IS TERRIBLY WRONG - STOP EVERYTHING!"

