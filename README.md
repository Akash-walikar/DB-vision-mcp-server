# Database Explorer MCP Server

A powerful Model Context Protocol (MCP) server for exploring and querying databases using natural language. This server provides tools for connecting to databases, exploring schemas, and executing queries with AI-assisted natural language to SQL conversion.

## 🚀 Features

- **Multi-Database Support**: Currently supports MySQL (extensible to PostgreSQL, SQLite, etc.)
- **Natural Language Queries**: Convert natural language questions to SQL using client-side AI
- **Schema Exploration**: Discover database structure, tables, columns, relationships, and indexes
- **Safe Query Execution**: Built-in safety limits (row limits, timeouts) to prevent resource exhaustion
- **Connection Management**: Manage multiple database connections simultaneously
- **MCP Protocol**: Full Model Context Protocol implementation for seamless AI integration

## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
- [API Reference](#api-reference)
- [Architecture](#architecture)
- [Development](#development)
- [Contributing](#contributing)

## 🔧 Installation

### Prerequisites

- Python 3.10 or higher
- A MySQL database (or other supported database)

### Install Dependencies

Using `uv` (recommended):
```bash
uv sync
```

Or using `pip`:
```bash
pip install -e .
```

### Dependencies

- `fastmcp>=2.14.3` - FastMCP framework for MCP servers
- `pymysql>=1.1.2` - MySQL database connector

## 🏃 Quick Start

### 1. Create a Database Connection Config

Create a connection configuration file in `src/config/connections/`:

```bash
mkdir -p src/config/connections
```

Example: `src/config/connections/my_database.json`

```json
{
  "type": "mysql",
  "host": "localhost",
  "port": 3306,
  "user": "your_username",
  "password": "your_password",
  "database": "your_database",
  "charset": "utf8mb4"
}
```

**Security Note**: For production, use environment variables for passwords:
```json
{
  "type": "mysql",
  "host": "localhost",
  "user": "your_username",
  "password": "env:DB_PASSWORD",
  "database": "your_database"
}
```

### 2. Start the MCP Server

```bash
python main.py
```

The server runs on stdio and is ready to accept MCP protocol requests.

### 3. Use with MCP Clients

This server is designed to work with MCP-compatible clients like:
- Claude Desktop
- Custom MCP clients
- AI assistants that support MCP

## 📖 Usage

### Connection Management

#### List Available Connections
```python
# Returns all configured connections and their status
list_connections_tool()
```

#### Connect to a Database
```python
connect_tool("my_database")
```

#### Test Connection
```python
test_connection_tool("my_database")
```

#### Get Database Information
```python
get_database_info_tool("my_database")
# Returns: version, size, character set, table count, etc.
```

#### Disconnect
```python
disconnect_tool("my_database")
```

### Schema Exploration

#### List All Tables
```python
list_tables_tool("my_database")
```

#### Get Full Schema
```python
# Get schema for all tables
get_schema_tool("my_database")

# Get schema for specific table
get_schema_tool("my_database", table_name="users")
```

#### Get Detailed Table Information
```python
get_table_info_tool("my_database", "users")
# Returns: columns, primary keys, foreign keys, indexes, row count
```

### Query Execution

#### Natural Language Query (AI-Assisted)

The server provides schema context for your AI to generate SQL:

```python
result = natural_language_query_tool(
    connection_name="my_database",
    question="Show me all customers from New York",
    max_rows=100
)
```

**Response includes:**
- `schema_context`: Complete database schema
- `prompt`: Ready-to-use prompt for your AI/LLM
- `instructions`: Step-by-step workflow

**Workflow:**
1. Server extracts schema and creates prompt
2. Client uses prompt with AI to generate SQL
3. Client calls `execute_sql_query_tool()` with generated SQL

#### Execute SQL Query

```python
execute_sql_query_tool(
    connection_name="my_database",
    sql_query="SELECT * FROM users WHERE city = 'New York' LIMIT 100",
    max_rows=1000,
    timeout=30
)
```

**Safety Features:**
- Automatic row limiting (`max_rows`)
- Query timeout protection (`timeout`)
- Parameterized queries to prevent SQL injection
- Read-only enforcement for natural language queries

## 📚 API Reference

### Connection Tools

#### `list_connections_tool() -> Dict[str, Any]`
List all available database connections.

#### `connect_tool(connection_name: str) -> Dict[str, Any]`
Connect to a database using a configuration file.

#### `disconnect_tool(connection_name: str) -> Dict[str, Any]`
Disconnect from a database.

#### `test_connection_tool(connection_name: str) -> Dict[str, Any]`
Test if a database connection is alive.

#### `get_database_info_tool(connection_name: str) -> Dict[str, Any]`
Get metadata about the connected database.

### Schema Tools

#### `get_schema_tool(connection_name: str, table_name: Optional[str] = None) -> Dict[str, Any]`
Get database schema information for all tables or a specific table.

#### `list_tables_tool(connection_name: str) -> Dict[str, Any]`
List all tables in the database.

#### `get_table_info_tool(connection_name: str, table_name: str) -> Dict[str, Any]`
Get detailed information about a specific table.

### Query Tools

#### `natural_language_query_tool(connection_name: str, question: str, max_rows: int = 100) -> Dict[str, Any]`
Convert natural language to SQL using client's AI. Returns schema context and prompt.

#### `execute_sql_query_tool(connection_name: str, sql_query: str, max_rows: int = 1000, timeout: int = 30) -> Dict[str, Any]`
Execute a SQL query on the database.

### MCP Resources

The server also exposes MCP resources for schema caching:

- `schema://{connection_name}` - Full database schema
- `schema://{connection_name}/{table_name}` - Specific table schema
- `connections://list` - List of all connections

## 🏗️ Architecture

### Project Structure

```
EDAEDA/
├── main.py                 # MCP server entry point
├── pyproject.toml          # Project configuration
├── README.md               # This file
│
└── src/
    ├── __init__.py
    │
    ├── database/           # Database abstraction layer
    │   ├── __init__.py
    │   ├── base.py         # Abstract base class
    │   └── mysql.py        # MySQL implementation
    │
    ├── tools/              # MCP tools
    │   ├── __init__.py
    │   ├── connection_tools.py  # Connection management
    │   ├── query_tools.py       # Query execution
    │   └── schema_tools.py      # Schema exploration
    │
    └── config/
        └── connections/    # Connection config files
            └── *.json
```

### Design Principles

1. **Abstraction**: Database-agnostic interface via `DatabaseConnection` base class
2. **Safety**: Built-in limits (row limits, timeouts) prevent resource exhaustion
3. **Extensibility**: Easy to add new database types (PostgreSQL, SQLite, etc.)
4. **Type Safety**: Full type hints for better IDE support and error detection
5. **Error Handling**: Comprehensive exception handling with custom error types

### Database Connection Flow

```
1. Client calls connect_tool("my_db")
2. Server loads config from src/config/connections/my_db.json
3. Server creates MySQLConnection instance
4. Server establishes connection using PyMySQL
5. Server stores connection in _active_connections
6. Client can now use other tools with "my_db"
```

### Natural Language Query Flow

```
1. Client: natural_language_query_tool("my_db", "Show customers from NY")
2. Server: Extracts schema, formats context, creates prompt
3. Server: Returns prompt + schema_context
4. Client: Uses prompt with AI/LLM to generate SQL
5. Client: execute_sql_query_tool("my_db", generated_sql)
6. Server: Executes SQL with safety limits
7. Server: Returns results
```

## 🔒 Security Features

- **Parameterized Queries**: All queries use parameterized statements to prevent SQL injection
- **Read-Only Enforcement**: Natural language queries only generate SELECT statements
- **Row Limits**: Automatic limiting of result sets
- **Timeouts**: Query execution timeouts prevent hanging operations
- **Environment Variables**: Support for secure password storage via env vars

## 🛠️ Development

### Running Tests

```bash
# Run connection test
python test_mysql_connection.py
```

### Code Quality

The project uses:
- Type hints throughout
- Comprehensive docstrings
- Logging for debugging
- Error handling with custom exceptions

### Adding New Database Types

1. Create a new class inheriting from `DatabaseConnection`
2. Implement all abstract methods
3. Add to `DatabaseType` enum
4. Update connection factory in `connection_tools.py`

Example structure:
```python
class PostgreSQLConnection(DatabaseConnection):
    def connect(self, config: Dict[str, Any]) -> bool:
        # Implementation
        pass
    
    # ... implement other abstract methods
```

## 📝 Configuration Examples

### MySQL Connection

```json
{
  "type": "mysql",
  "host": "localhost",
  "port": 3306,
  "user": "root",
  "password": "env:MY_DB_PASSWORD",
  "database": "mydb",
  "charset": "utf8mb4",
  "ssl": false,
  "connect_timeout": 10
}
```

### Remote MySQL with SSL

```json
{
  "type": "mysql",
  "host": "db.example.com",
  "port": 3306,
  "user": "app_user",
  "password": "env:PROD_DB_PASSWORD",
  "database": "production_db",
  "charset": "utf8mb4",
  "ssl": true,
  "connect_timeout": 30
}
```

## 🐛 Troubleshooting

### Connection Issues

**Error: "Config file not found"**
- Ensure config file exists in `src/config/connections/`
- Check file name matches connection name exactly

**Error: "Environment variable not set"**
- Set the environment variable: `export DB_PASSWORD=your_password`
- Or use direct password in config (not recommended for production)

**Error: "Connection refused"**
- Verify database is running
- Check host, port, and firewall settings
- Verify credentials

### Query Issues

**Error: "Query exceeded timeout"**
- Increase `timeout` parameter
- Optimize your query
- Check database performance

**Warning: "Results may be truncated"**
- Increase `max_rows` parameter if needed
- This is a safety feature to prevent large result sets

## 🤝 Contributing

Contributions are welcome! Areas for improvement:

- Additional database support (PostgreSQL, SQLite, etc.)
- Query optimization suggestions
- Enhanced schema analysis
- Performance improvements
- Documentation improvements

## 📄 License

## 🙏 Acknowledgments

- Built with [FastMCP](https://github.com/jlowin/fastmcp)
- MySQL connectivity via [PyMySQL](https://github.com/PyMySQL/PyMySQL)
- Follows [Model Context Protocol](https://modelcontextprotocol.io/) specification

## 📞 Support

For issues, questions, or contributions, please open an issue on the repository.

---

**Version**: 0.1.0  
**Python**: 3.10+  
**Status**: Active Development
