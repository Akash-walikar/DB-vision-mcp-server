"""
Connection Tools for Database Explorer MCP Server.

Tools for managing database connections (both sync and async).
"""

from typing import Dict, Any, List, Optional
import json
import os
import logging
import asyncio
from pathlib import Path
from ..database.mysql import MySQLConnection, create_mysql_connection

logger = logging.getLogger(__name__)

# We'll store active connections here
_active_connections: Dict[str, MySQLConnection] = {}
_async_active_connections: Dict[str, MySQLConnection] = {}

# Config directory
CONFIG_DIR = Path(__file__).parent.parent / "config" / "connections"


def get_connection(connection_name: str) -> Optional[MySQLConnection]:
    """Get an active database connection by name."""
    return _active_connections.get(connection_name)


def list_connections() -> Dict[str, Any]:
    """
    List all available database connections.
    
    Returns:
        Dictionary with connection information
    """
    try:
        # Ensure config directory exists
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        
        # List config files
        config_files = list(CONFIG_DIR.glob("*.json"))
        
        connections = []
        for config_file in config_files:
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                
                connection_name = config_file.stem  # Filename without .json
                is_active = connection_name in _active_connections
                
                connection_info = {
                    "name": connection_name,
                    "type": config.get("type", "unknown"),
                    "host": config.get("host", "unknown"),
                    "database": config.get("database", "unknown"),
                    "config_file": str(config_file),
                    "status": "active" if is_active else "inactive",
                    "port": config.get("port", 3306)
                }
                connections.append(connection_info)
                
            except Exception as e:
                logger.warning(f"Failed to read config {config_file}: {e}")
        
        return {
            "success": True,
            "connections": connections,
            "active_count": len(_active_connections),
            "config_dir": str(CONFIG_DIR),
            "note": "Use connect() to activate a connection"
        }
        
    except Exception as e:
        logger.error(f"Connection listing failed: {e}")
        return {
            "success": False,
            "error": f"Connection listing failed: {str(e)}"
        }


def connect(
    connection_name: str,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Connect to a database.
    
    Args:
        connection_name: Name for this connection
        config: Optional connection configuration (if not provided, loads from config file)
        
    Returns:
        Dictionary with connection status
    """
    try:
        # Check if already connected
        if connection_name in _active_connections:
            return {
                "success": False,
                "error": f"Connection '{connection_name}' is already active",
                "suggestion": "Use disconnect() first or use a different name"
            }
        
        connection_config: Dict[str, Any]
        
        # Load config from file if not provided
        if config is None:
            config_file = CONFIG_DIR / f"{connection_name}.json"
            if not config_file.exists():
                return {
                    "success": False,
                    "error": f"Config file not found: {config_file}",
                    "suggestion": f"Create {config_file} or provide config parameter"
                }
            
            with open(config_file, 'r') as f:
                connection_config = json.load(f)
        else:
            connection_config = config
        
        # Determine connection type
        db_type = connection_config.get("type", "mysql").lower()
        
        if db_type == "mysql":
            # Create and connect
            db = create_mysql_connection(connection_name, connection_config)
            _active_connections[connection_name] = db
            
            # Test the connection
            success, message = db.test_connection()
            
            return {
                "success": success,
                "message": message,
                "connection_name": connection_name,
                "database": connection_config.get("database"),
                "host": connection_config.get("host"),
                "mysql_version": db.mysql_version,
                "active_connections": list(_active_connections.keys())
            }
        else:
            return {
                "success": False,
                "error": f"Unsupported database type: {db_type}",
                "supported_types": ["mysql"]
            }
        
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return {
            "success": False,
            "error": f"Connection failed: {str(e)}",
            "connection_name": connection_name
        }


def disconnect(connection_name: str) -> Dict[str, Any]:
    """
    Disconnect from a database.
    
    Args:
        connection_name: Name of the connection to disconnect
        
    Returns:
        Dictionary with disconnection status
    """
    try:
        if connection_name not in _active_connections:
            return {
                "success": False,
                "error": f"No active connection named '{connection_name}'",
                "active_connections": list(_active_connections.keys())
            }
        
        # Disconnect
        db = _active_connections[connection_name]
        db.disconnect()
        
        # Remove from active connections
        del _active_connections[connection_name]
        
        return {
            "success": True,
            "message": f"Disconnected from '{connection_name}'",
            "active_connections": list(_active_connections.keys())
        }
        
    except Exception as e:
        logger.error(f"Disconnection failed: {e}")
        return {
            "success": False,
            "error": f"Disconnection failed: {str(e)}"
        }


def test_connection(connection_name: str) -> Dict[str, Any]:
    """
    Test a database connection.
    
    Args:
        connection_name: Name of the connection to test
        
    Returns:
        Dictionary with test results
    """
    try:
        db = get_connection(connection_name)
        if db is None:
            return {
                "success": False,
                "error": f"No active connection named '{connection_name}'",
                "suggestion": "Use connect() first"
            }
        
        success, message = db.test_connection()
        
        return {
            "success": success,
            "message": message,
            "connection_name": connection_name,
            "is_connected": db.is_connected,
            "db_type": str(db.db_type)
        }
        
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return {
            "success": False,
            "error": f"Connection test failed: {str(e)}"
        }


def get_database_info(connection_name: str) -> Dict[str, Any]:
    """
    Get information about the connected database.
    
    Args:
        connection_name: Name of the connection
        
    Returns:
        Dictionary with database information
    """
    try:
        db = get_connection(connection_name)
        if db is None:
            return {
                "success": False,
                "error": f"No active connection named '{connection_name}'"
            }
        
        info = db.get_database_info()
        
        return {
            "success": True,
            **info,
            "connection_name": connection_name
        }
        
    except Exception as e:
        logger.error(f"Database info retrieval failed: {e}")
        return {
            "success": False,
            "error": f"Database info retrieval failed: {str(e)}"
        }


# -----------------------------------------------------------------
# ASYNC Connection Tools
# -----------------------------------------------------------------

async def async_connect(
    connection_name: str,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Asynchronously connect to a database.
    
    Args:
        connection_name: Name for this connection
        config: Optional connection configuration
        
    Returns:
        Dictionary with connection status
    """
    try:
        if connection_name in _async_active_connections:
            return {
                "success": False,
                "error": f"Async connection '{connection_name}' is already active",
                "suggestion": "Use async_disconnect() first"
            }
        
        connection_config: Dict[str, Any]
        
        if config is None:
            config_file = CONFIG_DIR / f"{connection_name}.json"
            if not config_file.exists():
                return {
                    "success": False,
                    "error": f"Config file not found: {config_file}"
                }
            
            with open(config_file, 'r') as f:
                connection_config = json.load(f)
        else:
            connection_config = config
        
        db_type = connection_config.get("type", "mysql").lower()
        
        if db_type == "mysql":
            db = create_mysql_connection(connection_name, connection_config)
            
            # Async connect
            await db.async_connect(connection_config)
            _async_active_connections[connection_name] = db
            
            success, message = await db.async_test_connection()
            
            return {
                "success": success,
                "message": message,
                "connection_name": connection_name,
                "database": connection_config.get("database"),
                "host": connection_config.get("host"),
                "async_active_connections": list(_async_active_connections.keys())
            }
        else:
            return {
                "success": False,
                "error": f"Unsupported database type: {db_type}"
            }
        
    except Exception as e:
        logger.error(f"Async connection failed: {e}")
        return {
            "success": False,
            "error": f"Async connection failed: {str(e)}"
        }


async def async_disconnect(connection_name: str) -> Dict[str, Any]:
    """
    Asynchronously disconnect from a database.
    
    Args:
        connection_name: Name of the connection to disconnect
        
    Returns:
        Dictionary with disconnection status
    """
    try:
        if connection_name not in _async_active_connections:
            return {
                "success": False,
                "error": f"No async active connection named '{connection_name}'",
                "async_active_connections": list(_async_active_connections.keys())
            }
        
        db = _async_active_connections[connection_name]
        await db.async_disconnect()
        
        del _async_active_connections[connection_name]
        
        return {
            "success": True,
            "message": f"Async disconnected from '{connection_name}'",
            "async_active_connections": list(_async_active_connections.keys())
        }
        
    except Exception as e:
        logger.error(f"Async disconnection failed: {e}")
        return {
            "success": False,
            "error": f"Async disconnection failed: {str(e)}"
        }


async def async_test_connection(connection_name: str) -> Dict[str, Any]:
    """
    Asynchronously test a database connection.
    
    Args:
        connection_name: Name of the connection to test
        
    Returns:
        Dictionary with test results
    """
    try:
        if connection_name not in _async_active_connections:
            return {
                "success": False,
                "error": f"No async active connection named '{connection_name}'"
            }
        
        db = _async_active_connections[connection_name]
        success, message = await db.async_test_connection()
        
        return {
            "success": success,
            "message": message,
            "connection_name": connection_name,
            "is_connected": db.is_connected,
            "db_type": str(db.db_type)
        }
        
    except Exception as e:
        logger.error(f"Async connection test failed: {e}")
        return {
            "success": False,
            "error": f"Async connection test failed: {str(e)}"
        }


async def async_get_database_info(connection_name: str) -> Dict[str, Any]:
    """
    Asynchronously get information about the connected database.
    
    Args:
        connection_name: Name of the connection
        
    Returns:
        Dictionary with database information
    """
    try:
        if connection_name not in _async_active_connections:
            return {
                "success": False,
                "error": f"No async active connection named '{connection_name}'"
            }
        
        db = _async_active_connections[connection_name]
        info = await db.async_get_database_info()
        
        return {
            "success": True,
            **info,
            "connection_name": connection_name
        }
        
    except Exception as e:
        logger.error(f"Async database info retrieval failed: {e}")
        return {
            "success": False,
            "error": f"Async database info retrieval failed: {str(e)}"
        }