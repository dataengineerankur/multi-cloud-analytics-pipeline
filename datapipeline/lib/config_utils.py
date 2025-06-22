"""
Configuration Utility Functions for Psycho Bunny Data Pipeline
"""

import json
import os
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from JSON file
    
    Args:
        config_path (str): Path to configuration file
    
    Returns:
        dict: Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        raise

def get_env_config() -> Dict[str, str]:
    """
    Get environment-specific configuration
    
    Returns:
        dict: Environment configuration
    """
    return {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST"),
        "DATABRICKS_TOKEN": os.getenv("DATABRICKS_TOKEN"),
        "REDSHIFT_HOST": os.getenv("REDSHIFT_HOST"),
        "REDSHIFT_DATABASE": os.getenv("REDSHIFT_DATABASE"),
        "REDSHIFT_USER": os.getenv("REDSHIFT_USER"),
        "REDSHIFT_PASSWORD": os.getenv("REDSHIFT_PASSWORD"),
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
        "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE")
    }

def validate_config(config: Dict[str, Any], required_keys: list) -> bool:
    """
    Validate that required keys exist in configuration
    
    Args:
        config (dict): Configuration dictionary
        required_keys (list): List of required keys
    
    Returns:
        bool: True if all required keys exist
    """
    missing_keys = []
    for key in required_keys:
        if key not in config:
            missing_keys.append(key)
    
    if missing_keys:
        logger.error(f"Missing required configuration keys: {missing_keys}")
        return False
    
    logger.info("Configuration validation passed")
    return True

def get_s3_config(config: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract S3 configuration
    
    Args:
        config (dict): Full configuration
    
    Returns:
        dict: S3 configuration
    """
    aws_config = config.get("aws", {})
    return {
        "bucket": aws_config.get("s3_bucket"),
        "landing_zone": aws_config.get("s3_paths", {}).get("landing_zone"),
        "raw_data": aws_config.get("s3_paths", {}).get("raw_data"),
        "processed_data": aws_config.get("s3_paths", {}).get("processed_data"),
        "quality_reports": aws_config.get("s3_paths", {}).get("quality_reports")
    }

def get_databricks_config(config: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract Databricks configuration
    
    Args:
        config (dict): Full configuration
    
    Returns:
        dict: Databricks configuration
    """
    return config.get("databricks", {})

def get_data_quality_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract data quality configuration
    
    Args:
        config (dict): Full configuration
    
    Returns:
        dict: Data quality configuration
    """
    return config.get("data_quality", {})

def get_business_rules(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract business rules configuration
    
    Args:
        config (dict): Full configuration
    
    Returns:
        dict: Business rules configuration
    """
    return config.get("business_rules", {})

def setup_logging(log_level: str = "INFO") -> None:
    """
    Setup logging configuration
    
    Args:
        log_level (str): Logging level
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger.info("Logging configured")

def merge_configs(*configs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge multiple configuration dictionaries
    
    Args:
        *configs: Variable number of configuration dictionaries
    
    Returns:
        dict: Merged configuration
    """
    merged = {}
    for config in configs:
        if config:
            merged.update(config)
    return merged

class ConfigManager:
    """Configuration manager class for centralized config handling"""
    
    def __init__(self, config_path: str):
        """
        Initialize configuration manager
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config_path = config_path
        self.config = self.load_config()
        self.env_config = get_env_config()
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from file"""
        return load_config(self.config_path)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value
        
        Args:
            key (str): Configuration key
            default: Default value if key not found
        
        Returns:
            Configuration value
        """
        return self.config.get(key, default)
    
    def get_env(self, key: str, default: str = None) -> str:
        """
        Get environment variable
        
        Args:
            key (str): Environment variable name
            default (str): Default value if not found
        
        Returns:
            str: Environment variable value
        """
        return self.env_config.get(key, default)
    
    def reload(self) -> None:
        """Reload configuration from file"""
        self.config = self.load_config()
        logger.info("Configuration reloaded") 