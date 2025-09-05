"""
Configuration Integration Module
===============================

This module provides backward compatibility between the new Pydantic-based
configuration system and the existing OnPatrolServer.py code. It handles
the migration and provides a seamless interface for accessing configuration
data in both old and new formats.

Features:
- Seamless migration from old CONFIG dictionary to new Pydantic models
- Backward compatibility with existing code
- Automatic validation and type conversion
- Environment variable support
- Configuration file format detection and loading

Author: OnPatrol Team  
Version: 2.0.0 (Modernized for Python 3.13)
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union

# MODERNIZED FOR PYTHON 3.13: Import the new configuration system
try:
    from config import (
        OnPatrolConfig, 
        load_config_from_ini, 
        save_config_to_ini,
        migrate_legacy_config,
        get_config
    )
    PYDANTIC_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import modern configuration system: {e}")
    print("Falling back to legacy configuration...")
    PYDANTIC_AVAILABLE = False
    OnPatrolConfig = None

logger = logging.getLogger('config_integration')


class ConfigurationManager:
    """
    Configuration Manager that bridges old and new configuration systems
    
    This class provides a unified interface for configuration management
    that works with both the legacy CONFIG dictionary system and the new
    Pydantic-based system. It automatically detects which system to use
    and provides seamless migration capabilities.
    """
    
    def __init__(self):
        self._modern_config: Optional[OnPatrolConfig] = None
        self._legacy_config: Optional[Dict[str, Any]] = None
        self._using_modern = PYDANTIC_AVAILABLE
        self._config_file_path: Optional[str] = None
        
    @property
    def using_modern_config(self) -> bool:
        """Check if using modern Pydantic configuration"""
        return self._using_modern and self._modern_config is not None
    
    def load_configuration(self, config_file_path: str, legacy_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Load configuration from file or migrate from legacy config
        
        Args:
            config_file_path: Path to config.ini file
            legacy_config: Optional legacy CONFIG dictionary to migrate
            
        Returns:
            Dict[str, Any]: Configuration in legacy format for backward compatibility
        """
        self._config_file_path = config_file_path
        
        if PYDANTIC_AVAILABLE and self._using_modern:
            try:
                return self._load_modern_config(config_file_path, legacy_config)
            except Exception as e:
                logger.warning(f"Failed to load modern config: {e}")
                logger.warning("Falling back to legacy configuration system...")
                self._using_modern = False
        
        # Fallback to legacy system or if modern system is not available
        return self._load_legacy_config(config_file_path, legacy_config)
    
    def _load_modern_config(self, config_file_path: str, legacy_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Load configuration using modern Pydantic system"""
        
        if legacy_config:
            # Migrate from existing legacy config
            logger.info("🔄 Migrating from legacy configuration to modern system...")
            
            # Create a working copy to avoid modifying the original
            working_config = {}
            for section, data in legacy_config.items():
                if isinstance(data, dict):
                    working_config[section] = data.copy()
                else:
                    working_config[section] = data
            
            self._modern_config = migrate_legacy_config(working_config)
        else:
            # Load from file
            if os.path.exists(config_file_path):
                logger.info(f"📖 Loading configuration from {config_file_path}")
                self._modern_config = load_config_from_ini(config_file_path)
            else:
                logger.info("🆕 Creating new configuration with defaults")
                self._modern_config = OnPatrolConfig()
        
        # Convert back to legacy format for backward compatibility
        self._legacy_config = self._modern_config.get_legacy_config()
        
        # Merge any additional legacy config values that might be missing
        if legacy_config:
            for section, data in legacy_config.items():
                if section in self._legacy_config and isinstance(data, dict):
                    for key, value in data.items():
                        if key not in self._legacy_config[section]:
                            self._legacy_config[section][key] = value
                elif section not in self._legacy_config:
                    self._legacy_config[section] = data
        
        # Save the modern config back to INI format for persistence
        if config_file_path:
            try:
                save_config_to_ini(self._modern_config, config_file_path)
                logger.info(f"💾 Configuration saved to {config_file_path}")
            except Exception as e:
                logger.warning(f"Could not save config to {config_file_path}: {e}")
        
        logger.info("✅ Modern configuration system initialized successfully")
        return self._legacy_config
    
    def _load_legacy_config(self, config_file_path: str, legacy_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Fallback to legacy configuration loading"""
        
        if legacy_config:
            self._legacy_config = legacy_config.copy()
        else:
            # This would be the original legacy loading logic
            # For now, return empty config as placeholder
            logger.warning("Legacy configuration loading not implemented in integration layer")
            self._legacy_config = {}
        
        return self._legacy_config
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get configuration in legacy format
        
        Returns:
            Dict[str, Any]: Configuration dictionary compatible with existing code
        """
        if self._legacy_config is None:
            raise RuntimeError("Configuration not loaded. Call load_configuration() first.")
        
        return self._legacy_config
    
    def get_modern_config(self) -> Optional[OnPatrolConfig]:
        """
        Get modern Pydantic configuration
        
        Returns:
            Optional[OnPatrolConfig]: Modern configuration or None if not available
        """
        return self._modern_config
    
    def update_config(self, section: str, key: str, value: Any) -> None:
        """
        Update configuration value
        
        Args:
            section: Configuration section name
            key: Configuration key
            value: New value
        """
        if self._legacy_config is None:
            raise RuntimeError("Configuration not loaded")
        
        # Update legacy config
        if section not in self._legacy_config:
            self._legacy_config[section] = {}
        self._legacy_config[section][key] = value
        
        # Update modern config if available
        if self._modern_config is not None:
            try:
                # This would need more sophisticated mapping for complex updates
                # For now, just recreate from legacy config
                self._modern_config = OnPatrolConfig.from_legacy_config(self._legacy_config)
            except Exception as e:
                logger.warning(f"Could not sync modern config: {e}")
    
    def save_configuration(self, config_file_path: Optional[str] = None) -> None:
        """
        Save current configuration to file
        
        Args:
            config_file_path: Optional path to save to (uses loaded path if not specified)
        """
        save_path = config_file_path or self._config_file_path
        if not save_path:
            raise ValueError("No configuration file path specified")
        
        if self._modern_config is not None:
            save_config_to_ini(self._modern_config, save_path)
        else:
            logger.warning("Modern config not available, cannot save configuration")
    
    def validate_configuration(self) -> bool:
        """
        Validate current configuration
        
        Returns:
            bool: True if configuration is valid
        """
        if self._modern_config is not None:
            try:
                # Pydantic automatically validates on creation/update
                return True
            except Exception as e:
                logger.error(f"Configuration validation failed: {e}")
                return False
        else:
            # For legacy config, basic validation would go here
            return self._legacy_config is not None
    
    def get_configuration_info(self) -> Dict[str, Any]:
        """
        Get information about the current configuration system
        
        Returns:
            Dict[str, Any]: Configuration system information
        """
        return {
            'using_modern': self.using_modern_config,
            'pydantic_available': PYDANTIC_AVAILABLE,
            'config_loaded': self._legacy_config is not None,
            'modern_config_available': self._modern_config is not None,
            'config_file_path': self._config_file_path,
            'sections_loaded': list(self._legacy_config.keys()) if self._legacy_config else []
        }


# MODERNIZED FOR PYTHON 3.13: Global configuration manager instance
_config_manager: Optional[ConfigurationManager] = None


def get_configuration_manager() -> ConfigurationManager:
    """
    Get the global configuration manager instance
    
    Returns:
        ConfigurationManager: The global configuration manager
    """
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigurationManager()
    return _config_manager


def migrate_config_for_onpatrol(legacy_config: Dict[str, Any], config_file_path: str) -> Dict[str, Any]:
    """
    Migration function specifically for OnPatrolServer.py integration
    
    This function provides a drop-in replacement for the configuration loading
    in OnPatrolServer.py. It attempts to use the modern system but falls back
    gracefully to the legacy system if needed.
    
    Args:
        legacy_config: The existing CONFIG dictionary from OnPatrolServer.py
        config_file_path: Path to the config.ini file
        
    Returns:
        Dict[str, Any]: Configuration dictionary in the original format
    """
    
    manager = get_configuration_manager()
    
    try:
        # Attempt to load/migrate using modern system
        new_config = manager.load_configuration(config_file_path, legacy_config)
        
        # Log migration status
        info = manager.get_configuration_info()
        if info['using_modern']:
            logger.info("🚀 Successfully using modern Pydantic configuration system")
            logger.info(f"📊 Configuration sections: {', '.join(info['sections_loaded'])}")
        else:
            logger.info("📋 Using legacy configuration system")
        
        return new_config
        
    except Exception as e:
        logger.error(f"Configuration migration failed: {e}")
        logger.error("Returning original legacy configuration")
        return legacy_config


def enhance_config_loading_with_validation(original_load_func):
    """
    Decorator to enhance the original config loading with modern validation
    
    This decorator can be applied to the original load_config function
    to add modern validation while maintaining backward compatibility.
    """
    
    def enhanced_load_config(*args, **kwargs):
        # Call original function
        result = original_load_func(*args, **kwargs)
        
        # Add modern enhancements if available
        if PYDANTIC_AVAILABLE:
            try:
                manager = get_configuration_manager()
                manager.load_configuration("", result)  # Validate existing config
                
                if manager.validate_configuration():
                    logger.info("✅ Configuration validation passed")
                else:
                    logger.warning("⚠️ Configuration validation issues detected")
                    
            except Exception as e:
                logger.warning(f"Configuration enhancement failed: {e}")
        
        return result
    
    return enhanced_load_config


# MODERNIZED FOR PYTHON 3.13: Configuration system status check
def check_configuration_system_status() -> None:
    """
    Check and report the status of the configuration system
    
    This function provides diagnostics about which configuration system
    is being used and any potential issues.
    """
    print("\n" + "="*60)
    print("🔧 OnPatrol Configuration System Status")
    print("="*60)
    
    print(f"📦 Pydantic Available: {'✅ Yes' if PYDANTIC_AVAILABLE else '❌ No'}")
    
    if PYDANTIC_AVAILABLE:
        try:
            test_config = OnPatrolConfig()
            print("🔬 Modern Config Test: ✅ Passed")
        except Exception as e:
            print(f"🔬 Modern Config Test: ❌ Failed - {e}")
    
    manager = get_configuration_manager()
    info = manager.get_configuration_info()
    
    print(f"🎯 Using Modern System: {'✅ Yes' if info['using_modern'] else '❌ No'}")
    print(f"📖 Config Loaded: {'✅ Yes' if info['config_loaded'] else '❌ No'}")
    
    if info['config_file_path']:
        print(f"📁 Config File: {info['config_file_path']}")
    
    if info['sections_loaded']:
        print(f"📋 Sections: {', '.join(info['sections_loaded'])}")
    
    print("="*60 + "\n")


# Export main integration functions
__all__ = [
    'ConfigurationManager',
    'get_configuration_manager', 
    'migrate_config_for_onpatrol',
    'enhance_config_loading_with_validation',
    'check_configuration_system_status'
]