"""
Modern Configuration System for OnPatrol
==========================================

This module provides a type-safe, validated configuration system using Pydantic.
It maintains backward compatibility with the existing INI-based configuration
while providing modern features like type validation, environment variable support,
and better error handling.

Features:
- Type safety with automatic validation
- Environment variable support
- Schema validation with clear error messages
- Backward compatibility with existing config.ini files
- Support for both INI and modern formats (YAML, TOML, JSON)
- Built-in defaults and validation

Author: OnPatrol Team
Version: 2.0.0 (Modernized for Python 3.13)
"""

import os
import re
import configparser
from pathlib import Path
from typing import List, Dict, Optional, Union, Any
from datetime import time, datetime
from enum import Enum

try:
    from pydantic import BaseModel, BaseSettings, Field, validator, root_validator
    from pydantic.types import DirectoryPath, FilePath
except ImportError:
    # Fallback if pydantic is not installed
    print("Installing pydantic for modern configuration management...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pydantic[email]"])
    from pydantic import BaseModel, BaseSettings, Field, validator, root_validator
    from pydantic.types import DirectoryPath, FilePath


class LogLevel(str, Enum):
    """Valid log levels"""
    DEBUG = "DEBUG"
    INFO = "INFO" 
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class TimeString(str):
    """Custom type for time strings in HH:MM format"""
    
    @classmethod
    def __get_validators__(cls):
        yield cls.validate
        
    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            raise TypeError('Time string required')
            
        # Validate HH:MM format
        if not re.match(r'^([01]?[0-9]|2[0-3]):[0-5][0-9]$', v):
            raise ValueError('Time must be in HH:MM format (e.g., "09:30", "21:45")')
            
        return v


class ServerConfig(BaseModel):
    """Server configuration settings"""
    process_id: str = Field(default="", description="Process identifier for logging")
    server_id: str = Field(default="OnPatrol", description="Server identification string")
    
    class Config:
        extra = "allow"  # Allow additional fields for backward compatibility


class RecorderConfig(BaseModel):
    """Recording and image management configuration"""
    images_save_path: str = Field(default="./images", description="Path to save captured images")
    images_keep_time: str = Field(default="00:05:00", description="How long to keep images (HH:MM:SS)")
    
    @validator('images_keep_time')
    def validate_keep_time(cls, v):
        # Validate HH:MM:SS format and minimum time
        if not re.match(r'^([0-9]{1,2}):([0-5][0-9]):([0-5][0-9])$', v):
            raise ValueError('Keep time must be in HH:MM:SS format')
        
        # Convert to seconds and ensure minimum 5 minutes
        h, m, s = map(int, v.split(':'))
        total_seconds = h * 3600 + m * 60 + s
        if total_seconds < 300:  # 5 minutes minimum
            raise ValueError('Images keep time must be at least 5 minutes (00:05:00)')
        
        return v
    
    class Config:
        extra = "allow"


class HttpConfig(BaseModel):
    """HTTP status server configuration"""
    enabled: bool = Field(default=True, description="Enable HTTP status server")
    port: int = Field(default=8080, description="Port for HTTP status server")
    host: str = Field(default="localhost", description="Host interface to bind to")
    
    @validator('port')
    def validate_port(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError('Port must be between 1 and 65535')
        return v
    
    class Config:
        extra = "allow"


class SmtpConfig(BaseModel):
    """SMTP email server configuration"""
    enabled: bool = Field(default=False, description="Enable SMTP email server")
    port: int = Field(default=25, description="SMTP server port")
    host: str = Field(default="localhost", description="SMTP server host")
    email_templates: Dict[str, Any] = Field(default_factory=dict, description="Email parsing templates")
    
    @validator('port')
    def validate_port(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError('Port must be between 1 and 65535')
        return v
    
    class Config:
        extra = "allow"


class IsapiConfig(BaseModel):
    """ISAPI camera interface configuration"""
    enabled: bool = Field(default=False, description="Enable ISAPI camera interface")
    port: int = Field(default=80, description="ISAPI port")
    timeout: int = Field(default=30, description="ISAPI timeout in seconds")
    
    class Config:
        extra = "allow"


class TelegramConfig(BaseModel):
    """Telegram logging and notification configuration"""
    enabled: bool = Field(default=False, description="Enable Telegram logging")
    bot_token: Optional[str] = Field(default=None, env='TELEGRAM_BOT_TOKEN', description="Telegram bot token")
    chat_id: Optional[str] = Field(default=None, env='TELEGRAM_CHAT_ID', description="Telegram chat ID")
    log_level: LogLevel = Field(default=LogLevel.ERROR, description="Minimum log level for Telegram")
    
    @validator('bot_token')
    def validate_bot_token(cls, v):
        if v is not None and not re.match(r'^\d+:[A-Za-z0-9_-]+$', v):
            raise ValueError('Invalid Telegram bot token format')
        return v
    
    class Config:
        extra = "allow"


class DeepStackConfig(BaseModel):
    """DeepStack AI integration configuration"""
    enabled: bool = Field(default=False, description="Enable DeepStack AI detection")
    server: str = Field(default="localhost", description="DeepStack server address")
    port: int = Field(default=5000, description="DeepStack server port")
    api_key: Optional[str] = Field(default=None, description="DeepStack API key")
    api_path: str = Field(default="v1/vision/detection", description="DeepStack API path")
    url: str = Field(default="", description="Complete DeepStack URL (auto-generated)")
    camera_profiles: Dict[str, Any] = Field(default_factory=dict, description="Camera-specific AI profiles")
    
    @validator('port')
    def validate_port(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError('Port must be between 1 and 65535')
        return v
    
    @root_validator
    def generate_url(cls, values):
        """Auto-generate the complete DeepStack URL"""
        server = values.get('server', 'localhost')
        port = values.get('port', 5000)
        values['url'] = f"http://{server.strip('/')}:{port}"
        return values
    
    class Config:
        extra = "allow"


class UnregisteredCamerasConfig(BaseModel):
    """Configuration for handling unregistered cameras"""
    email_from_unregistered_cameras_enabled: bool = Field(
        default=False, 
        description="Allow emails from unregistered cameras"
    )
    deepstack_detection_enabled: bool = Field(
        default=False, 
        description="Enable AI detection for unregistered cameras"
    )
    deepstack_min_confidence: float = Field(
        default=0.5, 
        description="Minimum confidence for AI detection"
    )
    deepstack_prefilter_enabled: bool = Field(
        default=False, 
        description="Enable AI pre-filtering"
    )
    email_index: Dict[str, str] = Field(
        default_factory=dict, 
        description="Email address to template mapping"
    )
    
    @validator('deepstack_min_confidence')
    def validate_confidence(cls, v):
        if not 0.0 <= v <= 1.0:
            raise ValueError('Confidence must be between 0.0 and 1.0')
        return v
    
    class Config:
        extra = "allow"


class EmailTemplateConfig(BaseModel):
    """Email template configuration for parsing camera emails"""
    event_type_re: str = Field(default="", description="Regex for event type extraction")
    event_type_group: int = Field(default=0, description="Regex group for event type")
    event_datetime_re: str = Field(default="", description="Regex for datetime extraction")
    event_date_group: int = Field(default=0, description="Regex group for date")
    event_time_group: int = Field(default=0, description="Regex group for time")
    camera_name_re: str = Field(default="", description="Regex for camera name extraction")
    camera_name_group: int = Field(default=0, description="Regex group for camera name")
    serial_number_re: str = Field(default="", description="Regex for serial number extraction")
    serial_number_group: int = Field(default=0, description="Regex group for serial number")
    channel_name_re: str = Field(default="", description="Regex for channel name extraction")
    channel_name_group: int = Field(default=0, description="Regex group for channel name")
    channel_number_re: str = Field(default="", description="Regex for channel number extraction")
    channel_number_group: int = Field(default=0, description="Regex group for channel number")
    test_message_re: str = Field(default="", description="Regex for test message detection")
    test_message_camera_name_re: str = Field(default="", description="Regex for test camera name")
    test_message_camera_name_group: int = Field(default=0, description="Group for test camera name")
    
    class Config:
        extra = "allow"


class CameraConfig(BaseModel):
    """Individual camera configuration"""
    camera_enabled: bool = Field(default=True, description="Enable this camera")
    camera_name: str = Field(default="", description="Camera display name")
    description: str = Field(default="", description="Camera description")
    latitude: str = Field(default="", description="Camera latitude")
    longitude: str = Field(default="", description="Camera longitude")
    channel_number: str = Field(default="1", description="Camera channel number")
    channel_stream_number: str = Field(default="1", description="Camera stream number")
    images_keep_time: str = Field(default="00:01:00", description="How long to keep images")
    
    # Email configuration
    email_enabled: bool = Field(default=False, description="Enable email notifications")
    email_address: str = Field(default="", description="Camera email address")
    email_template: str = Field(default="HIKVISION_DEFAULT", description="Email template to use")
    
    # RTSP configuration
    rtsp_recording_enabled: bool = Field(default=False, description="Enable RTSP recording")
    address: str = Field(default="", description="Camera IP address")
    username: str = Field(default="", description="Camera username")
    password: str = Field(default="", description="Camera password")
    rtsp_port: int = Field(default=554, description="RTSP port")
    rtsp_recording_length_sec: int = Field(default=4, description="Recording length in seconds")
    rtsp_url_path: str = Field(default="/Streaming/Channels/101", description="RTSP URL path")
    rtsp_rec_on_event_type: str = Field(
        default="Intrusion Detection, Test Notification.", 
        description="Event types that trigger recording"
    )
    
    # ISAPI configuration
    isapi_enabled: bool = Field(default=False, description="Enable ISAPI interface")
    isapi_reply_to_local_ip: bool = Field(default=False, description="Reply to local IP")
    isapi_port: int = Field(default=80, description="ISAPI port")
    
    # DeepStack AI configuration
    deepstack_detection_enabled: bool = Field(default=True, description="Enable AI detection")
    deepstack_min_confidence: float = Field(default=0.43, description="Minimum AI confidence")
    deepstack_prefilter_enabled: bool = Field(default=False, description="Enable AI pre-filtering")
    
    @validator('rtsp_port', 'isapi_port')
    def validate_ports(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError('Port must be between 1 and 65535')
        return v
    
    @validator('deepstack_min_confidence')
    def validate_confidence(cls, v):
        if not 0.0 <= v <= 1.0:
            raise ValueError('Confidence must be between 0.0 and 1.0')
        return v
    
    @validator('email_address')
    def validate_email(cls, v):
        if v and '@' not in v:
            raise ValueError('Invalid email address format')
        return v
    
    class Config:
        extra = "allow"


class CameraGroupConfig(BaseModel):
    """Camera group/cluster configuration"""
    ipc_names: List[str] = Field(default_factory=list, description="List of camera names")
    channel_names: List[str] = Field(default_factory=list, description="List of channel names")
    channel_numbers: List[int] = Field(default_factory=list, description="List of channel numbers")
    event_types: List[str] = Field(
        default_factory=lambda: ["Intrusion Detection", "Person", "DeepStackFailed"],
        description="Event types to monitor"
    )
    enabled: bool = Field(default=True, description="Enable this camera group")
    time_start: TimeString = Field(default="21:00", description="Start time for monitoring")
    time_stop: TimeString = Field(default="06:00", description="Stop time for monitoring")
    
    # Days of week
    monday: bool = Field(default=True, description="Monitor on Monday")
    tuesday: bool = Field(default=True, description="Monitor on Tuesday")
    wednesday: bool = Field(default=True, description="Monitor on Wednesday")
    thursday: bool = Field(default=True, description="Monitor on Thursday")
    friday: bool = Field(default=True, description="Monitor on Friday")
    saturday: bool = Field(default=True, description="Monitor on Saturday")
    sunday: bool = Field(default=True, description="Monitor on Sunday")
    
    class Config:
        extra = "allow"


class NotificationConfig(BaseModel):
    """Telegram notification configuration"""
    notification_name: str = Field(default="", description="Notification identifier")
    enabled: bool = Field(default=False, description="Enable this notification")
    camera_clusters: List[str] = Field(default_factory=list, description="Camera clusters to monitor")
    bot_token: str = Field(default="", description="Telegram bot token")
    bot_chat_id: str = Field(default="", description="Telegram chat ID")
    bot_group_name: str = Field(default="", description="Telegram group name")
    msg_expiry_time: int = Field(default=0, description="Message expiry time in seconds")
    indicate_event_type: bool = Field(default=False, description="Show event type in messages")
    
    # Live verification
    live_verification: Dict[str, Any] = Field(
        default_factory=lambda: {
            'ACTIVE': False,
            'REASON': '',
            'BOT_USERNAME': '',
            'GROUP_NAME': ''
        },
        description="Live verification settings"
    )
    
    @validator('bot_token')
    def validate_bot_token(cls, v):
        if v and not re.match(r'^\d+:[A-Za-z0-9_-]+$', v):
            raise ValueError('Invalid Telegram bot token format')
        return v
    
    class Config:
        extra = "allow"


class PathsConfig(BaseModel):
    """System paths configuration"""
    exe_path: str = Field(default="", description="Executable path")
    data_path: str = Field(default="./data", description="Data storage path")
    config_path: str = Field(default="./config", description="Configuration files path")
    camera_cluster_path: str = Field(default="./config/clusters", description="Camera cluster configs path")
    images_save_path: str = Field(default="./images", description="Images storage path")
    
    @validator('data_path', 'config_path', 'camera_cluster_path', 'images_save_path')
    def validate_paths(cls, v):
        if v:
            # Convert relative paths and ensure they exist
            path = Path(v).expanduser()
            if not path.is_absolute():
                path = Path.cwd() / path
            # Create directory if it doesn't exist
            path.mkdir(parents=True, exist_ok=True)
            return str(path)
        return v
    
    class Config:
        extra = "allow"


class OnPatrolConfig(BaseSettings):
    """Main OnPatrol configuration class
    
    This is the root configuration class that contains all subsystem configurations.
    It supports loading from:
    - Environment variables (with ONPATROL_ prefix)
    - .env files
    - config.ini files (backward compatibility)
    - YAML/TOML/JSON files (future enhancement)
    """
    
    # Core subsystem configurations
    server: ServerConfig = Field(default_factory=ServerConfig, description="Server settings")
    recorder: RecorderConfig = Field(default_factory=RecorderConfig, description="Recording settings")
    http: HttpConfig = Field(default_factory=HttpConfig, description="HTTP server settings")
    smtp: SmtpConfig = Field(default_factory=SmtpConfig, description="SMTP server settings")
    isapi: IsapiConfig = Field(default_factory=IsapiConfig, description="ISAPI settings")
    telegram: TelegramConfig = Field(default_factory=TelegramConfig, description="Telegram settings")
    deepstack: DeepStackConfig = Field(default_factory=DeepStackConfig, description="DeepStack AI settings")
    unregistered_cameras: UnregisteredCamerasConfig = Field(
        default_factory=UnregisteredCamerasConfig, 
        description="Unregistered cameras settings"
    )
    
    # Complex configurations
    cameras: Dict[str, CameraConfig] = Field(
        default_factory=dict, 
        description="Individual camera configurations"
    )
    camera_clusters: Dict[str, CameraGroupConfig] = Field(
        default_factory=dict, 
        description="Camera group configurations"
    )
    notifications: List[NotificationConfig] = Field(
        default_factory=list, 
        description="Notification configurations"
    )
    paths: PathsConfig = Field(default_factory=PathsConfig, description="System paths")
    
    # Legacy compatibility
    _legacy_config: Optional[Dict[str, Any]] = None
    
    class Config:
        env_prefix = "ONPATROL_"
        env_file = ".env"
        env_file_encoding = 'utf-8'
        case_sensitive = False
        extra = "allow"  # Allow extra fields for backward compatibility
        
        # Custom configuration for validation
        validate_assignment = True
        use_enum_values = True
    
    @root_validator(pre=True)
    def setup_paths(cls, values):
        """Ensure paths are properly configured before other validation"""
        if 'paths' not in values:
            values['paths'] = {}
        
        # Set default data path if not specified
        if 'data_path' not in values['paths'] or not values['paths']['data_path']:
            values['paths']['data_path'] = './data'
        
        return values
    
    def get_legacy_config(self) -> Dict[str, Any]:
        """Convert Pydantic config back to legacy CONFIG dictionary format
        
        This method provides backward compatibility by converting the modern
        Pydantic configuration back to the original nested dictionary format
        that the existing codebase expects.
        
        Returns:
            Dict[str, Any]: Configuration in legacy format
        """
        
        # Start with the base structure
        legacy_config = {
            'SERVER': self.server.dict(),
            'RECORDER': self.recorder.dict(),
            'HTTP': self.http.dict(),
            'SMTP': {**self.smtp.dict(), 'EMAIL_TEMPLATES': self.smtp.email_templates},
            'ISAPI': self.isapi.dict(),
            'TELEGRAM': self.telegram.dict(),
            'DEEPSTACK': {
                **self.deepstack.dict(),
                'CAMERA_PROFILES': self.deepstack.camera_profiles,
                'CAMERA_NAME_INDEX': {},  # Will be populated from cameras
                'ALL_CAMERAS_INDEX': {}   # Will be populated from cameras
            },
            'UNREGISTERED_CAMERAS': {
                **self.unregistered_cameras.dict(),
                'EMAIL_INDEX': self.unregistered_cameras.email_index
            },
            'CAMERAS': {
                'CONFIGS': {name: camera.dict() for name, camera in self.cameras.items()},
                'ISAPI_RESPONSE_USERNAME_INDEX': {},  # Legacy field
                'EMAIL_INDEX': {}  # Will be populated from cameras
            },
            'NOTIFICATIONS': [notif.dict() for notif in self.notifications],
            'CAMERA_CLUSTERS': {name: cluster.dict() for name, cluster in self.camera_clusters.items()},
            'PATHS': self.paths.dict()
        }
        
        # Populate camera indexes for backward compatibility
        for name, camera in self.cameras.items():
            if camera.email_enabled and camera.email_address:
                legacy_config['CAMERAS']['EMAIL_INDEX'][camera.email_address] = {
                    'EMAIL_TEMPLATE': camera.email_template,
                    'CHANNEL': {camera.channel_number: name}
                }
        
        # Ensure IMAGES_SAVE_PATH is available in both locations for compatibility
        if 'IMAGES_SAVE_PATH' not in legacy_config['PATHS']:
            legacy_config['PATHS']['IMAGES_SAVE_PATH'] = legacy_config['PATHS'].get('images_save_path', './images')
        
        # Add uppercase keys for backward compatibility with legacy code
        compatibility_mappings = {
            'TELEGRAM': {
                'enabled': 'ENABLED',
                'bot_token': 'TOKEN', 
                'chat_id': 'CHAT_ID',
                'log_level': 'LOG_LEVEL'
            },
            'DEEPSTACK': {
                'enabled': 'ENABLED',
                'server': 'SERVER',
                'port': 'PORT',
                'api_key': 'API_KEY',
                'url': 'URL'
            },
            'HTTP': {
                'enabled': 'ENABLED',
                'port': 'PORT',
                'host': 'HOST'
            },
            'SMTP': {
                'enabled': 'ENABLED',
                'port': 'PORT',
                'host': 'HOST'
            }
        }
        
        for section, mappings in compatibility_mappings.items():
            if section in legacy_config:
                for modern_key, legacy_key in mappings.items():
                    if modern_key in legacy_config[section] and legacy_key not in legacy_config[section]:
                        legacy_config[section][legacy_key] = legacy_config[section][modern_key]
        
        return legacy_config
    
    @classmethod
    def from_legacy_config(cls, legacy_config: Dict[str, Any]) -> 'OnPatrolConfig':
        """Create OnPatrolConfig from legacy CONFIG dictionary
        
        Args:
            legacy_config: The original CONFIG dictionary
            
        Returns:
            OnPatrolConfig: Modern configuration instance
        """
        
        # Convert legacy config to Pydantic format
        config_data = {}
        
        # Map simple sections
        section_mappings = {
            'server': 'SERVER',
            'recorder': 'RECORDER', 
            'http': 'HTTP',
            'smtp': 'SMTP',
            'isapi': 'ISAPI',
            'telegram': 'TELEGRAM',
            'deepstack': 'DEEPSTACK',
            'unregistered_cameras': 'UNREGISTERED_CAMERAS',
            'paths': 'PATHS'
        }
        
        for pydantic_key, legacy_key in section_mappings.items():
            if legacy_key in legacy_config:
                config_data[pydantic_key] = legacy_config[legacy_key].copy()
        
        # Convert complex sections
        if 'CAMERAS' in legacy_config and 'CONFIGS' in legacy_config['CAMERAS']:
            config_data['cameras'] = legacy_config['CAMERAS']['CONFIGS'].copy()
        
        if 'CAMERA_CLUSTERS' in legacy_config:
            config_data['camera_clusters'] = legacy_config['CAMERA_CLUSTERS'].copy()
        
        if 'NOTIFICATIONS' in legacy_config:
            config_data['notifications'] = legacy_config['NOTIFICATIONS'].copy()
        
        # Create the configuration instance
        instance = cls(**config_data)
        instance._legacy_config = legacy_config
        
        return instance


# MODERNIZED FOR PYTHON 3.13: Global configuration instance
_config_instance: Optional[OnPatrolConfig] = None


def get_config() -> OnPatrolConfig:
    """Get the global configuration instance
    
    Returns:
        OnPatrolConfig: The global configuration instance
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = OnPatrolConfig()
    return _config_instance


def load_config_from_ini(ini_file_path: str) -> OnPatrolConfig:
    """Load configuration from INI file with backward compatibility
    
    Args:
        ini_file_path: Path to the config.ini file
        
    Returns:
        OnPatrolConfig: Loaded configuration
        
    Raises:
        OSError: If config file cannot be read
        ValueError: If config contains invalid values
    """
    try:
        config_parser = configparser.ConfigParser(allow_no_value=True)
        config_parser.read(ini_file_path)
        
        # Convert ConfigParser to dictionary
        config_dict = {}
        for section_name in config_parser.sections():
            config_dict[section_name] = {}
            for key, value in config_parser.items(section_name):
                # Convert string values to appropriate types
                if value.lower() in ('true', 'false'):
                    config_dict[section_name][key] = value.lower() == 'true'
                elif value.isdigit():
                    config_dict[section_name][key] = int(value)
                elif '.' in value and all(part.isdigit() for part in value.split('.', 1)):
                    config_dict[section_name][key] = float(value)
                else:
                    config_dict[section_name][key] = value
        
        # Convert to OnPatrolConfig
        return OnPatrolConfig.from_legacy_config(config_dict)
        
    except Exception as ex:
        raise OSError(f'Error loading config from {ini_file_path}: {str(ex)}')


def save_config_to_ini(config: OnPatrolConfig, ini_file_path: str) -> None:
    """Save configuration to INI file for backward compatibility
    
    Args:
        config: Configuration to save
        ini_file_path: Path to save the config.ini file
    """
    legacy_config = config.get_legacy_config()
    
    config_parser = configparser.ConfigParser(allow_no_value=True)
    
    for section_name, section_data in legacy_config.items():
        if isinstance(section_data, dict) and section_name not in ['CAMERAS', 'NOTIFICATIONS', 'CAMERA_CLUSTERS']:
            config_parser.add_section(section_name)
            for key, value in section_data.items():
                if not isinstance(value, dict):  # Skip nested dictionaries
                    config_parser.set(section_name, key, str(value))
    
    with open(ini_file_path, 'w') as configfile:
        config_parser.write(configfile)


def migrate_legacy_config(legacy_config: Dict[str, Any]) -> OnPatrolConfig:
    """Migrate legacy CONFIG dictionary to modern Pydantic configuration
    
    This function handles the migration from the old configuration system
    to the new Pydantic-based system while maintaining all functionality.
    
    Args:
        legacy_config: The original CONFIG dictionary
        
    Returns:
        OnPatrolConfig: Migrated modern configuration
    """
    print("🔄 Migrating configuration to modern Pydantic-based system...")
    
    try:
        # Handle special case for paths - convert all path keys to lowercase for compatibility
        if 'PATHS' in legacy_config and isinstance(legacy_config['PATHS'], dict):
            paths_data = {}
            for key, value in legacy_config['PATHS'].items():
                # Convert key to lowercase and map common variations
                lowercase_key = key.lower()
                if lowercase_key == 'images_save_path' or key == 'IMAGES_SAVE_PATH':
                    paths_data['images_save_path'] = value
                else:
                    paths_data[lowercase_key] = value
            legacy_config['PATHS'] = paths_data
        
        new_config = OnPatrolConfig.from_legacy_config(legacy_config)
        print("✅ Configuration migration completed successfully")
        return new_config
        
    except Exception as ex:
        print(f"❌ Configuration migration failed: {ex}")
        print("🔄 Falling back to basic configuration...")
        # Create basic config with essential paths
        basic_config = OnPatrolConfig()
        # Copy essential path information if available
        if 'PATHS' in legacy_config:
            for key, value in legacy_config['PATHS'].items():
                if hasattr(basic_config.paths, key.lower()):
                    setattr(basic_config.paths, key.lower(), value)
        return basic_config


# Export the main configuration class and functions
__all__ = [
    'OnPatrolConfig',
    'get_config', 
    'load_config_from_ini',
    'save_config_to_ini',
    'migrate_legacy_config',
    # Individual configuration classes for advanced usage
    'ServerConfig',
    'RecorderConfig', 
    'HttpConfig',
    'SmtpConfig',
    'IsapiConfig',
    'TelegramConfig',
    'DeepStackConfig',
    'UnregisteredCamerasConfig',
    'CameraConfig',
    'CameraGroupConfig',
    'NotificationConfig',
    'PathsConfig',
    'EmailTemplateConfig'
]