#!/usr/bin/env python3
"""
Quick test script for the new configuration system
"""

import sys
import json
from config import OnPatrolConfig
from config_integration import get_configuration_manager, migrate_config_for_onpatrol

# Test basic configuration creation
print("=== Testing Basic Configuration ===")
config = OnPatrolConfig()
print(f"Basic config created successfully")
legacy = config.get_legacy_config()
print(f"Legacy format sections: {list(legacy.keys())}")

# Test Telegram section
print(f"\nTelegram section: {legacy.get('TELEGRAM', {})}")
print(f"Paths section: {legacy.get('PATHS', {})}")

# Test with a sample legacy config
print("\n=== Testing Legacy Migration ===")
sample_legacy = {
    'PATHS': {
        'EXE_PATH': '/test/exe',
        'DATA_PATH': '/test/data',
        'CONFIG_PATH': '/test/config',
        'IMAGES_SAVE_PATH': '/test/images'
    },
    'TELEGRAM': {
        'ENABLED': True,
        'TOKEN': 'test_token',
        'CHAT_ID': 'test_chat'
    }
}

try:
    migrated = migrate_config_for_onpatrol(sample_legacy, '')
    print("Migration successful!")
    print(f"Migrated TELEGRAM: {migrated.get('TELEGRAM', {})}")
    print(f"Migrated PATHS: {migrated.get('PATHS', {})}")
except Exception as e:
    print(f"Migration failed: {e}")
    import traceback
    traceback.print_exc()