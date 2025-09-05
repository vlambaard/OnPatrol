#!/usr/bin/env python3
"""
Debug configuration creation issues
"""

try:
    from config import OnPatrolConfig
    print("✓ Import OnPatrolConfig successful")
    
    # Try to create basic config
    config = OnPatrolConfig()
    print("✓ OnPatrolConfig() creation successful")
    print(f"Config type: {type(config)}")
    
    # Try to access cameras
    print(f"Cameras: {config.cameras}")
    
except Exception as ex:
    print(f"❌ Error: {ex}")
    import traceback
    traceback.print_exc()