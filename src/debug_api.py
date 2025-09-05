#!/usr/bin/env python3
"""
Debug API creation issues
"""

try:
    from config import OnPatrolConfig, CameraConfig
    print("✓ Import config modules successful")
    
    from ConfigWebAPI import create_camera_routes
    print("✓ Import ConfigWebAPI successful")
    
    # Create config
    config = OnPatrolConfig()
    print("✓ OnPatrolConfig() creation successful")
    
    # Add a camera manually
    camera = CameraConfig(
        camera_name='Test Camera',
        address='192.168.1.100'
    )
    config.cameras['test'] = camera
    print("✓ Camera added to config")
    
    # Try to create routes
    routes = create_camera_routes(config)
    print(f"✓ Routes created successfully: {len(routes)} routes")
    
    from aiohttp import web
    print("✓ aiohttp import successful")
    
    # Try to create app
    app = web.Application()
    app.add_routes(routes)
    print("✓ App creation with routes successful")
    
except Exception as ex:
    print(f"❌ Error: {ex}")
    import traceback
    traceback.print_exc()