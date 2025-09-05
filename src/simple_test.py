#!/usr/bin/env python3
"""
Simple test without pytest framework
"""

import asyncio
import json
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from config import OnPatrolConfig, CameraConfig
from ConfigWebAPI import get_cameras_handler, create_camera_handler

async def test_basic_functionality():
    try:
        # Create config
        config = OnPatrolConfig()
        
        # Add test camera
        camera = CameraConfig(
            camera_name='Test Camera',
            address='192.168.1.100'
        )
        config.cameras['test_cam'] = camera
        
        print("✓ Config and camera created successfully")
        
        # Test get_cameras_handler
        request = make_mocked_request('GET', '/api/cameras')
        response = await get_cameras_handler(request, config)
        
        print(f"✓ get_cameras_handler response status: {response.status}")
        
        # Test create_camera_handler
        camera_data = {
            'camera_name': 'New Camera',
            'address': '192.168.1.101'
        }
        
        request = make_mocked_request('POST', '/api/cameras', 
                                    headers={'Content-Type': 'application/json'})
        request._payload = json.dumps(camera_data).encode()
        
        response = await create_camera_handler(request, config)
        print(f"✓ create_camera_handler response status: {response.status}")
        
    except Exception as ex:
        print(f"❌ Error: {ex}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(test_basic_functionality())