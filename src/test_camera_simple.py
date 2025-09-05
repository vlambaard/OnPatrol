#!/usr/bin/env python3
"""
Simplified tests for Camera Management API endpoints
"""

import pytest
import json
import asyncio
from unittest.mock import AsyncMock, patch, Mock
from aiohttp import web
from config import OnPatrolConfig, CameraConfig
from ConfigWebAPI import (
    get_cameras_handler, 
    get_camera_handler, 
    create_camera_handler,
    delete_camera_handler,
    test_camera_handler
)

def create_mock_request(method, path, match_info=None):
    """Create a proper mock request object"""
    request = Mock()
    request.method = method
    request.path = path
    request.match_info = match_info or {}
    request.content_type = 'application/json'
    return request

@pytest.mark.asyncio
async def test_get_cameras():
    """Test getting list of cameras"""
    # Setup
    config = OnPatrolConfig()
    camera = CameraConfig(camera_name='Test Camera', address='192.168.1.100')
    config.cameras['test'] = camera
    
    # Create request
    request = create_mock_request('GET', '/api/cameras')
    
    # Execute
    response = await get_cameras_handler(request, config)
    
    # Assert
    assert response.status == 200
    response_text = response.text
    response_data = json.loads(response_text)
    assert 'cameras' in response_data
    assert len(response_data['cameras']) == 1
    assert 'test' in response_data['cameras']


@pytest.mark.asyncio  
async def test_get_camera_by_id():
    """Test getting specific camera"""
    # Setup
    config = OnPatrolConfig()
    camera = CameraConfig(camera_name='Test Camera', address='192.168.1.100')
    config.cameras['test'] = camera
    
    # Create request with path parameter
    request = create_mock_request('GET', '/api/cameras/test', {'camera_id': 'test'})
    
    # Execute
    response = await get_camera_handler(request, config)
    
    # Assert
    assert response.status == 200
    response_data = json.loads(response.text)
    assert response_data['camera_name'] == 'Test Camera'
    assert response_data['id'] == 'test'


@pytest.mark.asyncio
async def test_get_camera_not_found():
    """Test getting non-existent camera"""
    # Setup
    config = OnPatrolConfig()
    
    # Create request
    request = create_mock_request('GET', '/api/cameras/nonexistent', {'camera_id': 'nonexistent'})
    
    # Execute
    response = await get_camera_handler(request, config)
    
    # Assert
    assert response.status == 404
    response_data = json.loads(response.text)
    assert response_data['error'] == 'Camera not found'


@pytest.mark.asyncio
async def test_delete_camera():
    """Test deleting camera"""
    # Setup
    config = OnPatrolConfig()
    camera = CameraConfig(camera_name='Test Camera', address='192.168.1.100')
    config.cameras['test'] = camera
    
    # Create request
    request = create_mock_request('DELETE', '/api/cameras/test', {'camera_id': 'test'})
    
    # Execute
    response = await delete_camera_handler(request, config)
    
    # Assert
    assert response.status == 200
    response_data = json.loads(response.text)
    assert response_data['message'] == 'Camera deleted successfully'
    assert 'test' not in config.cameras


@pytest.mark.asyncio
async def test_delete_camera_not_found():
    """Test deleting non-existent camera"""
    # Setup
    config = OnPatrolConfig()
    
    # Create request
    request = create_mock_request('DELETE', '/api/cameras/nonexistent', {'camera_id': 'nonexistent'})
    
    # Execute
    response = await delete_camera_handler(request, config)
    
    # Assert
    assert response.status == 404


@pytest.mark.asyncio
async def test_camera_connectivity():
    """Test camera connectivity testing"""
    # Setup
    config = OnPatrolConfig()
    camera = CameraConfig(
        camera_name='Test Camera',
        address='192.168.1.100',
        rtsp_recording_enabled=True,
        email_enabled=True,
        email_address='test@test.com'
    )
    config.cameras['test'] = camera
    
    # Mock the connectivity test function
    with patch('ConfigWebAPI.test_camera_connection') as mock_test:
        mock_test.return_value = {
            'ping_successful': True,
            'rtsp_connected': True,
            'email_valid': True,
            'response_time_ms': 50,
            'errors': []
        }
        
        # Create request
        request = create_mock_request('POST', '/api/cameras/test/test', {'camera_id': 'test'})
        
        # Execute
        response = await test_camera_handler(request, config)
        
        # Assert
        assert response.status == 200
        response_data = json.loads(response.text)
        assert response_data['ping_successful'] is True
        assert response_data['rtsp_connected'] is True
        assert response_data['email_valid'] is True


if __name__ == '__main__':
    # Run tests directly
    async def run_tests():
        print("Running simplified camera API tests...")
        
        try:
            await test_get_cameras()
            print("✓ test_get_cameras passed")
        except Exception as e:
            print(f"❌ test_get_cameras failed: {e}")
        
        try:
            await test_get_camera_by_id()
            print("✓ test_get_camera_by_id passed")
        except Exception as e:
            print(f"❌ test_get_camera_by_id failed: {e}")
        
        try:
            await test_get_camera_not_found()
            print("✓ test_get_camera_not_found passed")
        except Exception as e:
            print(f"❌ test_get_camera_not_found failed: {e}")
        
        try:
            await test_delete_camera()
            print("✓ test_delete_camera passed")
        except Exception as e:
            print(f"❌ test_delete_camera failed: {e}")
        
        try:
            await test_delete_camera_not_found()
            print("✓ test_delete_camera_not_found passed")
        except Exception as e:
            print(f"❌ test_delete_camera_not_found failed: {e}")
        
        try:
            await test_camera_connectivity()
            print("✓ test_camera_connectivity passed")
        except Exception as e:
            print(f"❌ test_camera_connectivity failed: {e}")
    
    asyncio.run(run_tests())