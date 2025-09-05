#!/usr/bin/env python3
"""
Comprehensive test suite for all camera management features
"""

import asyncio
import json
from unittest.mock import Mock, patch
from config import OnPatrolConfig, CameraConfig
from ConfigWebAPI import (
    get_cameras_handler, 
    get_camera_handler, 
    create_camera_handler,
    update_camera_handler,
    delete_camera_handler,
    test_camera_handler
)

def create_mock_request(method, path, match_info=None, json_data=None):
    """Create a proper mock request object"""
    request = Mock()
    request.method = method
    request.path = path
    request.match_info = match_info or {}
    request.content_type = 'application/json'
    
    # Mock JSON parsing for POST/PUT requests
    if json_data:
        async def mock_json():
            return json_data
        request.json = mock_json
    
    return request

async def test_complete_crud_workflow():
    """Test complete CRUD workflow for cameras"""
    print("Testing complete CRUD workflow...")
    
    config = OnPatrolConfig()
    
    # Test 1: List empty cameras
    request = create_mock_request('GET', '/api/cameras')
    response = await get_cameras_handler(request, config)
    assert response.status == 200
    data = json.loads(response.text)
    assert data['count'] == 0
    print("✓ Empty cameras list works")
    
    # Test 2: Create new camera
    camera_data = {
        'camera_name': 'Front Door Camera',
        'description': 'Main entrance monitoring',
        'address': '192.168.1.100',
        'email_address': 'camera1@test.com',
        'email_enabled': True
    }
    
    request = create_mock_request('POST', '/api/cameras', json_data=camera_data)
    response = await create_camera_handler(request, config)
    assert response.status == 201
    data = json.loads(response.text)
    camera_id = data['id']
    assert data['camera_name'] == 'Front Door Camera'
    print("✓ Camera creation works")
    
    # Test 3: Get created camera
    request = create_mock_request('GET', f'/api/cameras/{camera_id}', {'camera_id': camera_id})
    response = await get_camera_handler(request, config)
    assert response.status == 200
    data = json.loads(response.text)
    assert data['camera_name'] == 'Front Door Camera'
    print("✓ Camera retrieval works")
    
    # Test 4: Update camera
    update_data = {
        'camera_name': 'Updated Front Door Camera',
        'description': 'Updated description'
    }
    request = create_mock_request('PUT', f'/api/cameras/{camera_id}', 
                                {'camera_id': camera_id}, json_data=update_data)
    response = await update_camera_handler(request, config)
    assert response.status == 200
    data = json.loads(response.text)
    assert data['camera_name'] == 'Updated Front Door Camera'
    print("✓ Camera update works")
    
    # Test 5: Test camera connectivity (mocked)
    with patch('ConfigWebAPI.test_camera_connection') as mock_test:
        mock_test.return_value = {
            'ping_successful': True,
            'rtsp_connected': True,
            'email_valid': True,
            'response_time_ms': 25,
            'errors': []
        }
        
        request = create_mock_request('POST', f'/api/cameras/{camera_id}/test', 
                                    {'camera_id': camera_id})
        response = await test_camera_handler(request, config)
        assert response.status == 200
        data = json.loads(response.text)
        assert data['ping_successful'] is True
        print("✓ Camera connectivity test works")
    
    # Test 6: List cameras (should have 1 now)
    request = create_mock_request('GET', '/api/cameras')
    response = await get_cameras_handler(request, config)
    assert response.status == 200
    data = json.loads(response.text)
    assert data['count'] == 1
    print("✓ Camera listing with data works")
    
    # Test 7: Delete camera
    request = create_mock_request('DELETE', f'/api/cameras/{camera_id}', 
                                {'camera_id': camera_id})
    response = await delete_camera_handler(request, config)
    assert response.status == 200
    assert camera_id not in config.cameras
    print("✓ Camera deletion works")
    
    print("🎉 Complete CRUD workflow test passed!")

async def test_error_cases():
    """Test error handling scenarios"""
    print("\nTesting error cases...")
    
    config = OnPatrolConfig()
    
    # Test 1: Get non-existent camera
    request = create_mock_request('GET', '/api/cameras/nonexistent', 
                                {'camera_id': 'nonexistent'})
    response = await get_camera_handler(request, config)
    assert response.status == 404
    print("✓ Non-existent camera 404 works")
    
    # Test 2: Invalid camera data
    invalid_data = {
        'camera_name': '',  # Empty name should fail
        'rtsp_port': 70000,  # Invalid port
        'email_address': 'invalid-email'  # Invalid email
    }
    
    request = create_mock_request('POST', '/api/cameras', json_data=invalid_data)
    response = await create_camera_handler(request, config)
    assert response.status == 400
    data = json.loads(response.text)
    assert 'validation_errors' in data
    print("✓ Validation error handling works")
    
    # Test 3: Update non-existent camera
    update_data = {'camera_name': 'New Name'}
    request = create_mock_request('PUT', '/api/cameras/nonexistent',
                                {'camera_id': 'nonexistent'}, json_data=update_data)
    response = await update_camera_handler(request, config)
    assert response.status == 404
    print("✓ Update non-existent camera 404 works")
    
    # Test 4: Delete non-existent camera
    request = create_mock_request('DELETE', '/api/cameras/nonexistent',
                                {'camera_id': 'nonexistent'})
    response = await delete_camera_handler(request, config)
    assert response.status == 404
    print("✓ Delete non-existent camera 404 works")
    
    print("🎉 Error cases test passed!")

async def run_all_tests():
    """Run all camera feature tests"""
    try:
        await test_complete_crud_workflow()
        await test_error_cases()
        print("\n✅ ALL CAMERA API TESTS PASSED!")
        return True
    except Exception as ex:
        print(f"\n❌ TEST FAILED: {ex}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    print("Running comprehensive camera API tests...")
    success = asyncio.run(run_all_tests())
    
    if success:
        print("\n🎉 Camera Management API is fully functional!")
    else:
        print("\n❌ Some tests failed - check the output above")