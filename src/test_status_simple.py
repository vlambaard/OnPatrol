#!/usr/bin/env python3
"""
Simple tests for Status Monitoring API endpoints
"""

import asyncio
import json
from unittest.mock import Mock, patch
from config import OnPatrolConfig, CameraConfig
from StatusAPI import (
    get_system_status_handler,
    get_component_status_handler,
    get_camera_status_handler,
    get_performance_metrics_handler
)

def create_mock_request(method, path, match_info=None, query_params=None):
    """Create a proper mock request object"""
    request = Mock()
    request.method = method
    request.path = path
    request.match_info = match_info or {}
    request.query = query_params or {}
    return request

async def test_system_status():
    """Test system status endpoint"""
    config = OnPatrolConfig()
    
    with patch('StatusAPI.get_system_components_status') as mock_status:
        mock_status.return_value = {
            'webserver': {'status': 'running', 'uptime': 3600},
            'smtp_server': {'status': 'running', 'port': 25},
            'telegram_notifier': {'status': 'running', 'bot_connected': True}
        }
        
        request = create_mock_request('GET', '/api/status')
        response = await get_system_status_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        assert 'system' in data
        assert 'components' in data
        assert data['system']['status'] == 'healthy'
        print("✓ System status endpoint works")

async def test_component_status():
    """Test component status endpoint"""
    config = OnPatrolConfig()
    
    with patch('StatusAPI.get_detailed_component_status') as mock_status:
        mock_status.return_value = {
            'webserver': {
                'status': 'running',
                'uptime_seconds': 3600,
                'active_connections': 5
            }
        }
        
        request = create_mock_request('GET', '/api/status/components')
        response = await get_component_status_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        assert 'components' in data
        print("✓ Component status endpoint works")

async def test_camera_status():
    """Test camera status endpoint"""
    config = OnPatrolConfig()
    config.cameras = {
        'camera1': CameraConfig(camera_name='Test Camera', address='192.168.1.100')
    }
    
    with patch('StatusAPI.get_all_camera_status') as mock_status:
        mock_status.return_value = {
            'camera1': {
                'name': 'Test Camera',
                'status': 'online',
                'ping_response_ms': 45
            }
        }
        
        request = create_mock_request('GET', '/api/status/cameras')
        response = await get_camera_status_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        assert 'cameras' in data
        assert 'summary' in data
        print("✓ Camera status endpoint works")

async def test_performance_metrics():
    """Test performance metrics endpoint"""
    config = OnPatrolConfig()
    
    with patch('StatusAPI.get_performance_metrics') as mock_metrics:
        mock_metrics.return_value = {
            'system': {
                'cpu_percent': 15.2,
                'memory_percent': 32.8
            },
            'application': {
                'events_processed_today': 145
            },
            'cameras': {
                'total_cameras': 3,
                'online_cameras': 2
            }
        }
        
        request = create_mock_request('GET', '/api/status/metrics')
        response = await get_performance_metrics_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        assert 'system' in data
        assert 'application' in data
        assert 'cameras' in data
        print("✓ Performance metrics endpoint works")

async def test_integration():
    """Test integration with WebServer"""
    try:
        from WebServer import WebServer
        from StatusAPI import create_status_routes
        
        config = OnPatrolConfig()
        routes = create_status_routes(config)
        
        print(f"✓ Created {len(routes)} status API routes")
        
        # Test WebServer can be created with status API
        server = WebServer('localhost', 8080, config)
        print("✓ WebServer integration with status API works")
        
        return True
    except Exception as ex:
        print(f"❌ Integration test failed: {ex}")
        return False

async def run_all_tests():
    """Run all status API tests"""
    try:
        await test_system_status()
        await test_component_status() 
        await test_camera_status()
        await test_performance_metrics()
        success = await test_integration()
        
        if success:
            print("\n✅ ALL STATUS API TESTS PASSED!")
        else:
            print("\n❌ Some integration tests failed")
            
        return success
    except Exception as ex:
        print(f"\n❌ TEST FAILED: {ex}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    print("Running status API tests...")
    success = asyncio.run(run_all_tests())
    
    if success:
        print("\n🎉 Status Monitoring API is fully functional!")
    else:
        print("\n❌ Some tests failed - check the output above")