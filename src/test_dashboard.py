#!/usr/bin/env python3
"""
Tests for System Status Dashboard functionality
Tests real-time status display and WebSocket integration
"""

import asyncio
import json
import time
from unittest.mock import Mock, patch, AsyncMock
import pytest
from config import OnPatrolConfig, CameraConfig
from StatusAPI import (
    get_system_status_handler,
    get_component_status_handler,
    get_camera_status_handler,
    get_performance_metrics_handler,
    websocket_handler
)

def create_mock_request(method='GET', path='/', match_info=None, query_params=None):
    """Create a proper mock request object"""
    request = Mock()
    request.method = method
    request.path = path
    request.match_info = match_info or {}
    request.query = query_params or {}
    return request

def create_mock_websocket():
    """Create a mock WebSocket for testing"""
    ws = Mock()
    ws.send_str = AsyncMock()
    ws.receive = AsyncMock()
    ws.close = AsyncMock()
    ws.closed = False
    return ws

@pytest.mark.asyncio
async def test_dashboard_system_health_aggregation():
    """Test system health status aggregation for dashboard"""
    config = OnPatrolConfig()
    
    with patch('StatusAPI.get_system_components_status') as mock_components:
        mock_components.return_value = {
            'webserver': {'status': 'running', 'uptime': 3600, 'port': 8080},
            'smtp_server': {'status': 'running', 'port': 25, 'messages_today': 15},
            'telegram_notifier': {'status': 'running', 'bot_connected': True},
            'database': {'status': 'running', 'size_mb': 125.5}
        }
        
        request = create_mock_request('GET', '/api/status')
        response = await get_system_status_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        
        # Test overall system health
        assert data['system']['status'] == 'healthy'
        assert 'uptime_seconds' in data['system']
        
        # Test component aggregation
        assert 'components' in data
        assert len(data['components']) == 4
        
        # Test that all components are running
        for component_name, component_data in data['components'].items():
            assert component_data['status'] == 'running'
        
        print("✓ Dashboard system health aggregation works")

@pytest.mark.asyncio
async def test_dashboard_performance_metrics():
    """Test performance metrics collection for dashboard display"""
    config = OnPatrolConfig()
    
    with patch('StatusAPI.get_performance_metrics') as mock_metrics:
        mock_metrics.return_value = {
            'system': {
                'cpu_percent': 25.8,
                'memory_percent': 45.2,
                'disk_percent': 65.0,
                'load_average': [0.8, 0.9, 1.1]
            },
            'application': {
                'events_processed_today': 342,
                'notifications_sent_today': 89,
                'avg_response_time_ms': 125.5,
                'websocket_connections': 3,
                'database_size_mb': 125.5
            },
            'cameras': {
                'total_cameras': 5,
                'online_cameras': 4,
                'offline_cameras': 1,
                'average_response_time': 85.2
            }
        }
        
        request = create_mock_request('GET', '/api/status/metrics')
        response = await get_performance_metrics_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        
        # Test system metrics
        assert data['system']['cpu_percent'] == 25.8
        assert data['system']['memory_percent'] == 45.2
        assert data['system']['disk_percent'] == 65.0
        
        # Test application metrics
        assert data['application']['events_processed_today'] == 342
        assert data['application']['websocket_connections'] == 3
        
        # Test camera metrics
        assert data['cameras']['total_cameras'] == 5
        assert data['cameras']['online_cameras'] == 4
        
        print("✓ Dashboard performance metrics work")

@pytest.mark.asyncio
async def test_camera_status_for_dashboard():
    """Test camera status aggregation for dashboard display"""
    config = OnPatrolConfig()
    config.cameras = {
        'front_door': CameraConfig(camera_name='Front Door', address='192.168.1.100'),
        'back_yard': CameraConfig(camera_name='Back Yard', address='192.168.1.101'),
        'driveway': CameraConfig(camera_name='Driveway', address='192.168.1.102')
    }
    
    with patch('StatusAPI.get_all_camera_status') as mock_camera_status:
        mock_camera_status.return_value = {
            'front_door': {
                'name': 'Front Door',
                'status': 'online',
                'address': '192.168.1.100',
                'ping_response_ms': 45,
                'last_event': '2025-01-17T10:30:00Z'
            },
            'back_yard': {
                'name': 'Back Yard', 
                'status': 'online',
                'address': '192.168.1.101',
                'ping_response_ms': 62,
                'last_event': '2025-01-17T09:15:00Z'
            },
            'driveway': {
                'name': 'Driveway',
                'status': 'offline',
                'address': '192.168.1.102',
                'ping_response_ms': None,
                'last_event': '2025-01-16T22:45:00Z'
            }
        }
        
        request = create_mock_request('GET', '/api/status/cameras')
        response = await get_camera_status_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        
        # Test camera summary for dashboard
        assert data['summary']['total_cameras'] == 3
        assert data['summary']['online_cameras'] == 2
        assert data['summary']['offline_cameras'] == 1
        assert 'average_response_time' in data['summary']
        
        # Test individual camera status
        assert 'front_door' in data['cameras']
        assert data['cameras']['front_door']['status'] == 'online'
        assert data['cameras']['driveway']['status'] == 'offline'
        
        print("✓ Camera status for dashboard works")

@pytest.mark.asyncio
async def test_websocket_real_time_updates():
    """Test WebSocket real-time updates for dashboard"""
    config = OnPatrolConfig()
    
    # Mock WebSocket connection
    ws = create_mock_websocket()
    request = create_mock_request('GET', '/api/status/ws')
    
    with patch('StatusAPI.get_system_components_status') as mock_status:
        mock_status.return_value = {
            'webserver': {'status': 'running', 'uptime': 3600}
        }
        
        # Test initial connection and status broadcast
        with patch('aiohttp.web.WebSocketResponse') as mock_ws_response:
            mock_ws_response.return_value = ws
            
            # Simulate WebSocket handler
            await websocket_handler(request, config)
            
            # Verify WebSocket was used
            assert ws.send_str.called
            
        print("✓ WebSocket real-time updates work")

@pytest.mark.asyncio 
async def test_dashboard_health_indicators():
    """Test system health indicators for dashboard"""
    config = OnPatrolConfig()
    
    # Test healthy system
    with patch('StatusAPI.get_system_components_status') as mock_status:
        mock_status.return_value = {
            'webserver': {'status': 'running'},
            'smtp_server': {'status': 'running'},
            'telegram_notifier': {'status': 'running'}
        }
        
        request = create_mock_request('GET', '/api/status')
        response = await get_system_status_handler(request, config)
        data = json.loads(response.text)
        
        assert data['system']['status'] == 'healthy'
        
    # Test degraded system (one component down)
    with patch('StatusAPI.get_system_components_status') as mock_status:
        mock_status.return_value = {
            'webserver': {'status': 'running'},
            'smtp_server': {'status': 'stopped'},
            'telegram_notifier': {'status': 'running'}
        }
        
        request = create_mock_request('GET', '/api/status')
        response = await get_system_status_handler(request, config)
        data = json.loads(response.text)
        
        assert data['system']['status'] == 'degraded'
        
    print("✓ Dashboard health indicators work")

@pytest.mark.asyncio
async def test_dashboard_component_details():
    """Test detailed component information for dashboard cards"""
    config = OnPatrolConfig()
    
    with patch('StatusAPI.get_detailed_component_status') as mock_details:
        mock_details.return_value = {
            'webserver': {
                'status': 'running',
                'uptime_seconds': 86400,
                'active_connections': 12,
                'total_requests': 1542,
                'port': 8080
            },
            'smtp_server': {
                'status': 'running', 
                'port': 25,
                'messages_today': 45,
                'queue_size': 0,
                'uptime_seconds': 86400
            },
            'telegram_notifier': {
                'status': 'running',
                'bot_connected': True,
                'messages_sent': 156,
                'last_message_time': '2025-01-17T14:30:00Z'
            }
        }
        
        request = create_mock_request('GET', '/api/status/components')
        response = await get_component_status_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        
        # Test webserver details
        webserver = data['components']['webserver']
        assert webserver['active_connections'] == 12
        assert webserver['port'] == 8080
        
        # Test SMTP server details
        smtp = data['components']['smtp_server']
        assert smtp['messages_today'] == 45
        assert smtp['queue_size'] == 0
        
        # Test Telegram bot details
        telegram = data['components']['telegram_notifier']
        assert telegram['bot_connected'] is True
        assert telegram['messages_sent'] == 156
        
        print("✓ Dashboard component details work")

@pytest.mark.asyncio
async def test_dashboard_mobile_optimization():
    """Test dashboard data structures for mobile display"""
    config = OnPatrolConfig()
    config.cameras = {
        'cam1': CameraConfig(camera_name='Camera 1', address='192.168.1.100')
    }
    
    with patch('StatusAPI.get_system_components_status') as mock_status, \
         patch('StatusAPI.get_all_camera_status') as mock_cameras, \
         patch('StatusAPI.get_performance_metrics') as mock_metrics:
        
        mock_status.return_value = {
            'webserver': {'status': 'running'},
            'smtp_server': {'status': 'running'}
        }
        
        mock_cameras.return_value = {
            'cam1': {'name': 'Camera 1', 'status': 'online'}
        }
        
        mock_metrics.return_value = {
            'system': {'cpu_percent': 15.5, 'memory_percent': 32.1}
        }
        
        # Test that all endpoints return mobile-friendly data structures
        status_request = create_mock_request('GET', '/api/status')
        status_response = await get_system_status_handler(status_request, config)
        status_data = json.loads(status_response.text)
        
        camera_request = create_mock_request('GET', '/api/status/cameras') 
        camera_response = await get_camera_status_handler(camera_request, config)
        camera_data = json.loads(camera_response.text)
        
        metrics_request = create_mock_request('GET', '/api/status/metrics')
        metrics_response = await get_performance_metrics_handler(metrics_request, config)
        metrics_data = json.loads(metrics_response.text)
        
        # Verify data structures are suitable for mobile display
        assert isinstance(status_data['system'], dict)
        assert isinstance(camera_data['summary'], dict)
        assert isinstance(metrics_data['system'], dict)
        
        print("✓ Dashboard mobile optimization works")

async def run_all_dashboard_tests():
    """Run all dashboard tests"""
    try:
        await test_dashboard_system_health_aggregation()
        await test_dashboard_performance_metrics()
        await test_camera_status_for_dashboard()
        await test_websocket_real_time_updates()
        await test_dashboard_health_indicators()
        await test_dashboard_component_details()
        await test_dashboard_mobile_optimization()
        
        print("\n✅ ALL DASHBOARD TESTS PASSED!")
        return True
    except Exception as ex:
        print(f"\n❌ DASHBOARD TEST FAILED: {ex}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    print("Running system status dashboard tests...")
    success = asyncio.run(run_all_dashboard_tests())
    
    if success:
        print("\n🎉 Dashboard functionality is fully tested and working!")
    else:
        print("\n❌ Some dashboard tests failed - check the output above")