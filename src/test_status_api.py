#!/usr/bin/env python3
"""
Tests for System Status Monitoring API endpoints

This module tests the RESTful API endpoints for system status monitoring,
health checks, and real-time status updates via WebSocket.
"""

import pytest
import json
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
from config import OnPatrolConfig, CameraConfig
from StatusAPI import (
    get_system_status_handler,
    get_component_status_handler, 
    get_camera_status_handler,
    get_performance_metrics_handler,
    status_websocket_handler
)

def create_mock_request(method, path, match_info=None, query_params=None):
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
async def test_get_system_status():
    """Test GET /api/status - overall system status"""
    # Setup
    config = OnPatrolConfig()
    
    # Mock system components
    mock_components = {
        'webserver': {'status': 'running', 'uptime': 3600},
        'smtp_server': {'status': 'running', 'port': 25},
        'telegram_notifier': {'status': 'running', 'bot_connected': True},
        'notification_recorder': {'status': 'running', 'events_processed': 150},
        'deepstack_client': {'status': 'running', 'api_connected': True}
    }
    
    with patch('StatusAPI.get_system_components_status') as mock_status:
        mock_status.return_value = mock_components
        
        # Create request
        request = create_mock_request('GET', '/api/status')
        
        # Execute
        response = await get_system_status_handler(request, config)
        
        # Assert
        assert response.status == 200
        data = json.loads(response.text)
        assert 'system' in data
        assert 'components' in data
        assert 'timestamp' in data
        assert data['system']['status'] == 'healthy'
        assert len(data['components']) == 5


@pytest.mark.asyncio
async def test_get_system_status_unhealthy():
    """Test system status when components are failing"""
    # Setup
    config = OnPatrolConfig()
    
    # Mock failing components
    mock_components = {
        'webserver': {'status': 'running', 'uptime': 3600},
        'smtp_server': {'status': 'error', 'error': 'Port 25 in use'},
        'telegram_notifier': {'status': 'running', 'bot_connected': True},
        'notification_recorder': {'status': 'stopped', 'last_error': 'Database connection failed'},
        'deepstack_client': {'status': 'error', 'api_connected': False}
    }
    
    with patch('StatusAPI.get_system_components_status') as mock_status:
        mock_status.return_value = mock_components
        
        request = create_mock_request('GET', '/api/status')
        response = await get_system_status_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        assert data['system']['status'] == 'degraded'
        assert data['system']['healthy_components'] == 2
        assert data['system']['total_components'] == 5


@pytest.mark.asyncio
async def test_get_component_status():
    """Test GET /api/status/components - detailed component status"""
    # Setup
    config = OnPatrolConfig()
    
    mock_detailed_status = {
        'webserver': {
            'status': 'running',
            'uptime_seconds': 3600,
            'active_connections': 5,
            'requests_processed': 1250,
            'last_request': '2025-09-05T14:30:15Z'
        },
        'smtp_server': {
            'status': 'running',
            'port': 25,
            'emails_processed': 45,
            'last_email': '2025-09-05T14:25:33Z',
            'queue_size': 0
        },
        'deepstack_client': {
            'status': 'running',
            'api_connected': True,
            'detections_processed': 89,
            'average_response_time_ms': 245,
            'last_detection': '2025-09-05T14:28:12Z'
        }
    }
    
    with patch('StatusAPI.get_detailed_component_status') as mock_status:
        mock_status.return_value = mock_detailed_status
        
        request = create_mock_request('GET', '/api/status/components')
        response = await get_component_status_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        assert 'components' in data
        assert 'webserver' in data['components']
        assert data['components']['webserver']['uptime_seconds'] == 3600
        assert data['components']['smtp_server']['emails_processed'] == 45


@pytest.mark.asyncio
async def test_get_specific_component_status():
    """Test GET /api/status/components/{component_id} - specific component"""
    config = OnPatrolConfig()
    
    mock_component_status = {
        'status': 'running',
        'uptime_seconds': 7200,
        'bot_connected': True,
        'messages_sent': 23,
        'last_message': '2025-09-05T14:20:45Z',
        'chat_groups': 3
    }
    
    with patch('StatusAPI.get_component_status_by_id') as mock_status:
        mock_status.return_value = mock_component_status
        
        request = create_mock_request('GET', '/api/status/components/telegram_notifier',
                                    {'component_id': 'telegram_notifier'})
        response = await get_component_status_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        assert data['component_id'] == 'telegram_notifier'
        assert data['status'] == 'running'
        assert data['messages_sent'] == 23


@pytest.mark.asyncio
async def test_get_camera_status():
    """Test GET /api/status/cameras - camera connectivity status"""
    # Setup
    config = OnPatrolConfig()
    config.cameras = {
        'camera1': CameraConfig(
            camera_name='Front Door',
            address='192.168.1.100',
            rtsp_recording_enabled=True,
            email_enabled=True
        ),
        'camera2': CameraConfig(
            camera_name='Back Yard',
            address='192.168.1.101',
            rtsp_recording_enabled=False,
            email_enabled=True
        )
    }
    
    mock_camera_status = {
        'camera1': {
            'name': 'Front Door',
            'status': 'online',
            'ping_response_ms': 45,
            'rtsp_connected': True,
            'email_valid': True,
            'last_event': '2025-09-05T14:15:30Z',
            'events_today': 8
        },
        'camera2': {
            'name': 'Back Yard', 
            'status': 'offline',
            'ping_response_ms': None,
            'rtsp_connected': False,
            'email_valid': True,
            'last_event': '2025-09-04T22:45:12Z',
            'events_today': 0,
            'error': 'Connection timeout'
        }
    }
    
    with patch('StatusAPI.get_all_camera_status') as mock_status:
        mock_status.return_value = mock_camera_status
        
        request = create_mock_request('GET', '/api/status/cameras')
        response = await get_camera_status_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        assert 'cameras' in data
        assert 'summary' in data
        assert len(data['cameras']) == 2
        assert data['summary']['total'] == 2
        assert data['summary']['online'] == 1
        assert data['summary']['offline'] == 1


@pytest.mark.asyncio
async def test_get_performance_metrics():
    """Test GET /api/status/metrics - performance metrics"""
    config = OnPatrolConfig()
    
    mock_metrics = {
        'system': {
            'cpu_percent': 15.2,
            'memory_percent': 32.8,
            'disk_percent': 45.1,
            'uptime_seconds': 86400,
            'load_average': [0.5, 0.7, 0.9]
        },
        'application': {
            'events_processed_today': 145,
            'emails_processed_today': 23,
            'notifications_sent_today': 18,
            'average_event_processing_time_ms': 120,
            'error_count_today': 2
        },
        'cameras': {
            'total_cameras': 3,
            'online_cameras': 2,
            'average_response_time_ms': 67,
            'events_captured_today': 145
        }
    }
    
    with patch('StatusAPI.get_performance_metrics') as mock_metrics_func:
        mock_metrics_func.return_value = mock_metrics
        
        request = create_mock_request('GET', '/api/status/metrics')
        response = await get_performance_metrics_handler(request, config)
        
        assert response.status == 200
        data = json.loads(response.text)
        assert 'system' in data
        assert 'application' in data
        assert 'cameras' in data
        assert data['system']['cpu_percent'] == 15.2
        assert data['application']['events_processed_today'] == 145


@pytest.mark.asyncio
async def test_status_websocket_handler():
    """Test WebSocket /api/status/ws - real-time status updates"""
    config = OnPatrolConfig()
    
    # Create mock WebSocket
    ws = create_mock_websocket()
    
    # Mock status updates
    mock_updates = [
        {'type': 'component_status', 'component': 'smtp_server', 'status': 'running'},
        {'type': 'camera_status', 'camera_id': 'camera1', 'status': 'online'},
        {'type': 'system_metrics', 'cpu_percent': 18.5}
    ]
    
    with patch('StatusAPI.get_status_updates_stream') as mock_stream:
        # Mock the async generator
        async def mock_status_stream():
            for update in mock_updates:
                yield update
        
        mock_stream.return_value = mock_status_stream()
        
        request = create_mock_request('GET', '/api/status/ws')
        
        # Execute WebSocket handler (this would normally run indefinitely)
        with patch('aiohttp.web_ws.WebSocketResponse') as mock_ws_response:
            mock_ws_response.return_value = ws
            
            # This would be called by aiohttp framework
            await status_websocket_handler(request, config)
            
            # Verify WebSocket messages were sent
            assert ws.send_str.call_count >= 1


@pytest.mark.asyncio
async def test_websocket_client_disconnect():
    """Test WebSocket handles client disconnection gracefully"""
    config = OnPatrolConfig()
    
    ws = create_mock_websocket()
    ws.closed = True  # Simulate disconnected client
    
    request = create_mock_request('GET', '/api/status/ws')
    
    with patch('aiohttp.web_ws.WebSocketResponse') as mock_ws_response:
        mock_ws_response.return_value = ws
        
        # Should handle disconnection gracefully without throwing
        try:
            await status_websocket_handler(request, config)
            # If we get here, the handler handled disconnection properly
            assert True
        except Exception as ex:
            pytest.fail(f"WebSocket handler should handle disconnection gracefully: {ex}")


@pytest.mark.asyncio
async def test_status_api_error_handling():
    """Test error handling in status API endpoints"""
    config = OnPatrolConfig()
    
    # Test system status with exception
    with patch('StatusAPI.get_system_components_status') as mock_status:
        mock_status.side_effect = Exception("Database connection failed")
        
        request = create_mock_request('GET', '/api/status')
        response = await get_system_status_handler(request, config)
        
        assert response.status == 500
        data = json.loads(response.text)
        assert 'error' in data
        assert 'Internal server error' in data['error']


@pytest.mark.asyncio
async def test_component_not_found():
    """Test component status for non-existent component"""
    config = OnPatrolConfig()
    
    with patch('StatusAPI.get_component_status_by_id') as mock_status:
        mock_status.return_value = None
        
        request = create_mock_request('GET', '/api/status/components/nonexistent',
                                    {'component_id': 'nonexistent'})
        response = await get_component_status_handler(request, config)
        
        assert response.status == 404
        data = json.loads(response.text)
        assert data['error'] == 'Component not found'


if __name__ == '__main__':
    # Run tests directly for debugging
    async def run_tests():
        print("Running status API tests...")
        
        test_functions = [
            test_get_system_status,
            test_get_system_status_unhealthy,
            test_get_component_status,
            test_get_specific_component_status,
            test_get_camera_status,
            test_get_performance_metrics,
            test_status_websocket_handler,
            test_websocket_client_disconnect,
            test_status_api_error_handling,
            test_component_not_found
        ]
        
        for test_func in test_functions:
            try:
                await test_func()
                print(f"✓ {test_func.__name__} passed")
            except Exception as e:
                print(f"❌ {test_func.__name__} failed: {e}")
    
    asyncio.run(run_tests())