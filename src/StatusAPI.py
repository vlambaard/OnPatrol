#!/usr/bin/env python3
"""
System Status Monitoring API for OnPatrol

This module provides RESTful API endpoints for monitoring system health,
component status, camera connectivity, and performance metrics. It includes
real-time status updates via WebSocket connections.

Features:
- System health aggregation and component status monitoring
- Camera connectivity and event statistics
- Performance metrics collection (CPU, memory, disk, application stats)
- Real-time status updates via WebSocket
- Detailed error reporting and health checks

Author: OnPatrol Team
Version: 1.0.0
"""

import json
import asyncio
import logging
import threading
import time
from typing import Dict, Any, Optional, AsyncIterator
from datetime import datetime, timedelta
import psutil
import os

from aiohttp import web, WSMsgType
from aiohttp.web_ws import WebSocketResponse
import aiohttp
from config import OnPatrolConfig

logger = logging.getLogger('patrol_bot')


def create_status_routes(config: OnPatrolConfig) -> list:
    """Create system status monitoring routes
    
    Args:
        config: OnPatrol configuration instance
        
    Returns:
        list: aiohttp routes for status monitoring
    """
    
    routes = []
    
    # System status endpoints
    routes.append(web.get('/api/status', lambda req: get_system_status_handler(req, config)))
    routes.append(web.get('/api/status/components', lambda req: get_component_status_handler(req, config)))
    routes.append(web.get('/api/status/components/{component_id}', lambda req: get_component_status_handler(req, config)))
    routes.append(web.get('/api/status/cameras', lambda req: get_camera_status_handler(req, config)))
    routes.append(web.get('/api/status/metrics', lambda req: get_performance_metrics_handler(req, config)))
    
    # WebSocket for real-time updates
    routes.append(web.get('/api/status/ws', lambda req: status_websocket_handler(req, config)))
    
    return routes


async def get_system_status_handler(request: web.Request, config: OnPatrolConfig) -> web.Response:
    """GET /api/status - Overall system health status
    
    Returns:
        JSON response with system health summary and component overview
    """
    try:
        # Get status of all components
        components_status = await get_system_components_status(config)
        
        # Calculate overall system health
        total_components = len(components_status)
        healthy_components = sum(1 for comp in components_status.values() 
                               if comp.get('status') == 'running')
        
        if healthy_components == total_components:
            system_status = 'healthy'
        elif healthy_components > 0:
            system_status = 'degraded'
        else:
            system_status = 'critical'
        
        response_data = {
            'system': {
                'status': system_status,
                'total_components': total_components,
                'healthy_components': healthy_components,
                'degraded_components': total_components - healthy_components,
                'uptime_seconds': get_system_uptime()
            },
            'components': components_status,
            'timestamp': datetime.now().isoformat(),
            'server_info': {
                'version': get_server_version(),
                'python_version': f"{psutil.sys.version_info.major}.{psutil.sys.version_info.minor}",
                'pid': os.getpid()
            }
        }
        
        logger.debug(f'[StatusAPI] System status: {system_status} ({healthy_components}/{total_components} healthy)')
        return web.json_response(response_data)
        
    except Exception as ex:
        logger.error(f'[StatusAPI] Error getting system status: {ex}')
        return web.json_response(
            {'error': 'Internal server error', 'details': str(ex)},
            status=500
        )


async def get_component_status_handler(request: web.Request, config: OnPatrolConfig) -> web.Response:
    """GET /api/status/components or /api/status/components/{component_id}
    
    Args:
        request: HTTP request, may include component_id parameter
        
    Returns:
        JSON response with detailed component status information
    """
    try:
        component_id = request.match_info.get('component_id')
        
        if component_id:
            # Get specific component status
            component_status = await get_component_status_by_id(component_id, config)
            
            if not component_status:
                return web.json_response(
                    {'error': 'Component not found', 'component_id': component_id},
                    status=404
                )
            
            response_data = {
                'component_id': component_id,
                **component_status,
                'timestamp': datetime.now().isoformat()
            }
        else:
            # Get all component statuses with detailed information
            detailed_status = await get_detailed_component_status(config)
            
            response_data = {
                'components': detailed_status,
                'timestamp': datetime.now().isoformat(),
                'component_count': len(detailed_status)
            }
        
        logger.debug(f'[StatusAPI] Component status requested: {component_id or "all"}')
        return web.json_response(response_data)
        
    except Exception as ex:
        logger.error(f'[StatusAPI] Error getting component status: {ex}')
        return web.json_response(
            {'error': 'Internal server error', 'details': str(ex)},
            status=500
        )


async def get_camera_status_handler(request: web.Request, config: OnPatrolConfig) -> web.Response:
    """GET /api/status/cameras - Camera connectivity and event status
    
    Returns:
        JSON response with camera status summary and detailed information
    """
    try:
        camera_status = await get_all_camera_status(config)
        
        # Calculate summary statistics
        total_cameras = len(camera_status)
        online_cameras = sum(1 for status in camera_status.values() 
                           if status.get('status') == 'online')
        offline_cameras = total_cameras - online_cameras
        
        # Calculate total events today
        today_events = sum(status.get('events_today', 0) for status in camera_status.values())
        
        response_data = {
            'cameras': camera_status,
            'summary': {
                'total': total_cameras,
                'online': online_cameras,
                'offline': offline_cameras,
                'events_today': today_events
            },
            'timestamp': datetime.now().isoformat()
        }
        
        logger.debug(f'[StatusAPI] Camera status: {online_cameras}/{total_cameras} online')
        return web.json_response(response_data)
        
    except Exception as ex:
        logger.error(f'[StatusAPI] Error getting camera status: {ex}')
        return web.json_response(
            {'error': 'Internal server error', 'details': str(ex)},
            status=500
        )


async def get_performance_metrics_handler(request: web.Request, config: OnPatrolConfig) -> web.Response:
    """GET /api/status/metrics - System and application performance metrics
    
    Returns:
        JSON response with system resource usage and application statistics
    """
    try:
        metrics = await get_performance_metrics(config)
        
        response_data = {
            **metrics,
            'timestamp': datetime.now().isoformat(),
            'collection_interval_seconds': 60  # How often metrics are collected
        }
        
        logger.debug(f'[StatusAPI] Performance metrics: CPU {metrics["system"]["cpu_percent"]}%, Memory {metrics["system"]["memory_percent"]}%')
        return web.json_response(response_data)
        
    except Exception as ex:
        logger.error(f'[StatusAPI] Error getting performance metrics: {ex}')
        return web.json_response(
            {'error': 'Internal server error', 'details': str(ex)},
            status=500
        )


async def status_websocket_handler(request: web.Request, config: OnPatrolConfig) -> WebSocketResponse:
    """WebSocket /api/status/ws - Real-time status updates
    
    Provides live status updates to connected clients including:
    - Component status changes
    - Camera connectivity updates  
    - Performance metric updates
    - System alerts and notifications
    
    Returns:
        WebSocket connection for real-time updates
    """
    ws = WebSocketResponse()
    await ws.prepare(request)
    
    logger.info('[StatusAPI] WebSocket client connected for status updates')
    
    try:
        # Send initial status
        initial_status = {
            'type': 'initial_status',
            'data': await get_system_components_status(config),
            'timestamp': datetime.now().isoformat()
        }
        await ws.send_str(json.dumps(initial_status))
        
        # Start status update stream
        async for update in get_status_updates_stream(config):
            if ws.closed:
                break
                
            update_message = {
                'type': 'status_update',
                'data': update,
                'timestamp': datetime.now().isoformat()
            }
            
            try:
                await ws.send_str(json.dumps(update_message))
            except ConnectionResetError:
                logger.debug('[StatusAPI] WebSocket client disconnected')
                break
                
    except Exception as ex:
        logger.error(f'[StatusAPI] WebSocket error: {ex}')
    finally:
        logger.info('[StatusAPI] WebSocket client disconnected')
        
    return ws


async def get_system_components_status(config: OnPatrolConfig) -> Dict[str, Any]:
    """Get basic status of all system components
    
    Returns:
        Dictionary with component names and basic status information
    """
    components = {}
    
    # Check WebServer status
    components['webserver'] = await get_webserver_status()
    
    # Check SMTP Server status
    components['smtp_server'] = await get_smtp_server_status(config)
    
    # Check Telegram Notifier status
    components['telegram_notifier'] = await get_telegram_notifier_status(config)
    
    # Check Notification Recorder status
    components['notification_recorder'] = await get_notification_recorder_status()
    
    # Check DeepStack Client status
    components['deepstack_client'] = await get_deepstack_client_status(config)
    
    return components


async def get_detailed_component_status(config: OnPatrolConfig) -> Dict[str, Any]:
    """Get detailed status information for all components
    
    Returns:
        Dictionary with detailed component status including metrics and activity
    """
    detailed_status = {}
    
    # WebServer detailed status
    detailed_status['webserver'] = {
        **await get_webserver_status(),
        'active_connections': get_active_connections(),
        'requests_processed': get_requests_processed(),
        'last_request': get_last_request_time()
    }
    
    # SMTP Server detailed status
    smtp_status = await get_smtp_server_status(config)
    detailed_status['smtp_server'] = {
        **smtp_status,
        'emails_processed': get_emails_processed_count(),
        'queue_size': get_email_queue_size(),
        'last_email': get_last_email_time()
    }
    
    # Telegram Notifier detailed status
    telegram_status = await get_telegram_notifier_status(config)
    detailed_status['telegram_notifier'] = {
        **telegram_status,
        'messages_sent': get_telegram_messages_sent(),
        'last_message': get_last_telegram_message_time(),
        'chat_groups': len(config.notifications) if config.notifications else 0
    }
    
    # Notification Recorder detailed status
    detailed_status['notification_recorder'] = {
        **await get_notification_recorder_status(),
        'events_processed': get_events_processed_count(),
        'last_event': get_last_event_time(),
        'database_size': get_database_size()
    }
    
    # DeepStack Client detailed status
    deepstack_status = await get_deepstack_client_status(config)
    detailed_status['deepstack_client'] = {
        **deepstack_status,
        'detections_processed': get_detections_processed_count(),
        'average_response_time_ms': get_average_detection_time(),
        'last_detection': get_last_detection_time()
    }
    
    return detailed_status


async def get_component_status_by_id(component_id: str, config: OnPatrolConfig) -> Optional[Dict[str, Any]]:
    """Get status for a specific component by ID
    
    Args:
        component_id: Component identifier (e.g., 'webserver', 'smtp_server')
        
    Returns:
        Component status dictionary or None if component not found
    """
    component_handlers = {
        'webserver': get_webserver_status,
        'smtp_server': lambda: get_smtp_server_status(config),
        'telegram_notifier': lambda: get_telegram_notifier_status(config),
        'notification_recorder': get_notification_recorder_status,
        'deepstack_client': lambda: get_deepstack_client_status(config)
    }
    
    handler = component_handlers.get(component_id)
    if not handler:
        return None
        
    return await handler()


async def get_all_camera_status(config: OnPatrolConfig) -> Dict[str, Any]:
    """Get status of all configured cameras
    
    Returns:
        Dictionary with camera IDs and their status information
    """
    camera_status = {}
    
    for camera_id, camera_config in config.cameras.items():
        try:
            # Test camera connectivity (reuse from ConfigWebAPI)
            from ConfigWebAPI import test_camera_connection
            connectivity = await test_camera_connection(camera_config)
            
            # Determine overall status
            if connectivity['ping_successful'] and connectivity.get('rtsp_connected', True):
                status = 'online'
            else:
                status = 'offline'
            
            camera_status[camera_id] = {
                'name': camera_config.camera_name,
                'address': camera_config.address,
                'status': status,
                'ping_response_ms': connectivity.get('response_time_ms'),
                'rtsp_connected': connectivity.get('rtsp_connected', False),
                'email_valid': connectivity.get('email_valid', True),
                'last_event': get_camera_last_event_time(camera_id),
                'events_today': get_camera_events_today(camera_id),
                'error': connectivity.get('errors', [{}])[0].get('error') if connectivity.get('errors') else None
            }
            
        except Exception as ex:
            camera_status[camera_id] = {
                'name': camera_config.camera_name,
                'address': camera_config.address,
                'status': 'error',
                'error': str(ex),
                'events_today': 0
            }
    
    return camera_status


async def get_performance_metrics(config: OnPatrolConfig) -> Dict[str, Any]:
    """Get system and application performance metrics
    
    Returns:
        Dictionary with system resource usage and application statistics
    """
    # System metrics using psutil
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    load_avg = psutil.getloadavg() if hasattr(psutil, 'getloadavg') else [0, 0, 0]
    
    system_metrics = {
        'cpu_percent': round(cpu_percent, 1),
        'memory_percent': round(memory.percent, 1),
        'memory_used_mb': round(memory.used / 1024 / 1024, 1),
        'memory_total_mb': round(memory.total / 1024 / 1024, 1),
        'disk_percent': round(disk.percent, 1),
        'disk_used_gb': round(disk.used / 1024 / 1024 / 1024, 1),
        'disk_total_gb': round(disk.total / 1024 / 1024 / 1024, 1),
        'uptime_seconds': get_system_uptime(),
        'load_average': [round(load, 2) for load in load_avg]
    }
    
    # Application metrics
    application_metrics = {
        'events_processed_today': get_events_processed_today(),
        'emails_processed_today': get_emails_processed_today(), 
        'notifications_sent_today': get_notifications_sent_today(),
        'detections_processed_today': get_detections_processed_today(),
        'average_event_processing_time_ms': get_average_processing_time(),
        'error_count_today': get_error_count_today(),
        'camera_events_today': get_total_camera_events_today()
    }
    
    # Camera metrics summary
    camera_metrics = {
        'total_cameras': len(config.cameras),
        'online_cameras': await get_online_camera_count(config),
        'average_response_time_ms': await get_average_camera_response_time(config),
        'events_captured_today': application_metrics['camera_events_today']
    }
    
    return {
        'system': system_metrics,
        'application': application_metrics,
        'cameras': camera_metrics
    }


async def get_status_updates_stream(config: OnPatrolConfig) -> AsyncIterator[Dict[str, Any]]:
    """Generate real-time status updates for WebSocket clients
    
    Yields:
        Status update dictionaries with component changes and metrics
    """
    last_status = {}
    
    while True:
        try:
            # Get current status
            current_status = await get_system_components_status(config)
            
            # Check for changes
            for component, status in current_status.items():
                if component not in last_status or last_status[component] != status:
                    yield {
                        'type': 'component_status',
                        'component': component,
                        **status
                    }
            
            # Update camera status periodically
            camera_updates = await check_camera_status_changes(config)
            for camera_id, status in camera_updates.items():
                yield {
                    'type': 'camera_status',
                    'camera_id': camera_id,
                    **status
                }
            
            # Send performance metrics every 30 seconds
            if int(time.time()) % 30 == 0:
                metrics = await get_performance_metrics(config)
                yield {
                    'type': 'performance_metrics',
                    **metrics['system']
                }
            
            last_status = current_status
            await asyncio.sleep(5)  # Update every 5 seconds
            
        except Exception as ex:
            logger.error(f'[StatusAPI] Error in status updates stream: {ex}')
            await asyncio.sleep(10)


# Component Status Functions
async def get_webserver_status() -> Dict[str, Any]:
    """Get WebServer component status"""
    # Check if WebServer thread is running
    webserver_thread = get_thread_by_name('WebServer')
    
    if webserver_thread and webserver_thread.is_alive():
        return {
            'status': 'running',
            'uptime_seconds': get_thread_uptime('WebServer'),
            'thread_id': webserver_thread.ident
        }
    else:
        return {
            'status': 'stopped',
            'error': 'WebServer thread not found or not running'
        }


async def get_smtp_server_status(config: OnPatrolConfig) -> Dict[str, Any]:
    """Get SMTP Server component status"""
    if not config.smtp.enabled:
        return {'status': 'disabled', 'reason': 'SMTP server disabled in configuration'}
    
    # Check if SMTP thread is running (would need to track this in actual implementation)
    smtp_active = check_smtp_server_active(config.smtp.host, config.smtp.port)
    
    if smtp_active:
        return {
            'status': 'running',
            'host': config.smtp.host,
            'port': config.smtp.port
        }
    else:
        return {
            'status': 'error',
            'error': f'SMTP server not responding on {config.smtp.host}:{config.smtp.port}'
        }


async def get_telegram_notifier_status(config: OnPatrolConfig) -> Dict[str, Any]:
    """Get Telegram Notifier component status"""
    if not config.telegram.enabled:
        return {'status': 'disabled', 'reason': 'Telegram notifications disabled in configuration'}
    
    telegram_thread = get_thread_by_name('TelegramNotifier')
    
    if telegram_thread and telegram_thread.is_alive():
        return {
            'status': 'running',
            'bot_connected': True,  # Would check actual bot connection in real implementation
            'bot_token_valid': bool(config.telegram.bot_token)
        }
    else:
        return {
            'status': 'stopped',
            'error': 'TelegramNotifier thread not found or not running'
        }


async def get_notification_recorder_status() -> Dict[str, Any]:
    """Get Notification Recorder component status"""
    recorder_thread = get_thread_by_name('NotificationRecorder')
    
    if recorder_thread and recorder_thread.is_alive():
        return {
            'status': 'running',
            'uptime_seconds': get_thread_uptime('NotificationRecorder')
        }
    else:
        return {
            'status': 'stopped',
            'error': 'NotificationRecorder thread not found or not running'
        }


async def get_deepstack_client_status(config: OnPatrolConfig) -> Dict[str, Any]:
    """Get DeepStack Client component status"""
    if not config.deepstack.enabled:
        return {'status': 'disabled', 'reason': 'DeepStack AI disabled in configuration'}
    
    deepstack_thread = get_thread_by_name('DeepStackClient')
    
    if deepstack_thread and deepstack_thread.is_alive():
        # Test DeepStack API connectivity
        api_connected = await test_deepstack_api_connection(config.deepstack.url)
        
        return {
            'status': 'running',
            'api_connected': api_connected,
            'server_url': config.deepstack.url
        }
    else:
        return {
            'status': 'stopped',
            'error': 'DeepStackClient thread not found or not running'
        }


# Helper Functions (These would need actual implementations)
def get_thread_by_name(name: str) -> Optional[threading.Thread]:
    """Find thread by name"""
    for thread in threading.enumerate():
        if thread.name == name:
            return thread
    return None


def get_system_uptime() -> int:
    """Get system uptime in seconds"""
    try:
        return int(time.time() - psutil.boot_time())
    except:
        return 0


def get_server_version() -> str:
    """Get OnPatrol server version"""
    try:
        import OnPatrolServer
        return getattr(OnPatrolServer, '__version__', '1.0.0')
    except:
        return '1.0.0'


def get_thread_uptime(thread_name: str) -> int:
    """Get thread uptime in seconds (would need to track start times)"""
    return 0  # Placeholder


def check_smtp_server_active(host: str, port: int) -> bool:
    """Check if SMTP server is active"""
    return True  # Placeholder


async def test_deepstack_api_connection(url: str) -> bool:
    """Test DeepStack API connectivity"""
    return True  # Placeholder


def get_active_connections() -> int:
    """Get number of active WebServer connections"""
    return 0  # Placeholder


def get_requests_processed() -> int:
    """Get total requests processed"""
    return 0  # Placeholder


def get_last_request_time() -> Optional[str]:
    """Get timestamp of last request"""
    return None  # Placeholder


def get_emails_processed_count() -> int:
    """Get total emails processed"""
    return 0  # Placeholder


def get_email_queue_size() -> int:
    """Get current email queue size"""
    return 0  # Placeholder


def get_last_email_time() -> Optional[str]:
    """Get timestamp of last email processed"""
    return None  # Placeholder


def get_telegram_messages_sent() -> int:
    """Get total Telegram messages sent"""
    return 0  # Placeholder


def get_last_telegram_message_time() -> Optional[str]:
    """Get timestamp of last Telegram message"""
    return None  # Placeholder


def get_events_processed_count() -> int:
    """Get total events processed"""
    return 0  # Placeholder


def get_last_event_time() -> Optional[str]:
    """Get timestamp of last event"""
    return None  # Placeholder


def get_database_size() -> int:
    """Get database size in bytes"""
    return 0  # Placeholder


def get_detections_processed_count() -> int:
    """Get total AI detections processed"""
    return 0  # Placeholder


def get_average_detection_time() -> float:
    """Get average detection processing time in milliseconds"""
    return 0.0  # Placeholder


def get_last_detection_time() -> Optional[str]:
    """Get timestamp of last AI detection"""
    return None  # Placeholder


def get_camera_last_event_time(camera_id: str) -> Optional[str]:
    """Get timestamp of last event for specific camera"""
    return None  # Placeholder


def get_camera_events_today(camera_id: str) -> int:
    """Get event count for specific camera today"""
    return 0  # Placeholder


def get_events_processed_today() -> int:
    """Get events processed today"""
    return 0  # Placeholder


def get_emails_processed_today() -> int:
    """Get emails processed today"""
    return 0  # Placeholder


def get_notifications_sent_today() -> int:
    """Get notifications sent today"""
    return 0  # Placeholder


def get_detections_processed_today() -> int:
    """Get AI detections processed today"""
    return 0  # Placeholder


def get_average_processing_time() -> float:
    """Get average event processing time in milliseconds"""
    return 0.0  # Placeholder


def get_error_count_today() -> int:
    """Get error count today"""
    return 0  # Placeholder


def get_total_camera_events_today() -> int:
    """Get total camera events today"""
    return 0  # Placeholder


async def get_online_camera_count(config: OnPatrolConfig) -> int:
    """Get count of online cameras"""
    return len(config.cameras)  # Placeholder


async def get_average_camera_response_time(config: OnPatrolConfig) -> float:
    """Get average camera response time in milliseconds"""
    return 0.0  # Placeholder


async def check_camera_status_changes(config: OnPatrolConfig) -> Dict[str, Any]:
    """Check for camera status changes"""
    return {}  # Placeholder