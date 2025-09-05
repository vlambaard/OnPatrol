#!/usr/bin/env python3
"""
Configuration Web API for OnPatrol Camera Management

This module provides RESTful API endpoints for managing camera configurations
through a web interface. It integrates with the existing Pydantic configuration
system to provide type-safe, validated camera CRUD operations.

Features:
- RESTful camera CRUD operations (GET, POST, PUT, DELETE)
- Camera connectivity testing endpoints
- Integration with Pydantic validation
- Proper HTTP error handling and responses
- JSON schema validation

Author: OnPatrol Team
Version: 1.0.0
"""

import json
import asyncio
import logging
import uuid
from typing import Dict, Any, Optional
from datetime import datetime

from aiohttp import web, ClientError
from aiohttp.web_response import Response
import aiohttp
from pydantic import ValidationError

from config import OnPatrolConfig, CameraConfig


logger = logging.getLogger('patrol_bot')


def create_camera_routes(config: OnPatrolConfig) -> list:
    """Create camera management routes
    
    Args:
        config: OnPatrol configuration instance
        
    Returns:
        list: aiohttp routes for camera management
    """
    
    routes = []
    
    # Camera CRUD endpoints
    routes.append(web.get('/api/cameras', lambda req: get_cameras_handler(req, config)))
    routes.append(web.get('/api/cameras/{camera_id}', lambda req: get_camera_handler(req, config)))
    routes.append(web.post('/api/cameras', lambda req: create_camera_handler(req, config)))
    routes.append(web.put('/api/cameras/{camera_id}', lambda req: update_camera_handler(req, config)))
    routes.append(web.delete('/api/cameras/{camera_id}', lambda req: delete_camera_handler(req, config)))
    
    # Camera connectivity testing
    routes.append(web.post('/api/cameras/{camera_id}/test', lambda req: test_camera_handler(req, config)))
    
    return routes


async def get_cameras_handler(request: web.Request, config: OnPatrolConfig) -> web.Response:
    """GET /api/cameras - List all cameras
    
    Returns:
        JSON response with all camera configurations
    """
    try:
        cameras_dict = {}
        for camera_id, camera_config in config.cameras.items():
            cameras_dict[camera_id] = camera_config.dict()
        
        response_data = {
            'cameras': cameras_dict,
            'count': len(cameras_dict),
            'timestamp': datetime.now().isoformat()
        }
        
        logger.debug(f'[ConfigWebAPI] Listed {len(cameras_dict)} cameras')
        return web.json_response(response_data)
        
    except Exception as ex:
        logger.error(f'[ConfigWebAPI] Error listing cameras: {ex}')
        return web.json_response(
            {'error': 'Internal server error', 'details': str(ex)}, 
            status=500
        )


async def get_camera_handler(request: web.Request, config: OnPatrolConfig) -> web.Response:
    """GET /api/cameras/{camera_id} - Get specific camera
    
    Args:
        request: HTTP request with camera_id parameter
        
    Returns:
        JSON response with camera configuration or 404 if not found
    """
    try:
        camera_id = request.match_info['camera_id']
        
        if camera_id not in config.cameras:
            return web.json_response(
                {'error': 'Camera not found', 'camera_id': camera_id},
                status=404
            )
        
        camera_config = config.cameras[camera_id]
        response_data = camera_config.dict()
        response_data['id'] = camera_id
        
        logger.debug(f'[ConfigWebAPI] Retrieved camera {camera_id}')
        return web.json_response(response_data)
        
    except Exception as ex:
        logger.error(f'[ConfigWebAPI] Error retrieving camera: {ex}')
        return web.json_response(
            {'error': 'Internal server error', 'details': str(ex)},
            status=500
        )


async def create_camera_handler(request: web.Request, config: OnPatrolConfig) -> web.Response:
    """POST /api/cameras - Create new camera
    
    Args:
        request: HTTP request with JSON camera data
        
    Returns:
        JSON response with created camera data or validation errors
    """
    try:
        # Validate content type
        if not request.content_type or 'application/json' not in request.content_type:
            return web.json_response(
                {'error': 'Content-Type must be application/json'},
                status=400
            )
        
        # Parse request body
        try:
            request_data = await request.json()
        except json.JSONDecodeError as ex:
            return web.json_response(
                {'error': 'Invalid JSON in request body', 'details': str(ex)},
                status=400
            )
        
        # Generate unique camera ID
        camera_id = f"camera_{uuid.uuid4().hex[:8]}"
        while camera_id in config.cameras:
            camera_id = f"camera_{uuid.uuid4().hex[:8]}"
        
        # Validate camera configuration using Pydantic
        try:
            camera_config = CameraConfig(**request_data)
        except ValidationError as ex:
            validation_errors = []
            for error in ex.errors():
                validation_errors.append({
                    'field': '.'.join(str(x) for x in error['loc']),
                    'message': error['msg'],
                    'type': error['type']
                })
            
            return web.json_response({
                'error': 'Validation failed',
                'validation_errors': validation_errors
            }, status=400)
        
        # Add camera to configuration
        config.cameras[camera_id] = camera_config
        
        # Prepare response
        response_data = camera_config.dict()
        response_data['id'] = camera_id
        response_data['created_at'] = datetime.now().isoformat()
        
        logger.info(f'[ConfigWebAPI] Created camera {camera_id}: {camera_config.camera_name}')
        return web.json_response(response_data, status=201)
        
    except Exception as ex:
        logger.error(f'[ConfigWebAPI] Error creating camera: {ex}')
        return web.json_response(
            {'error': 'Internal server error', 'details': str(ex)},
            status=500
        )


async def update_camera_handler(request: web.Request, config: OnPatrolConfig) -> web.Response:
    """PUT /api/cameras/{camera_id} - Update existing camera
    
    Args:
        request: HTTP request with camera_id parameter and JSON update data
        
    Returns:
        JSON response with updated camera data or errors
    """
    try:
        camera_id = request.match_info['camera_id']
        
        # Check if camera exists
        if camera_id not in config.cameras:
            return web.json_response(
                {'error': 'Camera not found', 'camera_id': camera_id},
                status=404
            )
        
        # Validate content type
        if not request.content_type or 'application/json' not in request.content_type:
            return web.json_response(
                {'error': 'Content-Type must be application/json'},
                status=400
            )
        
        # Parse request body
        try:
            request_data = await request.json()
        except json.JSONDecodeError as ex:
            return web.json_response(
                {'error': 'Invalid JSON in request body', 'details': str(ex)},
                status=400
            )
        
        # Get current camera config and merge with updates
        current_config = config.cameras[camera_id]
        current_dict = current_config.dict()
        current_dict.update(request_data)
        
        # Validate updated configuration
        try:
            updated_config = CameraConfig(**current_dict)
        except ValidationError as ex:
            validation_errors = []
            for error in ex.errors():
                validation_errors.append({
                    'field': '.'.join(str(x) for x in error['loc']),
                    'message': error['msg'],
                    'type': error['type']
                })
            
            return web.json_response({
                'error': 'Validation failed',
                'validation_errors': validation_errors
            }, status=400)
        
        # Update camera in configuration
        config.cameras[camera_id] = updated_config
        
        # Prepare response
        response_data = updated_config.dict()
        response_data['id'] = camera_id
        response_data['updated_at'] = datetime.now().isoformat()
        
        logger.info(f'[ConfigWebAPI] Updated camera {camera_id}: {updated_config.camera_name}')
        return web.json_response(response_data)
        
    except Exception as ex:
        logger.error(f'[ConfigWebAPI] Error updating camera: {ex}')
        return web.json_response(
            {'error': 'Internal server error', 'details': str(ex)},
            status=500
        )


async def delete_camera_handler(request: web.Request, config: OnPatrolConfig) -> web.Response:
    """DELETE /api/cameras/{camera_id} - Delete camera
    
    Args:
        request: HTTP request with camera_id parameter
        
    Returns:
        JSON response confirming deletion or error if not found
    """
    try:
        camera_id = request.match_info['camera_id']
        
        # Check if camera exists
        if camera_id not in config.cameras:
            return web.json_response(
                {'error': 'Camera not found', 'camera_id': camera_id},
                status=404
            )
        
        # Get camera name for logging
        camera_name = config.cameras[camera_id].camera_name
        
        # Remove camera from configuration
        del config.cameras[camera_id]
        
        response_data = {
            'message': 'Camera deleted successfully',
            'camera_id': camera_id,
            'camera_name': camera_name,
            'deleted_at': datetime.now().isoformat()
        }
        
        logger.info(f'[ConfigWebAPI] Deleted camera {camera_id}: {camera_name}')
        return web.json_response(response_data)
        
    except Exception as ex:
        logger.error(f'[ConfigWebAPI] Error deleting camera: {ex}')
        return web.json_response(
            {'error': 'Internal server error', 'details': str(ex)},
            status=500
        )


async def test_camera_handler(request: web.Request, config: OnPatrolConfig) -> web.Response:
    """POST /api/cameras/{camera_id}/test - Test camera connectivity
    
    Args:
        request: HTTP request with camera_id parameter
        
    Returns:
        JSON response with connectivity test results
    """
    try:
        camera_id = request.match_info['camera_id']
        
        # Check if camera exists
        if camera_id not in config.cameras:
            return web.json_response(
                {'error': 'Camera not found', 'camera_id': camera_id},
                status=404
            )
        
        camera_config = config.cameras[camera_id]
        
        # Run connectivity tests
        test_results = await test_camera_connection(camera_config)
        test_results['camera_id'] = camera_id
        test_results['camera_name'] = camera_config.camera_name
        test_results['tested_at'] = datetime.now().isoformat()
        
        logger.info(f'[ConfigWebAPI] Tested connectivity for camera {camera_id}')
        return web.json_response(test_results)
        
    except Exception as ex:
        logger.error(f'[ConfigWebAPI] Error testing camera connectivity: {ex}')
        return web.json_response(
            {'error': 'Internal server error', 'details': str(ex)},
            status=500
        )


async def test_camera_connection(camera_config: CameraConfig) -> Dict[str, Any]:
    """Test camera connectivity
    
    Args:
        camera_config: Camera configuration to test
        
    Returns:
        Dictionary with test results
    """
    results = {
        'ping_successful': False,
        'rtsp_connected': False,
        'email_valid': False,
        'response_time_ms': None,
        'errors': []
    }
    
    try:
        # Test basic ping connectivity
        if camera_config.address:
            ping_result = await test_ping_connectivity(camera_config.address)
            results['ping_successful'] = ping_result['success']
            results['response_time_ms'] = ping_result.get('response_time_ms')
            if not ping_result['success']:
                results['errors'].append(f"Ping failed: {ping_result.get('error', 'Unknown error')}")
        
        # Test RTSP connectivity if enabled
        if camera_config.rtsp_recording_enabled and camera_config.address:
            rtsp_result = await test_rtsp_connectivity(camera_config)
            results['rtsp_connected'] = rtsp_result['success']
            if not rtsp_result['success']:
                results['errors'].append(f"RTSP failed: {rtsp_result.get('error', 'Unknown error')}")
        
        # Validate email address format
        if camera_config.email_enabled and camera_config.email_address:
            email_result = validate_email_address(camera_config.email_address)
            results['email_valid'] = email_result['valid']
            if not email_result['valid']:
                results['errors'].append(f"Email invalid: {email_result.get('error', 'Unknown error')}")
        else:
            results['email_valid'] = True  # Not enabled, so considered valid
            
    except Exception as ex:
        results['errors'].append(f"Test error: {str(ex)}")
    
    return results


async def test_ping_connectivity(address: str, timeout: int = 5) -> Dict[str, Any]:
    """Test basic ping connectivity to camera IP
    
    Args:
        address: Camera IP address
        timeout: Timeout in seconds
        
    Returns:
        Dictionary with ping test results
    """
    import subprocess
    import time
    
    try:
        start_time = time.time()
        
        # Use platform-appropriate ping command
        import platform
        if platform.system().lower() == "windows":
            cmd = ["ping", "-n", "1", "-w", str(timeout * 1000), address]
        else:
            cmd = ["ping", "-c", "1", "-W", str(timeout), address]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), 
            timeout=timeout + 1
        )
        
        end_time = time.time()
        response_time_ms = int((end_time - start_time) * 1000)
        
        if process.returncode == 0:
            return {
                'success': True,
                'response_time_ms': response_time_ms
            }
        else:
            return {
                'success': False,
                'error': f'Ping returned code {process.returncode}',
                'stderr': stderr.decode() if stderr else ''
            }
            
    except asyncio.TimeoutError:
        return {
            'success': False,
            'error': 'Ping timeout'
        }
    except Exception as ex:
        return {
            'success': False,
            'error': str(ex)
        }


async def test_rtsp_connectivity(camera_config: CameraConfig, timeout: int = 10) -> Dict[str, Any]:
    """Test RTSP stream connectivity
    
    Args:
        camera_config: Camera configuration
        timeout: Timeout in seconds
        
    Returns:
        Dictionary with RTSP test results
    """
    try:
        # Build RTSP URL
        rtsp_url = f"rtsp://"
        
        if camera_config.username and camera_config.password:
            rtsp_url += f"{camera_config.username}:{camera_config.password}@"
        
        rtsp_url += f"{camera_config.address}:{camera_config.rtsp_port}"
        rtsp_url += camera_config.rtsp_url_path
        
        # Test basic TCP connection to RTSP port
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(camera_config.address, camera_config.rtsp_port),
                timeout=timeout
            )
            
            writer.close()
            await writer.wait_closed()
            
            return {
                'success': True,
                'rtsp_url': rtsp_url.replace(f":{camera_config.password}@", ":***@")  # Hide password
            }
            
        except asyncio.TimeoutError:
            return {
                'success': False,
                'error': 'RTSP connection timeout'
            }
        except ConnectionRefusedError:
            return {
                'success': False,
                'error': 'RTSP connection refused'
            }
            
    except Exception as ex:
        return {
            'success': False,
            'error': str(ex)
        }


def validate_email_address(email: str) -> Dict[str, Any]:
    """Validate email address format
    
    Args:
        email: Email address to validate
        
    Returns:
        Dictionary with validation results
    """
    import re
    
    # Basic email regex pattern
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    if re.match(email_pattern, email):
        return {'valid': True}
    else:
        return {
            'valid': False,
            'error': 'Invalid email address format'
        }