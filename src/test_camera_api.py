#!/usr/bin/env python3
"""
Tests for Camera Management API endpoints

This module tests the RESTful API endpoints for camera CRUD operations.
Uses aiohttp test client to test all camera management functionality.
"""

import pytest
import json
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from config import OnPatrolConfig, CameraConfig
from ConfigWebAPI import create_camera_routes

# Add pytest asyncio marker
pytestmark = pytest.mark.asyncio


class TestCameraAPI(AioHTTPTestCase):
    """Test suite for Camera Management API endpoints"""

    async def get_application(self):
        """Create test application with camera routes"""
        app = web.Application()
        
        # Create mock configuration with minimal required data
        self.mock_config = OnPatrolConfig()
        
        # Manually create camera configs to avoid validation issues
        camera1 = CameraConfig(
            camera_name='Front Door Camera',
            description='Main entrance monitoring',
            address='192.168.1.100',
            email_address='camera1@test.com',
            email_enabled=True,
            rtsp_recording_enabled=True
        )
        
        camera2 = CameraConfig(
            camera_name='Back Yard Camera', 
            description='Rear perimeter monitoring',
            address='192.168.1.101',
            email_address='camera2@test.com',
            email_enabled=False,
            rtsp_recording_enabled=False
        )
        
        self.mock_config.cameras = {
            'camera1': camera1,
            'camera2': camera2
        }
        
        # Add camera routes to test app
        camera_routes = create_camera_routes(self.mock_config)
        app.add_routes(camera_routes)
        
        return app

    @unittest_run_loop
    async def test_get_cameras_list(self):
        """Test GET /api/cameras - list all cameras"""
        resp = await self.client.request('GET', '/api/cameras')
        self.assertEqual(resp.status, 200)
        
        data = await resp.json()
        self.assertIsInstance(data, dict)
        self.assertIn('cameras', data)
        self.assertEqual(len(data['cameras']), 2)
        
        # Verify camera data structure
        camera1 = data['cameras']['camera1']
        self.assertEqual(camera1['camera_name'], 'Front Door Camera')
        self.assertEqual(camera1['address'], '192.168.1.100')
        self.assertTrue(camera1['email_enabled'])

    @unittest_run_loop
    async def test_get_camera_by_id(self):
        """Test GET /api/cameras/{id} - get specific camera"""
        resp = await self.client.request('GET', '/api/cameras/camera1')
        self.assertEqual(resp.status, 200)
        
        data = await resp.json()
        self.assertEqual(data['camera_name'], 'Front Door Camera')
        self.assertEqual(data['description'], 'Main entrance monitoring')
        self.assertEqual(data['address'], '192.168.1.100')

    @unittest_run_loop
    async def test_get_camera_not_found(self):
        """Test GET /api/cameras/{id} - camera not found"""
        resp = await self.client.request('GET', '/api/cameras/nonexistent')
        self.assertEqual(resp.status, 404)
        
        data = await resp.json()
        self.assertIn('error', data)
        self.assertEqual(data['error'], 'Camera not found')

    @unittest_run_loop
    async def test_create_camera(self):
        """Test POST /api/cameras - create new camera"""
        new_camera = {
            'camera_name': 'Side Gate Camera',
            'description': 'Side entrance monitoring',
            'address': '192.168.1.102',
            'email_address': 'camera3@test.com',
            'email_enabled': True,
            'rtsp_recording_enabled': True,
            'username': 'admin',
            'password': 'password123'
        }
        
        resp = await self.client.request(
            'POST', 
            '/api/cameras',
            data=json.dumps(new_camera),
            headers={'Content-Type': 'application/json'}
        )
        self.assertEqual(resp.status, 201)
        
        data = await resp.json()
        self.assertIn('id', data)
        self.assertEqual(data['camera_name'], 'Side Gate Camera')
        
        # Verify camera was added to config
        camera_id = data['id']
        self.assertIn(camera_id, self.mock_config.cameras)

    @unittest_run_loop
    async def test_create_camera_invalid_data(self):
        """Test POST /api/cameras - invalid camera data"""
        invalid_camera = {
            'camera_name': '',  # Empty name should fail validation
            'address': 'invalid-ip'  # Invalid IP format
        }
        
        resp = await self.client.request(
            'POST',
            '/api/cameras',
            data=json.dumps(invalid_camera),
            headers={'Content-Type': 'application/json'}
        )
        self.assertEqual(resp.status, 400)
        
        data = await resp.json()
        self.assertIn('error', data)
        self.assertIn('validation', data['error'].lower())

    @unittest_run_loop
    async def test_update_camera(self):
        """Test PUT /api/cameras/{id} - update existing camera"""
        updated_data = {
            'camera_name': 'Updated Front Door Camera',
            'description': 'Updated main entrance monitoring',
            'email_enabled': False
        }
        
        resp = await self.client.request(
            'PUT',
            '/api/cameras/camera1',
            data=json.dumps(updated_data),
            headers={'Content-Type': 'application/json'}
        )
        self.assertEqual(resp.status, 200)
        
        data = await resp.json()
        self.assertEqual(data['camera_name'], 'Updated Front Door Camera')
        self.assertEqual(data['description'], 'Updated main entrance monitoring')
        self.assertFalse(data['email_enabled'])

    @unittest_run_loop
    async def test_update_camera_not_found(self):
        """Test PUT /api/cameras/{id} - camera not found"""
        updated_data = {'camera_name': 'New Name'}
        
        resp = await self.client.request(
            'PUT',
            '/api/cameras/nonexistent',
            data=json.dumps(updated_data),
            headers={'Content-Type': 'application/json'}
        )
        self.assertEqual(resp.status, 404)

    @unittest_run_loop
    async def test_delete_camera(self):
        """Test DELETE /api/cameras/{id} - delete camera"""
        resp = await self.client.request('DELETE', '/api/cameras/camera2')
        self.assertEqual(resp.status, 200)
        
        data = await resp.json()
        self.assertIn('message', data)
        self.assertEqual(data['message'], 'Camera deleted successfully')
        
        # Verify camera was removed from config
        self.assertNotIn('camera2', self.mock_config.cameras)

    @unittest_run_loop
    async def test_delete_camera_not_found(self):
        """Test DELETE /api/cameras/{id} - camera not found"""
        resp = await self.client.request('DELETE', '/api/cameras/nonexistent')
        self.assertEqual(resp.status, 404)


class TestCameraConnectivity(AioHTTPTestCase):
    """Test suite for camera connectivity testing endpoints"""

    async def get_application(self):
        """Create test application with camera routes"""
        app = web.Application()
        
        # Create mock configuration
        self.mock_config = OnPatrolConfig()
        
        # Create test camera
        test_camera = CameraConfig(
            camera_name='Test Camera',
            address='192.168.1.100',
            username='admin',
            password='password123',
            rtsp_recording_enabled=True,
            email_address='camera1@test.com'
        )
        
        self.mock_config.cameras = {
            'camera1': test_camera
        }
        
        # Add camera routes to test app
        camera_routes = create_camera_routes(self.mock_config)
        app.add_routes(camera_routes)
        
        return app

    @unittest_run_loop
    async def test_test_camera_connectivity_success(self):
        """Test POST /api/cameras/{id}/test - successful connection"""
        with patch('ConfigWebAPI.test_camera_connection') as mock_test:
            mock_test.return_value = {
                'rtsp_connected': True,
                'email_valid': True,
                'ping_successful': True,
                'response_time_ms': 45
            }
            
            resp = await self.client.request('POST', '/api/cameras/camera1/test')
            self.assertEqual(resp.status, 200)
            
            data = await resp.json()
            self.assertTrue(data['rtsp_connected'])
            self.assertTrue(data['email_valid'])
            self.assertTrue(data['ping_successful'])
            self.assertEqual(data['response_time_ms'], 45)

    @unittest_run_loop
    async def test_test_camera_connectivity_failure(self):
        """Test POST /api/cameras/{id}/test - connection failure"""
        with patch('ConfigWebAPI.test_camera_connection') as mock_test:
            mock_test.return_value = {
                'rtsp_connected': False,
                'email_valid': True,
                'ping_successful': False,
                'error': 'Connection timeout',
                'response_time_ms': None
            }
            
            resp = await self.client.request('POST', '/api/cameras/camera1/test')
            self.assertEqual(resp.status, 200)
            
            data = await resp.json()
            self.assertFalse(data['rtsp_connected'])
            self.assertTrue(data['email_valid'])
            self.assertFalse(data['ping_successful'])
            self.assertIn('error', data)

    @unittest_run_loop
    async def test_test_camera_not_found(self):
        """Test POST /api/cameras/{id}/test - camera not found"""
        resp = await self.client.request('POST', '/api/cameras/nonexistent/test')
        self.assertEqual(resp.status, 404)


class TestAPIValidation(AioHTTPTestCase):
    """Test suite for API request validation and error handling"""

    async def get_application(self):
        """Create test application with camera routes"""
        app = web.Application()
        
        # Create mock configuration with minimal required data
        self.mock_config = OnPatrolConfig()
        
        # Add camera routes to test app
        camera_routes = create_camera_routes(self.mock_config)
        app.add_routes(camera_routes)
        
        return app

    @unittest_run_loop
    async def test_invalid_json_request(self):
        """Test handling of invalid JSON in request body"""
        resp = await self.client.request(
            'POST',
            '/api/cameras',
            data='invalid json',
            headers={'Content-Type': 'application/json'}
        )
        self.assertEqual(resp.status, 400)
        
        data = await resp.json()
        self.assertIn('error', data)
        self.assertIn('json', data['error'].lower())

    @unittest_run_loop
    async def test_missing_content_type(self):
        """Test handling of missing Content-Type header"""
        resp = await self.client.request(
            'POST',
            '/api/cameras',
            data=json.dumps({'camera_name': 'Test'})
        )
        self.assertEqual(resp.status, 400)

    @unittest_run_loop
    async def test_validation_errors(self):
        """Test Pydantic validation error responses"""
        camera_data = {
            'camera_name': '',  # Empty name
            'rtsp_port': 70000,  # Invalid port
            'deepstack_min_confidence': 1.5,  # Invalid confidence
            'email_address': 'invalid-email'  # Invalid email
        }
        
        resp = await self.client.request(
            'POST',
            '/api/cameras',
            data=json.dumps(camera_data),
            headers={'Content-Type': 'application/json'}
        )
        self.assertEqual(resp.status, 400)
        
        data = await resp.json()
        self.assertIn('validation_errors', data)
        self.assertIsInstance(data['validation_errors'], list)
        self.assertGreater(len(data['validation_errors']), 0)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])