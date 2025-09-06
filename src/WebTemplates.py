#!/usr/bin/env python3
"""
Web template handling for OnPatrol web interface
Serves HTML templates and static assets
"""

import os
from aiohttp import web, hdrs
from aiohttp.web import FileResponse, Response
import mimetypes
import logging

logger = logging.getLogger('patrol_bot')

class WebTemplateHandler:
    """Handles web template serving and static assets"""
    
    def __init__(self, config=None):
        self.config = config
        self.web_root = os.path.join(os.path.dirname(__file__), 'web')
        self.templates_path = os.path.join(self.web_root, 'templates')
        self.static_path = os.path.join(self.web_root, 'static')
        
    def create_template_routes(self):
        """Create routes for web templates and static assets"""
        routes = [
            web.get('/', self.handle_index),
            web.get('/cameras', self.handle_cameras),
            web.get('/status', self.handle_status),
            web.get('/settings', self.handle_settings),
            web.get('/static/{path:.*}', self.handle_static),
        ]
        return routes
    
    async def handle_index(self, request):
        """Serve the main dashboard page"""
        return await self.serve_template('index.html')
    
    async def handle_cameras(self, request):
        """Serve the camera management page"""
        return await self.serve_template('cameras.html')
    
    async def handle_status(self, request):
        """Serve the system status page"""
        return await self.serve_template('status.html')
    
    async def handle_settings(self, request):
        """Serve the settings configuration page"""
        return await self.serve_template('settings.html')
    
    async def handle_static(self, request):
        """Serve static assets (CSS, JS, images)"""
        path = request.match_info['path']
        file_path = os.path.join(self.static_path, path)
        
        # Security check - ensure path is within static directory
        if not self.is_safe_path(file_path, self.static_path):
            raise web.HTTPNotFound()
        
        if not os.path.exists(file_path):
            raise web.HTTPNotFound()
        
        if os.path.isdir(file_path):
            raise web.HTTPNotFound()
        
        # Get MIME type
        content_type, _ = mimetypes.guess_type(file_path)
        if content_type is None:
            content_type = 'application/octet-stream'
        
        # Set caching headers for static assets
        headers = {}
        if path.startswith(('css/', 'js/', 'images/')):
            headers['Cache-Control'] = 'public, max-age=86400'  # 24 hours
        
        # Set content type in headers (FileResponse doesn't accept content_type parameter)
        headers['Content-Type'] = content_type
        
        return FileResponse(
            file_path,
            headers=headers
        )
    
    async def serve_template(self, template_name):
        """Serve an HTML template with proper headers"""
        template_path = os.path.join(self.templates_path, template_name)
        
        if not os.path.exists(template_path):
            logger.error(f"Template not found: {template_path}")
            raise web.HTTPNotFound()
        
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # You could add template processing here if needed
            # For now, serving static HTML
            
            return Response(
                text=content,
                content_type='text/html',
                headers={
                    'Cache-Control': 'no-cache',
                    'X-Content-Type-Options': 'nosniff',
                    'X-Frame-Options': 'DENY',
                    'X-XSS-Protection': '1; mode=block'
                }
            )
        except Exception as e:
            logger.error(f"Error serving template {template_name}: {e}")
            raise web.HTTPInternalServerError()
    
    def is_safe_path(self, path, base_path):
        """Check if path is within the allowed base path"""
        try:
            real_path = os.path.realpath(path)
            real_base = os.path.realpath(base_path)
            return real_path.startswith(real_base + os.sep) or real_path == real_base
        except:
            return False

def create_template_routes(config=None):
    """Create template routes for integration with WebServer"""
    handler = WebTemplateHandler(config)
    return handler.create_template_routes()