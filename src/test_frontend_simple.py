#!/usr/bin/env python3
"""
Simple frontend integration tests for OnPatrol web interface
Tests basic functionality without requiring external dependencies
"""

import asyncio
import sys
import os

def test_imports():
    """Test that all modules can be imported"""
    try:
        from WebServer import WebServer
        from ConfigWebAPI import create_camera_routes
        from StatusAPI import create_status_routes
        from WebTemplates import create_template_routes
        from config import OnPatrolConfig
        print("✓ All imports successful")
        return True
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_web_structure():
    """Test that web files exist"""
    web_root = os.path.join(os.path.dirname(__file__), 'web')
    
    required_files = [
        'templates/index.html',
        'templates/cameras.html', 
        'templates/status.html',
        'static/css/main.css',
        'static/css/components.css',
        'static/js/api.js',
        'static/js/components.js',
        'static/js/main.js'
    ]
    
    missing_files = []
    for file_path in required_files:
        full_path = os.path.join(web_root, file_path)
        if not os.path.exists(full_path):
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing web files: {missing_files}")
        return False
    else:
        print("✓ All required web files exist")
        return True

def test_route_creation():
    """Test that route creation works"""
    try:
        from WebTemplates import create_template_routes
        from ConfigWebAPI import create_camera_routes
        from StatusAPI import create_status_routes
        from config import OnPatrolConfig
        
        config = OnPatrolConfig()
        
        # Test route creation
        template_routes = create_template_routes(config)
        camera_routes = create_camera_routes(config)
        status_routes = create_status_routes(config)
        
        print(f"✓ Template routes: {len(template_routes)}")
        print(f"✓ Camera routes: {len(camera_routes)}")
        print(f"✓ Status routes: {len(status_routes)}")
        
        return True
    except Exception as e:
        print(f"❌ Route creation failed: {e}")
        return False

def test_webserver_creation():
    """Test that WebServer can be created"""
    try:
        from WebServer import WebServer
        from config import OnPatrolConfig
        
        config = OnPatrolConfig()
        server = WebServer('localhost', 8080, config)
        
        print("✓ WebServer created successfully")
        return True
    except Exception as e:
        print(f"❌ WebServer creation failed: {e}")
        return False

def test_html_templates():
    """Test that HTML templates have required elements"""
    web_root = os.path.join(os.path.dirname(__file__), 'web')
    
    templates = ['index.html', 'cameras.html', 'status.html']
    required_elements = {
        'index.html': ['OnPatrol', 'Dashboard', 'status-overview'],
        'cameras.html': ['Camera Management', 'camera-grid', 'camera-form'],
        'status.html': ['System Status', 'components-grid', 'performance-section']
    }
    
    for template in templates:
        template_path = os.path.join(web_root, 'templates', template)
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            for element in required_elements[template]:
                if element not in content:
                    print(f"❌ Missing element '{element}' in {template}")
                    return False
            
            print(f"✓ Template {template} has required elements")
            
        except Exception as e:
            print(f"❌ Error reading {template}: {e}")
            return False
    
    return True

def test_css_files():
    """Test that CSS files have basic structure"""
    web_root = os.path.join(os.path.dirname(__file__), 'web')
    
    css_files = ['main.css', 'components.css']
    required_rules = {
        'main.css': [':root', '.app-container', '.main-content'],
        'components.css': ['.card', '.btn', '.form-input']
    }
    
    for css_file in css_files:
        css_path = os.path.join(web_root, 'static', 'css', css_file)
        try:
            with open(css_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            for rule in required_rules[css_file]:
                if rule not in content:
                    print(f"❌ Missing CSS rule '{rule}' in {css_file}")
                    return False
            
            print(f"✓ CSS file {css_file} has required rules")
            
        except Exception as e:
            print(f"❌ Error reading {css_file}: {e}")
            return False
    
    return True

def test_javascript_files():
    """Test that JavaScript files have basic structure"""
    web_root = os.path.join(os.path.dirname(__file__), 'web')
    
    js_files = ['api.js', 'components.js', 'main.js']
    required_elements = {
        'api.js': ['OnPatrolAPI', 'FormValidator', 'UIHelpers'],
        'components.js': ['CameraManager', 'StatusMonitor', 'Dashboard'],
        'main.js': ['OnPatrolApp', 'DOMContentLoaded']
    }
    
    for js_file in js_files:
        js_path = os.path.join(web_root, 'static', 'js', js_file)
        try:
            with open(js_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            for element in required_elements[js_file]:
                if element not in content:
                    print(f"❌ Missing element '{element}' in {js_file}")
                    return False
            
            print(f"✓ JavaScript file {js_file} has required elements")
            
        except Exception as e:
            print(f"❌ Error reading {js_file}: {e}")
            return False
    
    return True

def run_all_tests():
    """Run all frontend tests"""
    print("Starting OnPatrol Frontend Integration Tests")
    print("=" * 50)
    
    tests = [
        ("Module Imports", test_imports),
        ("Web File Structure", test_web_structure), 
        ("Route Creation", test_route_creation),
        ("WebServer Creation", test_webserver_creation),
        ("HTML Templates", test_html_templates),
        ("CSS Files", test_css_files),
        ("JavaScript Files", test_javascript_files)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\nRunning: {test_name}")
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
    
    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} passed")
    
    if passed == total:
        print("🎉 ALL FRONTEND INTEGRATION TESTS PASSED!")
        return True
    else:
        print("❌ Some tests failed - check the output above")
        return False

if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)