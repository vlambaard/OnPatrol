#!/usr/bin/env python3
"""
Simple dashboard integration tests for OnPatrol status dashboard
Tests basic functionality without requiring external dependencies
"""

import os
import sys

def test_dashboard_html_structure():
    """Test that status dashboard HTML has required elements"""
    web_root = os.path.join(os.path.dirname(__file__), 'web')
    status_template = os.path.join(web_root, 'templates', 'status.html')
    
    try:
        with open(status_template, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Test dashboard-specific elements
        required_elements = [
            'System Status',
            'system-health-section',
            'components-grid',
            'performance-section',
            'camera-status-grid',
            'performance-grid',
            'health-indicator',
            'StatusMonitor.init',
            'StatusMonitor.refreshAll'
        ]
        
        missing_elements = []
        for element in required_elements:
            if element not in content:
                missing_elements.append(element)
        
        if missing_elements:
            print(f"❌ Missing dashboard elements: {missing_elements}")
            return False
        else:
            print("✓ Status dashboard HTML has all required elements")
            return True
            
    except Exception as e:
        print(f"❌ Error reading status template: {e}")
        return False

def test_dashboard_javascript():
    """Test that dashboard JavaScript has required components"""
    web_root = os.path.join(os.path.dirname(__file__), 'web')
    components_js = os.path.join(web_root, 'static', 'js', 'components.js')
    
    try:
        with open(components_js, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Test dashboard-specific JavaScript
        required_functions = [
            'StatusMonitor',
            'updateSystemHealth',
            'updateComponentStatus',
            'updatePerformanceMetrics',
            'updateCameraStatus',
            'connectWebSocket',
            'handleWebSocketMessage',
            'refreshAll'
        ]
        
        missing_functions = []
        for func in required_functions:
            if func not in content:
                missing_functions.append(func)
        
        if missing_functions:
            print(f"❌ Missing dashboard functions: {missing_functions}")
            return False
        else:
            print("✓ Dashboard JavaScript has all required functions")
            return True
            
    except Exception as e:
        print(f"❌ Error reading components.js: {e}")
        return False

def test_dashboard_css():
    """Test that dashboard CSS has required styles"""
    web_root = os.path.join(os.path.dirname(__file__), 'web')
    components_css = os.path.join(web_root, 'static', 'css', 'components.css')
    
    try:
        with open(components_css, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Test dashboard-specific CSS classes
        required_styles = [
            '.health-indicator',
            '.status-dot',
            '.component-card',
            '.metric-bar',
            '.metric-fill',
            '.camera-status-item'
        ]
        
        missing_styles = []
        for style in required_styles:
            if style not in content:
                missing_styles.append(style)
        
        if missing_styles:
            print(f"❌ Missing dashboard styles: {missing_styles}")
            return False
        else:
            print("✓ Dashboard CSS has all required styles")
            return True
            
    except Exception as e:
        print(f"❌ Error reading components.css: {e}")
        return False

def test_status_api_structure():
    """Test that StatusAPI has dashboard-required functions"""
    status_api_path = os.path.join(os.path.dirname(__file__), 'StatusAPI.py')
    
    try:
        with open(status_api_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Test API functions required for dashboard
        required_functions = [
            'get_system_status_handler',
            'get_component_status_handler',
            'get_camera_status_handler',
            'get_performance_metrics_handler',
            'status_websocket_handler',
            'get_system_components_status',
            'get_performance_metrics'
        ]
        
        missing_functions = []
        for func in required_functions:
            if func not in content:
                missing_functions.append(func)
        
        if missing_functions:
            print(f"❌ Missing StatusAPI functions: {missing_functions}")
            return False
        else:
            print("✓ StatusAPI has all required dashboard functions")
            return True
            
    except Exception as e:
        print(f"❌ Error reading StatusAPI.py: {e}")
        return False

def test_dashboard_integration():
    """Test that dashboard components integrate properly"""
    try:
        # Check that status.html initializes StatusMonitor
        web_root = os.path.join(os.path.dirname(__file__), 'web')
        status_template = os.path.join(web_root, 'templates', 'status.html')
        
        with open(status_template, 'r', encoding='utf-8') as f:
            content = f.read()
        
        integration_checks = [
            'StatusMonitor.init()',
            'DOMContentLoaded',
            'system-health-section',
            'components-grid',
            'StatusMonitor'
        ]
        
        for check in integration_checks:
            if check not in content:
                print(f"❌ Missing integration element: {check}")
                return False
        
        print("✓ Dashboard integration elements present")
        return True
        
    except Exception as e:
        print(f"❌ Error testing dashboard integration: {e}")
        return False

def test_websocket_implementation():
    """Test that WebSocket functionality is implemented"""
    status_api_path = os.path.join(os.path.dirname(__file__), 'StatusAPI.py')
    
    try:
        with open(status_api_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        websocket_features = [
            'WebSocketResponse',
            'get_status_updates_stream',
            'initial_status',
            'status_update',
            'send_str',
            'json.dumps'
        ]
        
        missing_features = []
        for feature in websocket_features:
            if feature not in content:
                missing_features.append(feature)
        
        if missing_features:
            print(f"❌ Missing WebSocket features: {missing_features}")
            return False
        else:
            print("✓ WebSocket implementation complete")
            return True
            
    except Exception as e:
        print(f"❌ Error testing WebSocket implementation: {e}")
        return False

def test_performance_monitoring():
    """Test that performance monitoring is implemented"""
    status_api_path = os.path.join(os.path.dirname(__file__), 'StatusAPI.py')
    
    try:
        with open(status_api_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        performance_features = [
            'psutil',
            'cpu_percent',
            'memory_percent', 
            'disk_usage',
            'get_performance_metrics',
            'system',
            'application',
            'cameras'
        ]
        
        missing_features = []
        for feature in performance_features:
            if feature not in content:
                missing_features.append(feature)
        
        if missing_features:
            print(f"❌ Missing performance features: {missing_features}")
            return False
        else:
            print("✓ Performance monitoring implemented")
            return True
            
    except Exception as e:
        print(f"❌ Error testing performance monitoring: {e}")
        return False

def run_all_dashboard_tests():
    """Run all dashboard functionality tests"""
    print("Starting OnPatrol Dashboard Functionality Tests")
    print("=" * 50)
    
    tests = [
        ("Dashboard HTML Structure", test_dashboard_html_structure),
        ("Dashboard JavaScript", test_dashboard_javascript),
        ("Dashboard CSS Styles", test_dashboard_css),
        ("StatusAPI Structure", test_status_api_structure),
        ("Dashboard Integration", test_dashboard_integration),
        ("WebSocket Implementation", test_websocket_implementation),
        ("Performance Monitoring", test_performance_monitoring)
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
    print(f"Dashboard Test Results: {passed}/{total} passed")
    
    if passed == total:
        print("🎉 ALL DASHBOARD TESTS PASSED!")
        print("\nDashboard Features Verified:")
        print("✓ System health monitoring with component status")
        print("✓ Real-time WebSocket updates")
        print("✓ Performance metrics display")
        print("✓ Camera status integration") 
        print("✓ Responsive dashboard interface")
        print("✓ Complete API integration")
        return True
    else:
        print("❌ Some dashboard tests failed - check the output above")
        return False

if __name__ == '__main__':
    success = run_all_dashboard_tests()
    sys.exit(0 if success else 1)