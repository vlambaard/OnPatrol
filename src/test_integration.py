#!/usr/bin/env python3
"""
Integration test for WebServer with Camera Management API
"""

def test_webserver_integration():
    """Test WebServer can be created with camera API integration"""
    try:
        from WebServer import WebServer
        from config import OnPatrolConfig
        print('✓ WebServer integration imports successful')
        
        # Create config with camera
        config = OnPatrolConfig()
        print('✓ OnPatrolConfig created')
        
        # Test WebServer instantiation with config
        server = WebServer('localhost', 8080, config)
        print('✓ WebServer created with camera API integration')
        
        return True
    except Exception as ex:
        print(f'❌ WebServer integration test failed: {ex}')
        import traceback
        traceback.print_exc()
        return False

def test_full_system_integration():
    """Test complete system integration"""
    try:
        from WebServer import WebServer
        from ConfigWebAPI import create_camera_routes
        from config import OnPatrolConfig, CameraConfig
        
        print('✓ All imports successful')
        
        # Test full integration
        config = OnPatrolConfig()
        config.cameras['test'] = CameraConfig(camera_name='Test', address='192.168.1.1')
        print('✓ Configuration with camera created')
        
        routes = create_camera_routes(config)
        print(f'✓ Created {len(routes)} camera API routes')
        
        # Test WebServer instantiation with config
        server = WebServer('localhost', 8080, config)
        print('✓ WebServer created with camera API integration')
        
        print('✅ Full integration test successful!')
        return True
        
    except Exception as ex:
        print(f'❌ Full integration test failed: {ex}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    print("Running integration tests...")
    print("\n=== WebServer Integration Test ===")
    success1 = test_webserver_integration()
    
    print("\n=== Full System Integration Test ===")  
    success2 = test_full_system_integration()
    
    if success1 and success2:
        print("\n🎉 All integration tests passed!")
    else:
        print("\n❌ Some integration tests failed!")