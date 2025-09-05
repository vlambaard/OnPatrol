# API Specification

This is the API specification for the spec detailed in @.agent-os/specs/2025-09-05-web-camera-config/spec.md

> Created: 2025-09-05
> Version: 1.0.0

## Endpoints

### GET /api/cameras

**Purpose:** Retrieve list of all configured cameras with status information
**Parameters:** 
- `status` (optional): Filter by camera status (online, offline, error)
- `cluster` (optional): Filter by camera cluster name
**Response:** 
```json
{
  "cameras": [
    {
      "id": "camera_001",
      "camera_name": "Front Gate Camera",
      "description": "Main entrance monitoring",
      "status": "online",
      "last_event": "2025-09-05T10:30:00Z",
      "email_address": "camera001@localhost.local",
      "address": "192.168.1.100",
      "rtsp_enabled": true,
      "deepstack_enabled": true,
      "telegram_notifications": true
    }
  ],
  "total": 5,
  "online": 4,
  "offline": 1
}
```
**Errors:** 
- 500: Internal server error reading configuration

### POST /api/cameras

**Purpose:** Create new camera configuration
**Parameters:** Camera configuration object in request body
**Response:** 
```json
{
  "success": true,
  "camera_id": "camera_006",
  "message": "Camera configuration created successfully"
}
```
**Errors:** 
- 400: Invalid configuration data, validation errors
- 409: Camera with same email address already exists
- 500: Internal server error saving configuration

### GET /api/cameras/{camera_id}

**Purpose:** Retrieve detailed configuration for specific camera
**Parameters:** 
- `camera_id` (path): Unique camera identifier
**Response:** 
```json
{
  "camera_enabled": true,
  "camera_name": "Front Gate Camera",
  "description": "Main entrance monitoring",
  "latitude": "40.7128",
  "longitude": "-74.0060",
  "email_address": "camera001@localhost.local",
  "email_template": "HIKVISION_DEFAULT",
  "address": "192.168.1.100",
  "username": "admin",
  "rtsp_recording_enabled": true,
  "rtsp_recording_length_sec": 10,
  "deepstack_detection_enabled": true,
  "deepstack_min_confidence": 0.43,
  "isapi_enabled": false
}
```
**Errors:** 
- 404: Camera not found
- 500: Internal server error reading configuration

### PUT /api/cameras/{camera_id}

**Purpose:** Update existing camera configuration
**Parameters:** 
- `camera_id` (path): Unique camera identifier
- Request body: Partial or complete camera configuration object
**Response:** 
```json
{
  "success": true,
  "message": "Camera configuration updated successfully"
}
```
**Errors:** 
- 400: Invalid configuration data, validation errors
- 404: Camera not found
- 500: Internal server error saving configuration

### DELETE /api/cameras/{camera_id}

**Purpose:** Remove camera configuration
**Parameters:** 
- `camera_id` (path): Unique camera identifier
**Response:** 
```json
{
  "success": true,
  "message": "Camera configuration deleted successfully"
}
```
**Errors:** 
- 404: Camera not found
- 500: Internal server error removing configuration

### POST /api/cameras/{camera_id}/test

**Purpose:** Test camera connectivity and configuration
**Parameters:** 
- `camera_id` (path): Unique camera identifier
- `test_type` (body): Type of test - "rtsp", "email", "isapi", or "all"
**Response:** 
```json
{
  "rtsp_test": {
    "status": "success",
    "response_time_ms": 250,
    "stream_available": true
  },
  "email_test": {
    "status": "success", 
    "template_valid": true,
    "last_email": "2025-09-05T09:15:00Z"
  },
  "isapi_test": {
    "status": "disabled",
    "message": "ISAPI not enabled for this camera"
  }
}
```
**Errors:** 
- 404: Camera not found
- 500: Test execution failed

### GET /api/status

**Purpose:** Retrieve overall system status and health metrics
**Parameters:** None
**Response:** 
```json
{
  "server": {
    "status": "running",
    "uptime_seconds": 86400,
    "version": "1.4.0",
    "process_id": "OnPatrol_001"
  },
  "email_server": {
    "status": "running",
    "port": 25,
    "messages_processed": 156,
    "last_message": "2025-09-05T10:25:00Z"
  },
  "telegram_bot": {
    "status": "connected",
    "bot_username": "@SecurityBot",
    "active_chats": 3,
    "messages_sent": 45
  },
  "deepstack": {
    "status": "connected",
    "url": "http://localhost:5000",
    "response_time_ms": 150,
    "detections_today": 23
  },
  "database": {
    "status": "healthy",
    "file_size_mb": 15.6,
    "events_today": 67,
    "last_cleanup": "2025-09-05T06:00:00Z"
  }
}
```
**Errors:** 
- 500: Internal server error gathering status

### GET /api/configuration

**Purpose:** Retrieve current system configuration (non-sensitive data)
**Parameters:** None
**Response:** 
```json
{
  "server": {
    "server_id": "OnPatrol",
    "http_port": 8080,
    "images_save_path": "./images"
  },
  "email_templates": ["HIKVISION_DEFAULT", "DAHUA_DEFAULT", "CUSTOM_001"],
  "camera_clusters": ["front_yard", "back_yard", "garage"],
  "notification_groups": [
    {
      "name": "Security Team",
      "chat_id": "-100123456789",
      "enabled": true
    }
  ]
}
```
**Errors:** 
- 500: Internal server error reading configuration

### POST /api/configuration

**Purpose:** Update system-wide configuration settings
**Parameters:** Configuration update object in request body
**Response:** 
```json
{
  "success": true,
  "message": "Configuration updated successfully",
  "restart_required": false
}
```
**Errors:** 
- 400: Invalid configuration data, validation errors
- 500: Internal server error saving configuration

### WebSocket /api/ws

**Purpose:** Real-time status updates and event streaming
**Events:** 
- `camera_status`: Camera online/offline status changes
- `new_event`: Security events from cameras
- `system_alert`: Server errors or warnings
- `heartbeat`: Connection health monitoring

## Controllers and Business Logic

### CameraController
- **camera_list()**: Query configuration system for all cameras, enrich with status
- **camera_create()**: Validate using Pydantic CameraConfig, save to INI
- **camera_read()**: Load specific camera configuration from config system
- **camera_update()**: Merge changes with existing config, validate, save
- **camera_delete()**: Remove from configuration, clean up references
- **camera_test()**: Execute connectivity tests based on camera configuration

### StatusController  
- **system_status()**: Aggregate health information from all server components
- **component_status()**: Query individual service status (email, telegram, deepstack)
- **metrics_summary()**: Database queries for recent activity and performance data

### ConfigurationController
- **config_read()**: Return non-sensitive configuration data for UI population
- **config_update()**: Validate and update system-wide settings
- **template_list()**: Return available email templates and camera clusters

### WebSocketController
- **handle_connection()**: Authenticate and register client for updates
- **broadcast_event()**: Send real-time updates to all connected clients
- **heartbeat_monitor()**: Track client connection health

## Error Handling

### Standard Error Response Format
```json
{
  "error": true,
  "message": "Human readable error message",
  "code": "VALIDATION_ERROR",
  "details": {
    "field": "email_address",
    "constraint": "Invalid email format"
  }
}
```

### Error Categories
- **Validation Errors (400)**: Pydantic validation failures with field-specific details
- **Not Found Errors (404)**: Camera or configuration entity not found
- **Conflict Errors (409)**: Duplicate camera configurations or constraint violations
- **Server Errors (500)**: File system errors, service unavailability, database issues

### Integration with Existing Error Handling
- Leverage existing logging infrastructure for API request/response logging
- Use existing Telegram notification system for critical API errors
- Integrate with current configuration validation system from Pydantic models