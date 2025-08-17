# Technical Stack

> Last Updated: 2025-01-17
> Version: 1.0.0

## Core Technologies

### Application Framework
- **Framework:** Python Application
- **Version:** 3.7+
- **Language:** Python 3.7+

### Database
- **Primary:** SQLite
- **Version:** 3.x
- **ORM:** Custom SQLite wrapper (sqlite3worker)

## Backend Stack

### Python Framework
- **Framework:** Custom asyncio/threading application
- **Version:** Python 3.7+
- **Async Library:** asyncio with threading support

### Package Manager
- **Package Manager:** pip
- **Requirements:** requirements.txt
- **Python Version:** 3.7+

### Key Dependencies
- **AI Integration:** DeepStack SDK for object detection
- **Telegram Bot:** aiogram 2.x for notifications
- **HTTP Client:** aiohttp for web requests
- **Email Server:** aiosmtpd for SMTP functionality
- **Video Processing:** python-ffmpeg for RTSP recording
- **Computer Vision:** OpenCV for image processing

## Communication & Monitoring

### Notification Systems
- **Telegram Integration:** aiogram bot framework
- **Email Processing:** SMTP server with aiosmtpd
- **Logging:** python-telegram-logger for remote notifications

### Web Interface
- **HTTP Server:** Built-in aiohttp web server
- **Configuration:** INI-based configuration files
- **UI Framework:** Console-based menu system (consolemenu)

## External Integrations

### AI Services
- **Provider:** DeepStack
- **Service:** Object Detection API
- **Integration:** deepstack_sdk

### Camera Systems
- **Protocol:** RTSP for video streams
- **Email:** SMTP for camera notifications
- **API:** ISAPI for camera management

### Notification Channels
- **Telegram:** Bot API for group notifications
- **Email:** SMTP for alert forwarding
- **Web:** HTTP status monitoring

## Development Tools

### Build System
- **Packaging:** PyInstaller for executable creation
- **Scripts:** Batch/shell scripts for environment setup
- **Virtual Environment:** Python venv

### Configuration Management
- **Format:** INI configuration files
- **Templates:** Default configuration templates
- **Validation:** Built-in configuration validation

## Infrastructure

### Application Hosting
- **Platform:** Self-hosted on local hardware
- **Service:** Standalone Python application
- **OS Support:** Windows, Linux, macOS

### Database Hosting
- **Provider:** Local SQLite file
- **Service:** File-based database
- **Backups:** File system backups

### Asset Storage
- **Provider:** Local file system
- **Service:** Image storage in configurable directory
- **Access:** Direct file access

## Deployment

### Installation Method
- **Method:** Manual installation with setup scripts
- **Requirements:** Python 3.7+ and pip
- **Configuration:** Edit INI files before first run

### Process Management
- **Approach:** Single application with multiple threads
- **Lock File:** PID-based process locking
- **Service Mode:** Console application with menu interface

### Monitoring
- **Logging:** File-based logging with rotation
- **Status:** HTTP endpoint for service status
- **Alerts:** Telegram notifications for errors