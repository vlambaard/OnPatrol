# Product Mission

> Last Updated: 2025-01-17
> Version: 1.0.0

## Pitch

OnPatrol is a community-focused security camera monitoring system that helps neighborhood watches, street groups, and local communities monitor their security cameras without the cost and complexity of expensive Video Management Systems (VMS).

## Users

### Primary Customers

- **Community Groups**: Neighborhood watch organizations, resident associations, and local security committees
- **Street Groups**: Informal community security networks and local surveillance cooperatives
- **Small Organizations**: Community centers, small businesses, and local facilities needing affordable monitoring

### User Personas

**Community Security Coordinator** (30-65 years old)
- **Role:** Neighborhood Watch Leader or Community Organizer
- **Context:** Manages security for residential areas or community spaces
- **Pain Points:** Expensive commercial VMS solutions, complex setup requirements, ongoing subscription costs
- **Goals:** Affordable monitoring solution, easy configuration, reliable notifications

**Technical Volunteer** (25-50 years old)
- **Role:** IT-savvy community member or local tech volunteer
- **Context:** Helps set up and maintain community security systems
- **Pain Points:** Limited budget, need for flexible configuration, integration with existing cameras
- **Goals:** Open-source solution, customizable alerts, easy maintenance

## The Problem

### High Cost of Commercial VMS Solutions

Commercial Video Management Systems are prohibitively expensive for community groups and neighborhood watches. These systems often require expensive licensing, professional installation, and ongoing subscription fees that strain community budgets.

**Our Solution:** Provide a free, open-source alternative that runs on standard hardware.

### Complex Configuration and Setup

Existing solutions require specialized knowledge and professional setup, making them inaccessible to volunteer-run community organizations.

**Our Solution:** Simple configuration files and straightforward setup process designed for non-technical users.

### Limited Integration with Existing Equipment

Many communities already have cameras installed but lack affordable software to monitor and manage notifications effectively.

**Our Solution:** Support for standard camera protocols (RTSP) and email notifications from existing camera systems.

## Differentiators

### Community-Focused Design

Unlike enterprise VMS solutions designed for large organizations, OnPatrol is specifically built for community groups with limited budgets and volunteer-based technical support.

### AI-Enhanced Monitoring

Integration with DeepStack AI for intelligent object detection reduces false alarms and provides more relevant notifications compared to basic motion detection systems.

### Multi-Channel Notification System

Comprehensive notification system supporting email, Telegram, and web-based monitoring ensures community coordinators stay informed across multiple communication channels.

## Key Features

### Core Features

- **Email Server Integration:** Direct integration with camera email notifications for seamless monitoring
- **Multi-Camera Support:** Monitor multiple cameras across different locations within the community
- **SQLite Database:** Lightweight database for event logging and image storage
- **Web-Based Monitoring:** HTTP server for real-time status monitoring and configuration
- **Flexible Configuration:** INI-based configuration system for easy customization

### Notification Features

- **Telegram Integration:** Real-time notifications to community chat groups
- **Email Processing:** Automated parsing of camera email notifications
- **Smart Filtering:** Time-based and event-type filtering to reduce noise
- **Image Attachments:** Automatic capture and sharing of security event images

### Advanced Features

- **DeepStack AI Integration:** Intelligent object detection to minimize false alarms
- **RTSP Recording:** Direct camera stream recording for security events
- **Channel Management:** Support for multi-channel cameras and camera clusters
- **Event Logging:** Comprehensive logging and audit trail for security events