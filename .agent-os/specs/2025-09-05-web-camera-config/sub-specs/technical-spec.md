# Technical Specification

This is the technical specification for the spec detailed in @.agent-os/specs/2025-09-05-web-camera-config/spec.md

> Created: 2025-09-05
> Version: 1.0.0

## Technical Requirements

- **Backend Integration**: Extend existing WebServer.py with aiohttp routes for RESTful API endpoints
- **Configuration System**: Integrate with existing Pydantic configuration classes (CameraConfig, ServerConfig, etc.)
- **Real-time Updates**: Implement WebSocket or Server-Sent Events for live status monitoring
- **Form Validation**: Client-side validation with server-side Pydantic validation for data integrity
- **Mobile Responsive**: CSS Grid/Flexbox layout optimized for mobile devices (min 320px width)
- **Browser Compatibility**: Support for modern browsers (Chrome 90+, Firefox 88+, Safari 14+, Edge 90+)
- **Performance**: Page load times under 2 seconds on typical community hardware
- **Security**: Input sanitization, CSRF protection, and secure configuration handling

## Approach Options

**Option A: Full JavaScript Framework (React/Vue)**
- Pros: Rich UI components, modern development experience, extensive ecosystem
- Cons: Complex build process, large bundle size, requires Node.js knowledge for maintenance

**Option B: Vanilla HTML/CSS/JavaScript** (Selected)
- Pros: No build dependencies, easy maintenance for volunteers, lightweight, aligns with community-focused philosophy
- Cons: More manual DOM manipulation, fewer pre-built components

**Option C: Lightweight Framework (Alpine.js/Petite-Vue)**
- Pros: Small footprint, familiar syntax, minimal build requirements
- Cons: Additional dependency, learning curve for volunteer maintainers

**Rationale:** Option B (Vanilla JavaScript) selected to align with OnPatrol's mission of volunteer maintainability and simplicity. The existing codebase already uses this approach with Rich console library replacement, indicating a preference for minimal dependencies and straightforward implementations.

## Architecture Design

### Frontend Structure
```
src/web/
├── static/
│   ├── css/
│   │   ├── main.css          # Core styles with CSS Grid/Flexbox
│   │   └── components.css    # Reusable component styles
│   ├── js/
│   │   ├── main.js          # Core application logic
│   │   ├── api.js           # API communication layer
│   │   ├── components.js    # Reusable UI components
│   │   └── validation.js    # Form validation logic
│   └── images/
│       └── favicon.ico
└── templates/
    ├── index.html           # Main dashboard page
    ├── cameras.html         # Camera management page
    ├── status.html          # System status page
    └── partials/
        ├── camera-form.html # Camera configuration form
        └── status-card.html # Status monitoring components
```

### Backend Extensions
- Extend `WebServer.py` with additional aiohttp routes
- Create new `ConfigWebAPI.py` module for camera configuration endpoints
- Add `StatusAPI.py` for system monitoring endpoints
- Integrate with existing configuration management in `config.py`

### Data Flow
1. Web interface sends AJAX requests to REST API endpoints
2. API validates requests using existing Pydantic models
3. Configuration changes update INI files through existing config system
4. Real-time updates broadcast via WebSocket to connected clients
5. Status monitoring queries existing server components directly

## UI/UX Specifications

### Design System
- **Color Scheme**: Professional blue (#2563eb) primary, gray (#6b7280) secondary, red (#dc2626) alerts, green (#16a34a) success
- **Typography**: System fonts (San Francisco, Segoe UI, Ubuntu, sans-serif) for consistency across platforms
- **Spacing**: 8px base unit with consistent 8px, 16px, 24px, 32px spacing scale
- **Icons**: Inline SVG icons for better performance and customization

### Component Library
- **Card Components**: Consistent card layout for status information and configuration sections
- **Form Components**: Standardized input fields, dropdowns, checkboxes with validation states
- **Button Components**: Primary, secondary, and danger button styles with consistent sizing
- **Table Components**: Sortable, filterable tables for camera lists and event history
- **Modal Components**: Overlay dialogs for confirmations and detailed forms

### Responsive Design
- **Mobile First**: Base styles optimized for 320px minimum width
- **Tablet**: 768px breakpoint with adjusted layouts and larger touch targets
- **Desktop**: 1024px breakpoint with multi-column layouts and hover states
- **Touch Friendly**: Minimum 44px touch targets on mobile devices

## Integration Points

### Existing Configuration System
- **CameraConfig Integration**: Directly map form fields to Pydantic model properties
- **Validation Reuse**: Leverage existing Pydantic validators for server-side validation
- **INI File Management**: Use existing configuration save/load mechanisms

### Server Component Monitoring
- **Email Server Status**: Query SMTP server thread status and recent activity
- **Telegram Bot Status**: Check bot connectivity and recent message statistics  
- **DeepStack Integration**: Monitor AI server connectivity and detection performance
- **SQLite Database**: Query event logs and system metrics for dashboard display

### Real-time Communication
- **WebSocket Events**: Camera status changes, new security events, system alerts
- **Heartbeat Monitoring**: Regular ping/pong for connection health monitoring
- **Event Broadcasting**: Distribute status updates to all connected web clients

## External Dependencies

- **No New Major Dependencies**: Leverage existing aiohttp web server framework
- **Static Assets**: Self-hosted CSS and JavaScript files (no CDN dependencies)
- **Icon Library**: Inline SVG icons (no external icon fonts or libraries)

**Justification:** Maintaining the existing minimal dependency philosophy ensures the system remains lightweight, offline-capable, and volunteer-maintainable without requiring additional package management or external service dependencies.