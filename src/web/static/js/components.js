/**
 * OnPatrol UI Components
 * Reusable UI components and managers for the application
 */

/**
 * Camera Management Component
 */
class CameraManager {
    constructor() {
        this.validator = new CameraFormValidator();
        this.editingCameraId = null;
        this.cameras = new Map();
        this.websocket = null;
    }

    static init() {
        if (!window.cameraManager) {
            window.cameraManager = new CameraManager();
        }
        return window.cameraManager.initialize();
    }

    async initialize() {
        try {
            await this.loadCameras();
            this.setupEventListeners();
            this.connectWebSocket();
            console.log('Camera manager initialized');
        } catch (error) {
            console.error('Failed to initialize camera manager:', error);
            UIHelpers.showToast('Failed to load camera data', 'error');
        }
    }

    setupEventListeners() {
        // Form submission
        const form = document.getElementById('camera-form');
        if (form) {
            form.addEventListener('submit', (e) => {
                e.preventDefault();
                this.handleFormSubmit();
            });
        }

        // Real-time validation
        const inputs = form?.querySelectorAll('input, select, textarea');
        inputs?.forEach(input => {
            input.addEventListener('blur', () => this.validateField(input.name, input.value));
            input.addEventListener('input', UIHelpers.debounce(() => this.clearFieldError(input.name), 300));
        });
    }

    async loadCameras() {
        try {
            UIHelpers.showLoading('Loading cameras...');
            const response = await api.getCameras();
            
            this.cameras.clear();
            Object.entries(response.cameras || {}).forEach(([id, config]) => {
                this.cameras.set(id, config);
            });

            this.renderCameraGrid();
            this.updateCameraCount();
            
        } catch (error) {
            console.error('Error loading cameras:', error);
            UIHelpers.showToast('Failed to load cameras', 'error');
        } finally {
            UIHelpers.hideLoading();
        }
    }

    renderCameraGrid() {
        const grid = document.getElementById('camera-grid');
        const emptyState = document.getElementById('empty-state');
        
        if (!grid) return;

        if (this.cameras.size === 0) {
            grid.style.display = 'none';
            if (emptyState) emptyState.style.display = 'block';
            return;
        }

        grid.style.display = 'grid';
        if (emptyState) emptyState.style.display = 'none';

        grid.innerHTML = '';
        
        for (const [cameraId, camera] of this.cameras) {
            const cameraCard = this.createCameraCard(cameraId, camera);
            grid.appendChild(cameraCard);
        }
    }

    createCameraCard(cameraId, camera) {
        const card = document.createElement('div');
        card.className = 'camera-card';
        card.innerHTML = `
            <div class="camera-card-header">
                <h4 class="camera-name">${this.escapeHtml(camera.camera_name || 'Unnamed Camera')}</h4>
                <div class="camera-status">
                    <span class="status-dot unknown" id="status-${cameraId}"></span>
                </div>
            </div>
            <div class="camera-card-body">
                <div class="camera-info">
                    <div class="info-item">
                        <span class="info-label">Address:</span>
                        <span class="info-value">${this.escapeHtml(camera.address || 'Not configured')}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Status:</span>
                        <span class="info-value" id="status-text-${cameraId}">Checking...</span>
                    </div>
                    ${camera.email_from ? `
                    <div class="info-item">
                        <span class="info-label">Email:</span>
                        <span class="info-value">${this.escapeHtml(camera.email_from)}</span>
                    </div>
                    ` : ''}
                </div>
            </div>
            <div class="camera-card-actions">
                <button class="btn btn-small btn-secondary" onclick="CameraManager.testConnection('${cameraId}')">
                    Test
                </button>
                <button class="btn btn-small btn-primary" onclick="CameraManager.editCamera('${cameraId}')">
                    Edit
                </button>
                <button class="btn btn-small btn-danger" onclick="CameraManager.deleteCamera('${cameraId}')">
                    Delete
                </button>
            </div>
        `;
        
        // Start checking camera status
        this.checkCameraStatus(cameraId);
        
        return card;
    }

    async checkCameraStatus(cameraId) {
        try {
            const response = await api.testCamera(cameraId);
            this.updateCameraStatusDisplay(cameraId, response.connectivity);
        } catch (error) {
            this.updateCameraStatusDisplay(cameraId, { status: 'error', error: error.message });
        }
    }

    updateCameraStatusDisplay(cameraId, status) {
        const statusDot = document.getElementById(`status-${cameraId}`);
        const statusText = document.getElementById(`status-text-${cameraId}`);
        
        if (!statusDot || !statusText) return;

        statusDot.className = 'status-dot';
        
        switch (status.status) {
            case 'online':
                statusDot.classList.add('online');
                statusText.textContent = `Online (${status.ping_response_ms}ms)`;
                break;
            case 'offline':
                statusDot.classList.add('offline');
                statusText.textContent = 'Offline';
                break;
            case 'error':
                statusDot.classList.add('error');
                statusText.textContent = 'Error';
                break;
            default:
                statusDot.classList.add('unknown');
                statusText.textContent = 'Unknown';
        }
    }

    updateCameraCount() {
        const countElement = document.getElementById('camera-count');
        if (countElement) {
            const count = this.cameras.size;
            countElement.textContent = `${count} camera${count !== 1 ? 's' : ''}`;
        }
    }

    static showAddForm() {
        return window.cameraManager?.showAddForm();
    }

    showAddForm() {
        this.editingCameraId = null;
        this.resetForm();
        document.getElementById('modal-title').textContent = 'Add New Camera';
        document.getElementById('camera-modal').style.display = 'flex';
        document.getElementById('camera_name').focus();
    }

    static editCamera(cameraId) {
        return window.cameraManager?.editCamera(cameraId);
    }

    editCamera(cameraId) {
        const camera = this.cameras.get(cameraId);
        if (!camera) return;

        this.editingCameraId = cameraId;
        this.populateForm(camera);
        document.getElementById('modal-title').textContent = 'Edit Camera';
        document.getElementById('camera-modal').style.display = 'flex';
        document.getElementById('camera_name').focus();
    }

    static deleteCamera(cameraId) {
        return window.cameraManager?.showDeleteConfirmation(cameraId);
    }

    showDeleteConfirmation(cameraId) {
        const camera = this.cameras.get(cameraId);
        if (!camera) return;

        this.deletingCameraId = cameraId;
        document.getElementById('delete-camera-name').textContent = camera.camera_name || 'Unnamed Camera';
        document.getElementById('delete-modal').style.display = 'flex';
    }

    static hideDeleteModal() {
        return window.cameraManager?.hideDeleteModal();
    }

    hideDeleteModal() {
        document.getElementById('delete-modal').style.display = 'none';
        this.deletingCameraId = null;
    }

    static confirmDelete() {
        return window.cameraManager?.performDelete();
    }

    async performDelete() {
        if (!this.deletingCameraId) return;

        try {
            UIHelpers.showLoading('Deleting camera...');
            await api.deleteCamera(this.deletingCameraId);
            
            this.cameras.delete(this.deletingCameraId);
            this.renderCameraGrid();
            this.updateCameraCount();
            this.hideDeleteModal();
            
            UIHelpers.showToast('Camera deleted successfully', 'success');
            
        } catch (error) {
            console.error('Error deleting camera:', error);
            UIHelpers.showToast(error.message || 'Failed to delete camera', 'error');
        } finally {
            UIHelpers.hideLoading();
        }
    }

    static hideForm() {
        return window.cameraManager?.hideForm();
    }

    hideForm() {
        document.getElementById('camera-modal').style.display = 'none';
        this.resetForm();
        this.editingCameraId = null;
    }

    resetForm() {
        const form = document.getElementById('camera-form');
        if (form) form.reset();
        this.clearAllErrors();
    }

    populateForm(camera) {
        const form = document.getElementById('camera-form');
        if (!form) return;

        Object.entries(camera).forEach(([key, value]) => {
            const input = form.querySelector(`[name="${key}"]`);
            if (input) {
                if (input.type === 'checkbox') {
                    input.checked = Boolean(value);
                } else {
                    input.value = value || '';
                }
            }
        });
    }

    async handleFormSubmit() {
        const form = document.getElementById('camera-form');
        const formData = new FormData(form);
        
        // Convert FormData to object
        const cameraData = {};
        for (const [key, value] of formData.entries()) {
            if (key === 'enabled') {
                cameraData[key] = Boolean(value);
            } else if (value.trim() !== '') {
                cameraData[key] = value.trim();
            }
        }

        // Validate form
        if (!this.validator.validate(cameraData)) {
            this.displayValidationErrors();
            return;
        }

        try {
            UIHelpers.showLoading(this.editingCameraId ? 'Updating camera...' : 'Creating camera...');
            
            let cameraId;
            if (this.editingCameraId) {
                cameraId = this.editingCameraId;
                await api.updateCamera(cameraId, cameraData);
                UIHelpers.showToast('Camera updated successfully', 'success');
            } else {
                // Generate camera ID from name
                cameraId = this.generateCameraId(cameraData.camera_name);
                await api.createCamera(cameraId, cameraData);
                UIHelpers.showToast('Camera created successfully', 'success');
            }

            // Update local cache
            this.cameras.set(cameraId, cameraData);
            this.renderCameraGrid();
            this.updateCameraCount();
            this.hideForm();
            
        } catch (error) {
            console.error('Error saving camera:', error);
            if (error.isValidationError()) {
                this.displayServerValidationErrors(error.data);
            } else {
                UIHelpers.showToast(error.message || 'Failed to save camera', 'error');
            }
        } finally {
            UIHelpers.hideLoading();
        }
    }

    generateCameraId(name) {
        const base = name.toLowerCase().replace(/[^a-z0-9]/g, '_').substring(0, 20);
        let id = base;
        let counter = 1;
        
        while (this.cameras.has(id)) {
            id = `${base}_${counter}`;
            counter++;
        }
        
        return id;
    }

    validateField(fieldName, value) {
        const fieldData = { [fieldName]: value };
        const rules = this.validator.rules.get(fieldName);
        
        if (rules) {
            for (const rule of rules) {
                if (!rule.validator(value, fieldData)) {
                    this.displayFieldError(fieldName, rule.errorMessage);
                    return false;
                }
            }
        }
        
        this.clearFieldError(fieldName);
        return true;
    }

    displayValidationErrors() {
        for (const [fieldName, message] of this.validator.errors) {
            this.displayFieldError(fieldName, message);
        }
    }

    displayServerValidationErrors(errorData) {
        if (errorData && errorData.validation_errors) {
            Object.entries(errorData.validation_errors).forEach(([field, messages]) => {
                this.displayFieldError(field, Array.isArray(messages) ? messages[0] : messages);
            });
        }
    }

    displayFieldError(fieldName, message) {
        const errorElement = document.getElementById(`${fieldName}-error`);
        const inputElement = document.querySelector(`[name="${fieldName}"]`);
        
        if (errorElement) {
            errorElement.textContent = message;
            errorElement.style.display = 'block';
        }
        
        if (inputElement) {
            inputElement.classList.add('error');
        }
    }

    clearFieldError(fieldName) {
        const errorElement = document.getElementById(`${fieldName}-error`);
        const inputElement = document.querySelector(`[name="${fieldName}"]`);
        
        if (errorElement) {
            errorElement.style.display = 'none';
        }
        
        if (inputElement) {
            inputElement.classList.remove('error');
        }
    }

    clearAllErrors() {
        const errorElements = document.querySelectorAll('.form-error');
        const inputElements = document.querySelectorAll('.form-input');
        
        errorElements.forEach(el => el.style.display = 'none');
        inputElements.forEach(el => el.classList.remove('error'));
        
        this.validator.clearErrors();
    }

    static testConnection(cameraId = null) {
        return window.cameraManager?.testCameraConnection(cameraId);
    }

    async testCameraConnection(cameraId = null) {
        try {
            let testId = cameraId;
            
            if (!testId) {
                // Test connection for form data
                const form = document.getElementById('camera-form');
                const formData = new FormData(form);
                
                if (!formData.get('address')) {
                    UIHelpers.showToast('Please enter an IP address first', 'warning');
                    return;
                }
                
                testId = 'test';
            }
            
            UIHelpers.showLoading('Testing connection...');
            const response = await api.testCamera(testId);
            
            if (response.connectivity.status === 'online') {
                UIHelpers.showToast(`Camera is online (${response.connectivity.ping_response_ms}ms)`, 'success');
            } else {
                UIHelpers.showToast('Camera is offline or unreachable', 'warning');
            }
            
        } catch (error) {
            console.error('Connection test failed:', error);
            UIHelpers.showToast(error.message || 'Connection test failed', 'error');
        } finally {
            UIHelpers.hideLoading();
        }
    }

    static testAllCameras() {
        return window.cameraManager?.testAllConnections();
    }

    async testAllConnections() {
        const cameraIds = Array.from(this.cameras.keys());
        
        if (cameraIds.length === 0) {
            UIHelpers.showToast('No cameras to test', 'info');
            return;
        }
        
        UIHelpers.showLoading('Testing all cameras...');
        
        const results = await Promise.allSettled(
            cameraIds.map(id => api.testCamera(id))
        );
        
        let online = 0;
        let offline = 0;
        
        results.forEach((result, index) => {
            const cameraId = cameraIds[index];
            if (result.status === 'fulfilled') {
                this.updateCameraStatusDisplay(cameraId, result.value.connectivity);
                if (result.value.connectivity.status === 'online') online++;
                else offline++;
            } else {
                this.updateCameraStatusDisplay(cameraId, { status: 'error' });
                offline++;
            }
        });
        
        UIHelpers.hideLoading();
        UIHelpers.showToast(`Test complete: ${online} online, ${offline} offline`, 'info');
    }

    connectWebSocket() {
        this.websocket = api.connectWebSocket(
            (data) => this.handleWebSocketMessage(data),
            (error) => console.warn('WebSocket connection failed:', error)
        );
    }

    handleWebSocketMessage(data) {
        if (data.type === 'camera_status_update' && data.camera_id) {
            this.updateCameraStatusDisplay(data.camera_id, data.status);
        }
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

/**
 * Status Monitor Component  
 */
class StatusMonitor {
    constructor() {
        this.refreshInterval = null;
        this.websocket = null;
    }

    static init() {
        if (!window.statusMonitor) {
            window.statusMonitor = new StatusMonitor();
        }
        return window.statusMonitor.initialize();
    }

    async initialize() {
        try {
            await this.loadAllStatus();
            this.setupRefreshInterval();
            this.connectWebSocket();
            console.log('Status monitor initialized');
        } catch (error) {
            console.error('Failed to initialize status monitor:', error);
            UIHelpers.showToast('Failed to load status data', 'error');
        }
    }

    async loadAllStatus() {
        UIHelpers.showLoading('Loading status...');
        
        try {
            const [systemStatus, componentStatus, cameraStatus, metrics] = await Promise.all([
                api.getSystemStatus(),
                api.getComponentStatus(),
                api.getCameraStatus(),
                api.getPerformanceMetrics()
            ]);
            
            this.updateSystemHealth(systemStatus);
            this.updateComponentStatus(componentStatus);
            this.updateCameraStatus(cameraStatus);
            this.updatePerformanceMetrics(metrics);
            
        } catch (error) {
            console.error('Error loading status:', error);
            UIHelpers.showToast('Failed to load status data', 'error');
        } finally {
            UIHelpers.hideLoading();
        }
    }

    updateSystemHealth(data) {
        const healthText = document.getElementById('health-status-text');
        const healthDot = document.getElementById('health-dot');
        const uptime = document.getElementById('system-uptime');
        const lastUpdated = document.getElementById('last-updated');
        const componentSummary = document.getElementById('component-summary');

        if (healthText && data.system) {
            healthText.textContent = data.system.status || 'Unknown';
        }

        if (healthDot && data.system) {
            healthDot.className = `health-dot ${data.system.status || 'unknown'}`;
        }

        if (uptime && data.system && data.system.uptime_seconds) {
            uptime.textContent = UIHelpers.formatDuration(data.system.uptime_seconds);
        }

        if (lastUpdated) {
            lastUpdated.textContent = new Date().toLocaleTimeString();
        }

        if (componentSummary && data.components) {
            const total = Object.keys(data.components).length;
            const healthy = Object.values(data.components).filter(c => c.status === 'running').length;
            componentSummary.textContent = `${healthy} / ${total}`;
        }
    }

    updateComponentStatus(data) {
        if (!data.components) return;

        Object.entries(data.components).forEach(([componentName, status]) => {
            this.updateComponentCard(componentName, status);
        });
    }

    updateComponentCard(componentName, status) {
        const statusElement = document.getElementById(`${componentName}-status`);
        const indicatorElement = document.getElementById(`${componentName}-status-indicator`);

        if (statusElement) {
            statusElement.textContent = status.status || 'Unknown';
        }

        if (indicatorElement) {
            const dot = indicatorElement.querySelector('.status-dot');
            if (dot) {
                dot.className = `status-dot ${status.status === 'running' ? 'online' : 'offline'}`;
            }
        }

        // Update component-specific details
        this.updateComponentDetails(componentName, status);
    }

    updateComponentDetails(componentName, status) {
        switch (componentName) {
            case 'webserver':
                this.updateElement('webserver-port', status.port);
                this.updateElement('webserver-connections', status.active_connections || 0);
                break;
            case 'smtp_server':
                this.updateElement('smtp-port', status.port);
                this.updateElement('smtp-messages', status.messages_today || 0);
                break;
            case 'telegram_notifier':
                this.updateElement('telegram-connected', status.bot_connected ? 'Yes' : 'No');
                this.updateElement('telegram-messages', status.messages_sent || 0);
                break;
        }
    }

    updateCameraStatus(data) {
        if (!data.cameras) return;

        const grid = document.getElementById('camera-status-grid');
        if (grid) {
            grid.innerHTML = '';
            
            Object.entries(data.cameras).forEach(([cameraId, camera]) => {
                const statusItem = this.createCameraStatusItem(cameraId, camera);
                grid.appendChild(statusItem);
            });
        }

        // Update camera summary
        if (data.summary) {
            this.updateElement('cameras-total', data.summary.total_cameras || 0);
            this.updateElement('cameras-online', data.summary.online_cameras || 0);
            this.updateElement('cameras-offline', data.summary.offline_cameras || 0);
            this.updateElement('avg-response-time', 
                data.summary.average_response_time ? `${data.summary.average_response_time}ms` : '--'
            );
        }
    }

    createCameraStatusItem(cameraId, camera) {
        const item = document.createElement('div');
        item.className = 'camera-status-item';
        item.innerHTML = `
            <div class="camera-status-header">
                <span class="camera-status-name">${this.escapeHtml(camera.name || cameraId)}</span>
                <span class="status-dot ${camera.status === 'online' ? 'online' : 'offline'}"></span>
            </div>
            <div class="camera-status-details">
                <span class="camera-address">${this.escapeHtml(camera.address || 'Unknown')}</span>
                ${camera.ping_response_ms ? `<span class="camera-ping">${camera.ping_response_ms}ms</span>` : ''}
            </div>
        `;
        return item;
    }

    updatePerformanceMetrics(data) {
        if (data.system) {
            this.updateMetricBar('cpu-usage', data.system.cpu_percent);
            this.updateMetricBar('memory-usage', data.system.memory_percent);
            this.updateMetricBar('disk-usage', data.system.disk_percent);
        }

        if (data.application) {
            this.updateElement('events-processed', data.application.events_processed_today || 0);
            this.updateElement('notifications-sent', data.application.notifications_sent_today || 0);
            this.updateElement('avg-api-response', 
                data.application.avg_response_time_ms ? `${data.application.avg_response_time_ms} ms` : '-- ms'
            );
            this.updateElement('websocket-connections', data.application.websocket_connections || 0);
        }
    }

    updateMetricBar(metricName, percentage) {
        const valueElement = document.getElementById(metricName);
        const barElement = document.getElementById(`${metricName}-bar`);

        if (valueElement) {
            valueElement.textContent = `${Math.round(percentage || 0)}%`;
        }

        if (barElement) {
            barElement.style.width = `${percentage || 0}%`;
            barElement.className = `metric-fill ${this.getMetricClass(percentage)}`;
        }
    }

    getMetricClass(percentage) {
        if (percentage >= 90) return 'critical';
        if (percentage >= 75) return 'warning';
        return 'normal';
    }

    updateElement(id, value) {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = value !== undefined ? value : '--';
        }
    }

    setupRefreshInterval() {
        this.refreshInterval = setInterval(() => {
            this.loadAllStatus();
        }, 30000); // Refresh every 30 seconds
    }

    connectWebSocket() {
        this.websocket = api.connectWebSocket(
            (data) => this.handleWebSocketMessage(data),
            (error) => console.warn('WebSocket connection failed:', error)
        );
    }

    handleWebSocketMessage(data) {
        switch (data.type) {
            case 'system_status_update':
                this.updateSystemHealth(data);
                break;
            case 'component_status_update':
                this.updateComponentCard(data.component, data.status);
                break;
            case 'camera_status_update':
                // Handle individual camera status updates
                break;
            case 'performance_update':
                this.updatePerformanceMetrics(data);
                break;
        }
    }

    static refreshAll() {
        return window.statusMonitor?.loadAllStatus();
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    destroy() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
        }
        if (this.websocket) {
            this.websocket.close();
        }
    }
}

/**
 * Dashboard Component
 */
class Dashboard {
    static init() {
        if (!window.dashboard) {
            window.dashboard = new Dashboard();
        }
        return window.dashboard.initialize();
    }

    async initialize() {
        try {
            await this.loadDashboardData();
            this.connectWebSocket();
            console.log('Dashboard initialized');
        } catch (error) {
            console.error('Failed to initialize dashboard:', error);
            UIHelpers.showToast('Failed to load dashboard data', 'error');
        }
    }

    async loadDashboardData() {
        try {
            const [systemStatus, cameraStatus] = await Promise.all([
                api.getSystemStatus(),
                api.getCameraStatus()
            ]);

            this.updateSystemStatusCard(systemStatus);
            this.updateCameraStatsCard(cameraStatus);

        } catch (error) {
            console.error('Error loading dashboard data:', error);
        }
    }

    updateSystemStatusCard(data) {
        const systemStatus = document.getElementById('system-status');
        const webserverStatus = document.getElementById('webserver-status');
        const emailStatus = document.getElementById('email-status');
        const telegramStatus = document.getElementById('telegram-status');

        if (systemStatus && data.system) {
            systemStatus.className = `status-indicator ${data.system.status === 'healthy' ? 'healthy' : 'warning'}`;
        }

        if (data.components) {
            if (webserverStatus) {
                webserverStatus.textContent = data.components.webserver?.status === 'running' ? 'Running' : 'Stopped';
            }
            if (emailStatus) {
                emailStatus.textContent = data.components.smtp_server?.status === 'running' ? 'Running' : 'Stopped';
            }
            if (telegramStatus) {
                telegramStatus.textContent = data.components.telegram_notifier?.status === 'running' ? 'Connected' : 'Disconnected';
            }
        }
    }

    updateCameraStatsCard(data) {
        const totalCameras = document.getElementById('total-cameras');
        const onlineCameras = document.getElementById('online-cameras');
        const offlineCameras = document.getElementById('offline-cameras');

        if (data.summary) {
            if (totalCameras) totalCameras.textContent = data.summary.total_cameras || 0;
            if (onlineCameras) onlineCameras.textContent = data.summary.online_cameras || 0;
            if (offlineCameras) offlineCameras.textContent = data.summary.offline_cameras || 0;
        }
    }

    connectWebSocket() {
        this.websocket = api.connectWebSocket(
            (data) => this.handleWebSocketMessage(data),
            (error) => console.warn('WebSocket connection failed:', error)
        );
    }

    handleWebSocketMessage(data) {
        switch (data.type) {
            case 'system_status_update':
                this.updateSystemStatusCard(data);
                break;
            case 'camera_status_update':
                // Reload camera status
                api.getCameraStatus().then(status => this.updateCameraStatsCard(status));
                break;
        }
    }
}

// Global action functions
function addNewCamera() {
    CameraManager.showAddForm();
}

function testAllCameras() {
    CameraManager.testAllCameras();
}

function viewSystemLogs() {
    window.location.href = '/status';
}