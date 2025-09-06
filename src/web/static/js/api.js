/**
 * OnPatrol API Communication Layer
 * Handles all HTTP requests to the backend API
 */

class OnPatrolAPI {
    constructor() {
        this.baseURL = '';
        this.requestTimeout = 10000; // 10 seconds
    }

    /**
     * Make authenticated HTTP request
     */
    async request(endpoint, options = {}) {
        const url = `${this.baseURL}${endpoint}`;
        const config = {
            timeout: this.requestTimeout,
            headers: {
                'Content-Type': 'application/json',
                ...options.headers
            },
            ...options
        };

        try {
            const response = await fetch(url, config);
            
            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new APIError(response.status, errorData.error || response.statusText, errorData);
            }

            return await response.json();
        } catch (error) {
            if (error instanceof APIError) {
                throw error;
            }
            throw new APIError(0, 'Network error or timeout', { originalError: error.message });
        }
    }

    // Camera Management API
    async getCameras() {
        return await this.request('/api/cameras');
    }

    async getCamera(cameraId) {
        return await this.request(`/api/cameras/${cameraId}`);
    }

    async createCamera(cameraId, cameraData) {
        return await this.request(`/api/cameras/${cameraId}`, {
            method: 'POST',
            body: JSON.stringify(cameraData)
        });
    }

    async updateCamera(cameraId, cameraData) {
        return await this.request(`/api/cameras/${cameraId}`, {
            method: 'PUT',
            body: JSON.stringify(cameraData)
        });
    }

    async deleteCamera(cameraId) {
        return await this.request(`/api/cameras/${cameraId}`, {
            method: 'DELETE'
        });
    }

    async testCamera(cameraId) {
        return await this.request(`/api/cameras/${cameraId}/test`, {
            method: 'POST'
        });
    }

    // Status API
    async getSystemStatus() {
        return await this.request('/api/status');
    }

    async getComponentStatus() {
        return await this.request('/api/status/components');
    }

    async getCameraStatus() {
        return await this.request('/api/status/cameras');
    }

    async getPerformanceMetrics() {
        return await this.request('/api/status/metrics');
    }

    // WebSocket connection
    connectWebSocket(onMessage, onError = null) {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsURL = `${protocol}//${window.location.host}/api/status/ws`;
        
        const ws = new WebSocket(wsURL);
        
        ws.onopen = () => {
            console.log('WebSocket connected');
        };
        
        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                onMessage(data);
            } catch (error) {
                console.error('Error parsing WebSocket message:', error);
            }
        };
        
        ws.onerror = (error) => {
            console.error('WebSocket error:', error);
            if (onError) onError(error);
        };
        
        ws.onclose = () => {
            console.log('WebSocket disconnected');
            // Auto-reconnect after 5 seconds
            setTimeout(() => {
                this.connectWebSocket(onMessage, onError);
            }, 5000);
        };
        
        return ws;
    }
}

/**
 * Custom API Error class
 */
class APIError extends Error {
    constructor(status, message, data = {}) {
        super(message);
        this.name = 'APIError';
        this.status = status;
        this.data = data;
    }

    isNetworkError() {
        return this.status === 0;
    }

    isValidationError() {
        return this.status === 400;
    }

    isNotFoundError() {
        return this.status === 404;
    }

    isServerError() {
        return this.status >= 500;
    }
}

/**
 * Form Validation utilities
 */
class FormValidator {
    constructor() {
        this.rules = new Map();
        this.errors = new Map();
    }

    /**
     * Add validation rule for a field
     */
    addRule(fieldName, validator, errorMessage) {
        if (!this.rules.has(fieldName)) {
            this.rules.set(fieldName, []);
        }
        this.rules.get(fieldName).push({ validator, errorMessage });
    }

    /**
     * Validate all fields
     */
    validate(formData) {
        this.errors.clear();
        let isValid = true;

        for (const [fieldName, rules] of this.rules) {
            const value = formData.get(fieldName) || formData[fieldName];
            
            for (const rule of rules) {
                if (!rule.validator(value, formData)) {
                    this.addError(fieldName, rule.errorMessage);
                    isValid = false;
                    break; // Stop at first error per field
                }
            }
        }

        return isValid;
    }

    /**
     * Add error message for field
     */
    addError(fieldName, message) {
        this.errors.set(fieldName, message);
    }

    /**
     * Get error message for field
     */
    getError(fieldName) {
        return this.errors.get(fieldName);
    }

    /**
     * Get all errors
     */
    getAllErrors() {
        return Object.fromEntries(this.errors);
    }

    /**
     * Clear all errors
     */
    clearErrors() {
        this.errors.clear();
    }

    /**
     * Common validation functions
     */
    static required(value) {
        return value !== null && value !== undefined && value.toString().trim() !== '';
    }

    static email(value) {
        if (!value) return true; // Optional field
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        return emailRegex.test(value);
    }

    static ipAddress(value) {
        if (!value) return false;
        const ipRegex = /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;
        return ipRegex.test(value) || this.isValidHostname(value);
    }

    static isValidHostname(value) {
        if (!value) return false;
        const hostnameRegex = /^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$/;
        return hostnameRegex.test(value);
    }

    static port(value) {
        if (!value) return true; // Optional field
        const portNum = parseInt(value);
        return !isNaN(portNum) && portNum >= 1 && portNum <= 65535;
    }

    static minLength(min) {
        return (value) => {
            if (!value) return true; // Optional field
            return value.toString().length >= min;
        };
    }

    static maxLength(max) {
        return (value) => {
            if (!value) return true; // Optional field
            return value.toString().length <= max;
        };
    }
}

/**
 * Camera Form Validator
 */
class CameraFormValidator extends FormValidator {
    constructor() {
        super();
        this.setupCameraValidation();
    }

    setupCameraValidation() {
        this.addRule('camera_name', FormValidator.required, 'Camera name is required');
        this.addRule('camera_name', FormValidator.maxLength(100), 'Camera name must be less than 100 characters');
        
        this.addRule('address', FormValidator.required, 'IP address or hostname is required');
        this.addRule('address', FormValidator.ipAddress, 'Please enter a valid IP address or hostname');
        
        this.addRule('email_from', (value) => !value || FormValidator.email(value), 'Please enter a valid email address');
        
        this.addRule('rtsp_port', FormValidator.port, 'Please enter a valid port number (1-65535)');
        
        this.addRule('username', FormValidator.maxLength(50), 'Username must be less than 50 characters');
        this.addRule('password', FormValidator.maxLength(100), 'Password must be less than 100 characters');
    }
}

/**
 * UI Helper functions
 */
class UIHelpers {
    /**
     * Show loading state
     */
    static showLoading(message = 'Loading...') {
        const overlay = document.getElementById('loading-overlay');
        const text = document.querySelector('#loading-overlay .loading-text');
        if (text) text.textContent = message;
        if (overlay) overlay.style.display = 'flex';
    }

    /**
     * Hide loading state
     */
    static hideLoading() {
        const overlay = document.getElementById('loading-overlay');
        if (overlay) overlay.style.display = 'none';
    }

    /**
     * Show toast notification
     */
    static showToast(message, type = 'info', duration = 5000) {
        // Create toast container if it doesn't exist
        let container = document.getElementById('toast-container');
        if (!container) {
            container = document.createElement('div');
            container.id = 'toast-container';
            container.className = 'toast-container';
            document.body.appendChild(container);
        }

        // Create toast element
        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.innerHTML = `
            <div class="toast-content">
                <span class="toast-message">${message}</span>
                <button class="toast-close" onclick="this.parentElement.parentElement.remove()">×</button>
            </div>
        `;

        container.appendChild(toast);

        // Auto-remove after duration
        setTimeout(() => {
            if (toast.parentElement) {
                toast.remove();
            }
        }, duration);

        return toast;
    }

    /**
     * Format file size
     */
    static formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    /**
     * Format duration
     */
    static formatDuration(seconds) {
        if (seconds < 60) return `${seconds}s`;
        if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        return `${hours}h ${minutes}m`;
    }

    /**
     * Format timestamp
     */
    static formatTimestamp(timestamp) {
        const date = new Date(timestamp);
        return date.toLocaleString();
    }

    /**
     * Debounce function
     */
    static debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }
}

// Global API instance
const api = new OnPatrolAPI();