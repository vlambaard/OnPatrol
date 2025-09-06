/**
 * OnPatrol Main Application Script
 * Handles application initialization and global functionality
 */

/**
 * Application Controller
 */
class OnPatrolApp {
    constructor() {
        this.currentPage = this.getCurrentPage();
        this.initialized = false;
    }

    getCurrentPage() {
        const path = window.location.pathname;
        if (path.includes('/cameras')) return 'cameras';
        if (path.includes('/status')) return 'status';
        return 'dashboard';
    }

    async initialize() {
        if (this.initialized) return;

        try {
            console.log(`Initializing OnPatrol app - ${this.currentPage} page`);
            
            // Initialize page-specific functionality
            switch (this.currentPage) {
                case 'cameras':
                    await CameraManager.init();
                    break;
                case 'status':
                    await StatusMonitor.init();
                    break;
                case 'dashboard':
                    await Dashboard.init();
                    break;
            }

            this.setupGlobalEventListeners();
            this.initialized = true;
            
            console.log('OnPatrol app initialized successfully');
            
        } catch (error) {
            console.error('Failed to initialize OnPatrol app:', error);
            UIHelpers.showToast('Application initialization failed', 'error');
        }
    }

    setupGlobalEventListeners() {
        // Handle navigation clicks
        document.addEventListener('click', (e) => {
            const navLink = e.target.closest('.nav-link');
            if (navLink && navLink.href) {
                this.handleNavigation(e, navLink);
            }
        });

        // Handle escape key to close modals
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this.closeModals();
            }
        });

        // Handle window beforeunload
        window.addEventListener('beforeunload', () => {
            this.cleanup();
        });

        // Handle visibility change (for pausing/resuming updates)
        document.addEventListener('visibilitychange', () => {
            this.handleVisibilityChange();
        });
    }

    handleNavigation(event, navLink) {
        // Update active navigation state
        document.querySelectorAll('.nav-link').forEach(link => {
            link.classList.remove('active');
        });
        navLink.classList.add('active');
    }

    closeModals() {
        // Close any open modals
        const modals = document.querySelectorAll('.modal-overlay');
        modals.forEach(modal => {
            if (modal.style.display !== 'none') {
                modal.style.display = 'none';
            }
        });

        // Call specific close functions
        if (window.cameraManager) {
            window.cameraManager.hideForm();
            window.cameraManager.hideDeleteModal();
        }
    }

    handleVisibilityChange() {
        if (document.hidden) {
            console.log('Page hidden - pausing updates');
            this.pauseUpdates();
        } else {
            console.log('Page visible - resuming updates');
            this.resumeUpdates();
        }
    }

    pauseUpdates() {
        // Pause real-time updates when page is hidden
        if (window.statusMonitor && window.statusMonitor.refreshInterval) {
            clearInterval(window.statusMonitor.refreshInterval);
        }
    }

    resumeUpdates() {
        // Resume updates when page becomes visible
        if (window.statusMonitor) {
            window.statusMonitor.setupRefreshInterval();
        }
    }

    cleanup() {
        // Clean up resources before page unload
        if (window.statusMonitor) {
            window.statusMonitor.destroy();
        }
        
        // Close WebSocket connections
        if (window.cameraManager && window.cameraManager.websocket) {
            window.cameraManager.websocket.close();
        }
        
        if (window.statusMonitor && window.statusMonitor.websocket) {
            window.statusMonitor.websocket.close();
        }

        if (window.dashboard && window.dashboard.websocket) {
            window.dashboard.websocket.close();
        }
    }
}

/**
 * Utility Functions
 */

// Check if device is mobile
function isMobile() {
    return window.innerWidth <= 768;
}

// Handle responsive navigation
function toggleMobileNav() {
    const nav = document.querySelector('.main-nav');
    if (nav) {
        nav.classList.toggle('mobile-open');
    }
}

// Format numbers with commas
function formatNumber(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

// Copy text to clipboard
async function copyToClipboard(text) {
    try {
        await navigator.clipboard.writeText(text);
        UIHelpers.showToast('Copied to clipboard', 'success');
        return true;
    } catch (error) {
        console.error('Failed to copy to clipboard:', error);
        UIHelpers.showToast('Failed to copy to clipboard', 'error');
        return false;
    }
}

// Download data as JSON file
function downloadJSON(data, filename) {
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

// Export camera configuration
async function exportCameraConfig() {
    try {
        const response = await api.getCameras();
        const timestamp = new Date().toISOString().split('T')[0];
        downloadJSON(response.cameras, `onpatrol-cameras-${timestamp}.json`);
        UIHelpers.showToast('Camera configuration exported', 'success');
    } catch (error) {
        console.error('Export failed:', error);
        UIHelpers.showToast('Failed to export configuration', 'error');
    }
}

// Show help modal
function showHelp() {
    // This could open a help modal or redirect to documentation
    UIHelpers.showToast('Help documentation coming soon', 'info');
}

// Show about modal  
function showAbout() {
    const aboutHtml = `
        <div class="about-content">
            <h3>OnPatrol Community Security Monitoring</h3>
            <p>Version 1.0.0</p>
            <p>An open-source security camera monitoring system designed for community groups and neighborhood watches.</p>
            <p><strong>Features:</strong></p>
            <ul>
                <li>Multi-camera support</li>
                <li>Email and Telegram notifications</li>
                <li>AI-powered object detection</li>
                <li>Real-time monitoring dashboard</li>
            </ul>
            <p>© 2025 OnPatrol Project</p>
        </div>
    `;
    
    // Could show in a modal - for now just show toast
    UIHelpers.showToast('OnPatrol v1.0.0 - Community Security Monitoring', 'info', 8000);
}

/**
 * Error Handling
 */

// Global error handler
window.addEventListener('error', (event) => {
    console.error('Global error:', event.error);
    
    // Don't show UI errors for script loading failures
    if (event.filename && event.filename.includes('.js')) {
        console.error('Script loading error:', event.filename);
        return;
    }
    
    UIHelpers.showToast('An unexpected error occurred', 'error');
});

// Handle unhandled promise rejections
window.addEventListener('unhandledrejection', (event) => {
    console.error('Unhandled promise rejection:', event.reason);
    
    // Don't show UI errors for network/API errors (handled by API layer)
    if (event.reason instanceof APIError) {
        return;
    }
    
    UIHelpers.showToast('An unexpected error occurred', 'error');
});

/**
 * Application Initialization
 */

// Initialize app when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM loaded, initializing OnPatrol app...');
    
    // Create global app instance
    window.onPatrolApp = new OnPatrolApp();
    window.onPatrolApp.initialize();
});

// Handle page load complete
window.addEventListener('load', () => {
    console.log('Page fully loaded');
    
    // Hide any loading indicators
    UIHelpers.hideLoading();
});

/**
 * Development Helpers (only in development)
 */
if (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1') {
    // Development mode helpers
    window.devHelpers = {
        api: api,
        showTestData: () => {
            console.log('Test data functions available');
        },
        clearStorage: () => {
            localStorage.clear();
            sessionStorage.clear();
            console.log('Storage cleared');
        }
    };
    
    console.log('Development mode - helpers available at window.devHelpers');
}