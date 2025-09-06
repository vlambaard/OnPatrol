/**
 * OnPatrol Mobile Dashboard Enhancements
 * Specialized functionality for mobile monitoring
 */

/**
 * Mobile Dashboard Manager
 */
class MobileDashboard {
    constructor() {
        this.isSmallScreen = window.innerWidth < 768;
        this.isTouchDevice = 'ontouchstart' in window;
        this.refreshInterval = null;
        this.connectionStatus = 'connected';
        this.lastUpdateTime = null;
    }

    static init() {
        if (!window.mobileDashboard) {
            window.mobileDashboard = new MobileDashboard();
        }
        return window.mobileDashboard.initialize();
    }

    async initialize() {
        try {
            this.detectDeviceCapabilities();
            this.setupResponsiveLayout();
            this.setupTouchOptimizations();
            this.setupOfflineDetection();
            this.setupMobileRefresh();
            this.setupOrientationHandling();
            
            // Initialize with mobile-specific refresh rate
            if (this.isSmallScreen) {
                this.setupMobileStatusMonitoring();
            }
            
            console.log('Mobile dashboard initialized');
        } catch (error) {
            console.error('Failed to initialize mobile dashboard:', error);
        }
    }

    detectDeviceCapabilities() {
        // Detect screen size changes
        window.addEventListener('resize', () => {
            const wasSmall = this.isSmallScreen;
            this.isSmallScreen = window.innerWidth < 768;
            
            if (wasSmall !== this.isSmallScreen) {
                this.handleScreenSizeChange();
            }
        });

        // Detect connection type for data usage optimization
        if ('connection' in navigator) {
            const connection = navigator.connection;
            this.connectionType = connection.effectiveType;
            
            connection.addEventListener('change', () => {
                this.handleConnectionChange(connection);
            });
        }

        // Detect device orientation
        if (screen.orientation) {
            screen.orientation.addEventListener('change', () => {
                this.handleOrientationChange();
            });
        }
    }

    setupResponsiveLayout() {
        // Add responsive classes based on screen size
        const updateResponsiveClasses = () => {
            const body = document.body;
            
            if (this.isSmallScreen) {
                body.classList.add('mobile-layout');
                body.classList.remove('desktop-layout');
            } else {
                body.classList.add('desktop-layout');
                body.classList.remove('mobile-layout');
            }
            
            if (this.isTouchDevice) {
                body.classList.add('touch-device');
            }
        };

        updateResponsiveClasses();
        window.addEventListener('resize', updateResponsiveClasses);
    }

    setupTouchOptimizations() {
        if (!this.isTouchDevice) return;

        // Add touch-friendly interactions
        const cards = document.querySelectorAll('.card');
        cards.forEach(card => {
            // Add touch feedback
            card.addEventListener('touchstart', () => {
                card.style.transform = 'scale(0.98)';
            });
            
            card.addEventListener('touchend', () => {
                card.style.transform = '';
            });
        });

        // Implement pull-to-refresh for mobile
        this.setupPullToRefresh();
    }

    setupPullToRefresh() {
        let startY = 0;
        let pullDistance = 0;
        let isPulling = false;
        
        const refreshThreshold = 80;
        const mainContent = document.querySelector('.main-content');
        
        if (!mainContent) return;

        mainContent.addEventListener('touchstart', (e) => {
            if (window.scrollY === 0) {
                startY = e.touches[0].clientY;
                isPulling = true;
            }
        });

        mainContent.addEventListener('touchmove', (e) => {
            if (!isPulling) return;
            
            const currentY = e.touches[0].clientY;
            pullDistance = currentY - startY;
            
            if (pullDistance > 0 && window.scrollY === 0) {
                e.preventDefault();
                
                // Visual feedback
                const opacity = Math.min(pullDistance / refreshThreshold, 1);
                mainContent.style.transform = `translateY(${Math.min(pullDistance / 3, 30)}px)`;
                mainContent.style.opacity = 1 - (opacity * 0.3);
                
                // Show refresh indicator
                if (pullDistance > refreshThreshold) {
                    this.showMobileRefreshIndicator();
                }
            }
        });

        mainContent.addEventListener('touchend', () => {
            if (isPulling && pullDistance > refreshThreshold) {
                this.triggerMobileRefresh();
            }
            
            // Reset visual state
            mainContent.style.transform = '';
            mainContent.style.opacity = '';
            this.hideMobileRefreshIndicator();
            
            isPulling = false;
            pullDistance = 0;
        });
    }

    showMobileRefreshIndicator() {
        let indicator = document.getElementById('mobile-refresh-indicator');
        if (!indicator) {
            indicator = document.createElement('div');
            indicator.id = 'mobile-refresh-indicator';
            indicator.className = 'mobile-refresh-indicator';
            indicator.innerHTML = `
                <div class="refresh-spinner"></div>
                <span>Pull to refresh</span>
            `;
            
            document.querySelector('.main-content').prepend(indicator);
        }
        indicator.style.display = 'flex';
    }

    hideMobileRefreshIndicator() {
        const indicator = document.getElementById('mobile-refresh-indicator');
        if (indicator) {
            indicator.style.display = 'none';
        }
    }

    async triggerMobileRefresh() {
        this.showMobileRefreshIndicator();
        
        try {
            // Refresh status data
            if (window.statusMonitor) {
                await window.statusMonitor.loadAllStatus();
            }
            
            this.showMobileNotification('Status updated', 'success');
            
        } catch (error) {
            this.showMobileNotification('Update failed', 'error');
            console.error('Mobile refresh failed:', error);
        } finally {
            setTimeout(() => {
                this.hideMobileRefreshIndicator();
            }, 500);
        }
    }

    setupOfflineDetection() {
        window.addEventListener('online', () => {
            this.connectionStatus = 'connected';
            this.handleOnlineStatus();
        });

        window.addEventListener('offline', () => {
            this.connectionStatus = 'offline';
            this.handleOfflineStatus();
        });

        // Initial status
        if (!navigator.onLine) {
            this.handleOfflineStatus();
        }
    }

    handleOnlineStatus() {
        this.removeMobileOfflineIndicator();
        this.showMobileNotification('Connection restored', 'success');
        
        // Resume status monitoring
        if (window.statusMonitor) {
            window.statusMonitor.loadAllStatus();
        }
    }

    handleOfflineStatus() {
        this.showMobileOfflineIndicator();
        this.showMobileNotification('No internet connection', 'warning', 0); // Persistent
    }

    showMobileOfflineIndicator() {
        let indicator = document.getElementById('mobile-offline-indicator');
        if (!indicator) {
            indicator = document.createElement('div');
            indicator.id = 'mobile-offline-indicator';
            indicator.className = 'mobile-offline-indicator';
            indicator.innerHTML = `
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M16.5 9.4L7.55 0.45C7.21 0.15 6.77 0 6.32 0s-.89.15-1.23.45L0.45 5.09C0.15 5.43 0 5.87 0 6.32s.15.89.45 1.23l4.64 4.64L.45 16.83c-.3.3-.45.74-.45 1.18s.15.88.45 1.18l4.64 4.64c.34.3.78.45 1.23.45s.89-.15 1.23-.45l4.64-4.64 4.64 4.64c.34.3.78.45 1.23.45s.89-.15 1.23-.45l4.64-4.64c.3-.3.45-.74.45-1.18s-.15-.88-.45-1.18l-4.64-4.64z"/>
                </svg>
                <span>Offline</span>
            `;
            document.body.appendChild(indicator);
        }
        indicator.style.display = 'flex';
    }

    removeMobileOfflineIndicator() {
        const indicator = document.getElementById('mobile-offline-indicator');
        if (indicator) {
            indicator.remove();
        }
    }

    setupMobileRefresh() {
        // Optimize refresh intervals based on device capabilities
        const baseInterval = this.isSmallScreen ? 45000 : 30000; // 45s mobile, 30s desktop
        const batteryMultiplier = this.getBatteryOptimizationMultiplier();
        
        this.refreshInterval = setInterval(() => {
            if (this.connectionStatus === 'connected' && !document.hidden) {
                this.updateMobileStatus();
            }
        }, baseInterval * batteryMultiplier);
    }

    getBatteryOptimizationMultiplier() {
        if ('getBattery' in navigator) {
            navigator.getBattery().then((battery) => {
                if (battery.level < 0.2 && !battery.charging) {
                    return 2; // Double interval when battery is low
                }
            });
        }
        return 1;
    }

    async updateMobileStatus() {
        try {
            if (window.statusMonitor) {
                await window.statusMonitor.loadAllStatus();
                this.lastUpdateTime = new Date();
                this.updateLastRefreshIndicator();
            }
        } catch (error) {
            console.warn('Mobile status update failed:', error);
        }
    }

    updateLastRefreshIndicator() {
        const indicator = document.getElementById('last-updated');
        if (indicator && this.lastUpdateTime) {
            const timeStr = this.lastUpdateTime.toLocaleTimeString([], { 
                hour: '2-digit', 
                minute: '2-digit' 
            });
            indicator.textContent = timeStr;
        }
    }

    setupOrientationHandling() {
        this.handleOrientationChange();
    }

    handleOrientationChange() {
        // Adjust layout based on orientation
        setTimeout(() => {
            const isLandscape = window.innerWidth > window.innerHeight;
            const body = document.body;
            
            if (isLandscape && this.isSmallScreen) {
                body.classList.add('mobile-landscape');
                body.classList.remove('mobile-portrait');
            } else {
                body.classList.add('mobile-portrait');
                body.classList.remove('mobile-landscape');
            }
            
            // Force layout recalculation
            if (window.statusMonitor) {
                window.statusMonitor.loadAllStatus();
            }
        }, 100);
    }

    setupMobileStatusMonitoring() {
        // Mobile-specific status monitoring optimizations
        if (window.statusMonitor) {
            // Override refresh interval for mobile
            if (window.statusMonitor.refreshInterval) {
                clearInterval(window.statusMonitor.refreshInterval);
                window.statusMonitor.refreshInterval = setInterval(() => {
                    if (this.connectionStatus === 'connected' && !document.hidden) {
                        window.statusMonitor.loadAllStatus();
                    }
                }, 45000); // 45 seconds for mobile
            }
        }
    }

    handleScreenSizeChange() {
        // Adjust functionality when screen size changes
        if (this.isSmallScreen) {
            this.setupMobileStatusMonitoring();
        } else {
            // Reset to desktop intervals
            if (window.statusMonitor && window.statusMonitor.refreshInterval) {
                clearInterval(window.statusMonitor.refreshInterval);
                window.statusMonitor.setupRefreshInterval();
            }
        }
    }

    handleConnectionChange(connection) {
        this.connectionType = connection.effectiveType;
        
        // Adjust update frequency based on connection speed
        if (connection.effectiveType === 'slow-2g' || connection.effectiveType === '2g') {
            // Reduce update frequency on slow connections
            if (this.refreshInterval) {
                clearInterval(this.refreshInterval);
                this.setupMobileRefresh();
            }
        }
    }

    showMobileNotification(message, type = 'info', duration = 3000) {
        // Mobile-optimized notifications
        const notification = document.createElement('div');
        notification.className = `mobile-notification mobile-notification-${type}`;
        notification.textContent = message;
        
        document.body.appendChild(notification);
        
        // Animate in
        setTimeout(() => {
            notification.classList.add('show');
        }, 10);
        
        // Auto-remove if duration specified
        if (duration > 0) {
            setTimeout(() => {
                notification.classList.remove('show');
                setTimeout(() => {
                    if (notification.parentElement) {
                        notification.remove();
                    }
                }, 300);
            }, duration);
        }
    }

    destroy() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
        }
    }
}

// Initialize mobile dashboard when status monitor is ready
document.addEventListener('DOMContentLoaded', () => {
    // Wait for status monitor to be available
    const initMobile = () => {
        if (window.statusMonitor) {
            MobileDashboard.init();
        } else {
            setTimeout(initMobile, 100);
        }
    };
    
    initMobile();
});

// Cleanup on page unload
window.addEventListener('beforeunload', () => {
    if (window.mobileDashboard) {
        window.mobileDashboard.destroy();
    }
});