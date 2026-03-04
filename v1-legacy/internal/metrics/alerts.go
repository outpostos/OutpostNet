// 智能告警系统
package metrics

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// AlertLevel 告警级别
type AlertLevel int

const (
	AlertLevelInfo AlertLevel = iota
	AlertLevelWarning
	AlertLevelCritical
	AlertLevelEmergency
)

func (level AlertLevel) String() string {
	switch level {
	case AlertLevelInfo:
		return "INFO"
	case AlertLevelWarning:
		return "WARNING"
	case AlertLevelCritical:
		return "CRITICAL"
	case AlertLevelEmergency:
		return "EMERGENCY"
	default:
		return "UNKNOWN"
	}
}

// Alert 告警信息
type Alert struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Level       AlertLevel             `json:"level"`
	Source      string                 `json:"source"`
	Tags        map[string]string      `json:"tags"`
	Metadata    map[string]interface{} `json:"metadata"`
	Timestamp   time.Time              `json:"timestamp"`
	Resolved    bool                   `json:"resolved"`
	ResolvedAt  *time.Time             `json:"resolved_at,omitempty"`
	Count       int                    `json:"count"`
	FirstSeen   time.Time              `json:"first_seen"`
	LastSeen    time.Time              `json:"last_seen"`
}

// AlertRule 告警规则
type AlertRule struct {
	Name        string                                   `json:"name"`
	Description string                                   `json:"description"`
	Level       AlertLevel                               `json:"level"`
	Condition   func(*MetricsCollector) bool            `json:"-"`
	Cooldown    time.Duration                           `json:"cooldown"`
	Tags        map[string]string                       `json:"tags"`
	Handler     func(alert *Alert)                      `json:"-"`
	Enabled     bool                                     `json:"enabled"`
	LastFired   time.Time                               `json:"last_fired"`
}

// AlertManager 告警管理器
type AlertManager struct {
	mu               sync.RWMutex
	rules            map[string]*AlertRule
	activeAlerts     map[string]*Alert
	alertHistory     []*Alert
	maxHistorySize   int
	collector        *MetricsCollector
	notificationChan chan *Alert
	ctx              context.Context
	cancel           context.CancelFunc
}

// NewAlertManager 创建告警管理器
func NewAlertManager(collector *MetricsCollector) *AlertManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	am := &AlertManager{
		rules:            make(map[string]*AlertRule),
		activeAlerts:     make(map[string]*Alert),
		alertHistory:     make([]*Alert, 0),
		maxHistorySize:   1000,
		collector:        collector,
		notificationChan: make(chan *Alert, 100),
		ctx:              ctx,
		cancel:           cancel,
	}
	
	// 注册默认告警规则
	am.registerDefaultRules()
	
	return am
}

// Start 启动告警管理器
func (am *AlertManager) Start() {
	// 启动规则评估循环
	go am.evaluationLoop()
	
	// 启动通知处理循环
	go am.notificationLoop()
	
	log.Println("Alert manager started")
}

// Stop 停止告警管理器
func (am *AlertManager) Stop() {
	am.cancel()
	close(am.notificationChan)
	log.Println("Alert manager stopped")
}

// evaluationLoop 规则评估循环
func (am *AlertManager) evaluationLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			am.evaluateRules()
		case <-am.ctx.Done():
			return
		}
	}
}

// notificationLoop 通知处理循环
func (am *AlertManager) notificationLoop() {
	for {
		select {
		case alert := <-am.notificationChan:
			am.handleNotification(alert)
		case <-am.ctx.Done():
			return
		}
	}
}

// evaluateRules 评估所有告警规则
func (am *AlertManager) evaluateRules() {
	am.mu.RLock()
	rules := make([]*AlertRule, 0, len(am.rules))
	for _, rule := range am.rules {
		if rule.Enabled {
			rules = append(rules, rule)
		}
	}
	am.mu.RUnlock()
	
	for _, rule := range rules {
		am.evaluateRule(rule)
	}
}

// evaluateRule 评估单个告警规则
func (am *AlertManager) evaluateRule(rule *AlertRule) {
	// 检查冷却时间
	if time.Since(rule.LastFired) < rule.Cooldown {
		return
	}
	
	// 评估条件
	if rule.Condition(am.collector) {
		am.fireAlert(rule)
	}
}

// fireAlert 触发告警
func (am *AlertManager) fireAlert(rule *AlertRule) {
	am.mu.Lock()
	defer am.mu.Unlock()
	
	alertID := fmt.Sprintf("%s-%d", rule.Name, time.Now().Unix())
	
	// 检查是否已存在相同的活跃告警
	existingAlert := am.findActiveAlert(rule.Name)
	if existingAlert != nil {
		// 更新现有告警
		existingAlert.Count++
		existingAlert.LastSeen = time.Now()
		am.notificationChan <- existingAlert
		return
	}
	
	// 创建新告警
	alert := &Alert{
		ID:          alertID,
		Name:        rule.Name,
		Description: rule.Description,
		Level:       rule.Level,
		Source:      "alert-manager",
		Tags:        rule.Tags,
		Metadata:    am.collectAlertMetadata(rule),
		Timestamp:   time.Now(),
		Resolved:    false,
		Count:       1,
		FirstSeen:   time.Now(),
		LastSeen:    time.Now(),
	}
	
	// 添加到活跃告警
	am.activeAlerts[alertID] = alert
	
	// 添加到历史记录
	am.addToHistory(alert)
	
	// 更新规则触发时间
	rule.LastFired = time.Now()
	
	// 发送通知
	am.notificationChan <- alert
	
	// 调用自定义处理器
	if rule.Handler != nil {
		go rule.Handler(alert)
	}
}

// resolveAlert 解决告警
func (am *AlertManager) ResolveAlert(alertID string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	
	if alert, exists := am.activeAlerts[alertID]; exists {
		now := time.Now()
		alert.Resolved = true
		alert.ResolvedAt = &now
		
		// 从活跃告警中移除
		delete(am.activeAlerts, alertID)
		
		// 更新历史记录中的告警
		am.updateHistoryAlert(alert)
		
		log.Printf("Alert resolved: %s", alert.Name)
	}
}

// findActiveAlert 查找活跃告警
func (am *AlertManager) findActiveAlert(ruleName string) *Alert {
	for _, alert := range am.activeAlerts {
		if alert.Name == ruleName && !alert.Resolved {
			return alert
		}
	}
	return nil
}

// collectAlertMetadata 收集告警元数据
func (am *AlertManager) collectAlertMetadata(rule *AlertRule) map[string]interface{} {
	metadata := make(map[string]interface{})
	
	// 收集系统指标
	systemMetrics := am.collector.GetSystemMetrics()
	metadata["cpu_usage"] = systemMetrics.CPUUsage
	metadata["memory_usage"] = systemMetrics.MemoryUsage
	metadata["goroutines"] = systemMetrics.GoroutineCount
	
	// 收集服务指标
	serviceMetrics := am.collector.GetServiceMetrics()
	metadata["total_services"] = serviceMetrics.TotalServices
	metadata["healthy_services"] = serviceMetrics.HealthyServices
	metadata["unhealthy_services"] = serviceMetrics.UnhealthyServices
	
	// 收集连接指标
	connectionMetrics := am.collector.GetConnectionMetrics()
	metadata["active_connections"] = connectionMetrics.ActiveConnections
	metadata["failed_connections"] = connectionMetrics.FailedConnections
	
	return metadata
}

// addToHistory 添加到历史记录
func (am *AlertManager) addToHistory(alert *Alert) {
	am.alertHistory = append(am.alertHistory, alert)
	
	// 保持历史记录大小限制
	if len(am.alertHistory) > am.maxHistorySize {
		am.alertHistory = am.alertHistory[1:]
	}
}

// updateHistoryAlert 更新历史记录中的告警
func (am *AlertManager) updateHistoryAlert(alert *Alert) {
	for i, historyAlert := range am.alertHistory {
		if historyAlert.ID == alert.ID {
			am.alertHistory[i] = alert
			break
		}
	}
}

// handleNotification 处理通知
func (am *AlertManager) handleNotification(alert *Alert) {
	// 根据告警级别进行不同的处理
	switch alert.Level {
	case AlertLevelEmergency:
		log.Printf("🚨 EMERGENCY ALERT: %s - %s", alert.Name, alert.Description)
		// 可以集成短信、电话等紧急通知方式
		
	case AlertLevelCritical:
		log.Printf("🔥 CRITICAL ALERT: %s - %s", alert.Name, alert.Description)
		// 可以集成即时通讯、邮件等通知方式
		
	case AlertLevelWarning:
		log.Printf("⚠️  WARNING ALERT: %s - %s", alert.Name, alert.Description)
		// 可以集成工单系统等
		
	case AlertLevelInfo:
		log.Printf("ℹ️  INFO ALERT: %s - %s", alert.Name, alert.Description)
		// 记录日志即可
	}
	
	// 这里可以集成各种通知渠道
	// - Webhook
	// - 邮件
	// - 短信
	// - Slack/钉钉/企业微信
	// - PagerDuty
	// - etc.
}

// RegisterRule 注册告警规则
func (am *AlertManager) RegisterRule(rule *AlertRule) {
	am.mu.Lock()
	defer am.mu.Unlock()
	
	am.rules[rule.Name] = rule
	log.Printf("Alert rule registered: %s", rule.Name)
}

// GetActiveAlerts 获取活跃告警
func (am *AlertManager) GetActiveAlerts() []*Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	alerts := make([]*Alert, 0, len(am.activeAlerts))
	for _, alert := range am.activeAlerts {
		alerts = append(alerts, alert)
	}
	
	return alerts
}

// GetAlertHistory 获取告警历史
func (am *AlertManager) GetAlertHistory(limit int) []*Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	if limit <= 0 || limit > len(am.alertHistory) {
		limit = len(am.alertHistory)
	}
	
	// 返回最近的告警
	start := len(am.alertHistory) - limit
	if start < 0 {
		start = 0
	}
	
	return am.alertHistory[start:]
}

// registerDefaultRules 注册默认告警规则
func (am *AlertManager) registerDefaultRules() {
	// 系统资源告警
	am.RegisterRule(&AlertRule{
		Name:        "high-cpu-usage",
		Description: "CPU使用率过高",
		Level:       AlertLevelWarning,
		Condition: func(collector *MetricsCollector) bool {
			systemMetrics := collector.GetSystemMetrics()
			return systemMetrics.CPUUsage > 80.0
		},
		Cooldown: 5 * time.Minute,
		Tags:     map[string]string{"category": "system", "resource": "cpu"},
		Enabled:  true,
	})
	
	am.RegisterRule(&AlertRule{
		Name:        "high-memory-usage",
		Description: "内存使用率过高",
		Level:       AlertLevelWarning,
		Condition: func(collector *MetricsCollector) bool {
			systemMetrics := collector.GetSystemMetrics()
			if systemMetrics.MemoryLimit > 0 {
				usage := float64(systemMetrics.MemoryUsage) / float64(systemMetrics.MemoryLimit)
				return usage > 0.85
			}
			return false
		},
		Cooldown: 5 * time.Minute,
		Tags:     map[string]string{"category": "system", "resource": "memory"},
		Enabled:  true,
	})
	
	// 服务健康告警
	am.RegisterRule(&AlertRule{
		Name:        "service-unavailable",
		Description: "有服务不可用",
		Level:       AlertLevelCritical,
		Condition: func(collector *MetricsCollector) bool {
			serviceMetrics := collector.GetServiceMetrics()
			return serviceMetrics.UnhealthyServices > 0
		},
		Cooldown: 2 * time.Minute,
		Tags:     map[string]string{"category": "service", "type": "availability"},
		Enabled:  true,
	})
	
	am.RegisterRule(&AlertRule{
		Name:        "high-connection-failure-rate",
		Description: "连接失败率过高",
		Level:       AlertLevelWarning,
		Condition: func(collector *MetricsCollector) bool {
			connectionMetrics := collector.GetConnectionMetrics()
			if connectionMetrics.TotalConnections > 100 {
				failureRate := float64(connectionMetrics.FailedConnections) / float64(connectionMetrics.TotalConnections)
				return failureRate > 0.1 // 10% 失败率
			}
			return false
		},
		Cooldown: 3 * time.Minute,
		Tags:     map[string]string{"category": "network", "type": "connection"},
		Enabled:  true,
	})
	
	// 网络性能告警
	am.RegisterRule(&AlertRule{
		Name:        "high-connection-latency",
		Description: "连接延迟过高",
		Level:       AlertLevelWarning,
		Condition: func(collector *MetricsCollector) bool {
			connectionMetrics := collector.GetConnectionMetrics()
			return connectionMetrics.AvgConnectionLatency > 5*time.Second
		},
		Cooldown: 5 * time.Minute,
		Tags:     map[string]string{"category": "network", "type": "latency"},
		Enabled:  true,
	})
	
	// 安全告警
	am.RegisterRule(&AlertRule{
		Name:        "too-many-goroutines",
		Description: "Goroutine数量异常",
		Level:       AlertLevelWarning,
		Condition: func(collector *MetricsCollector) bool {
			systemMetrics := collector.GetSystemMetrics()
			return systemMetrics.GoroutineCount > 10000
		},
		Cooldown: 10 * time.Minute,
		Tags:     map[string]string{"category": "system", "type": "goroutine-leak"},
		Enabled:  true,
	})
	
	log.Println("Default alert rules registered")
}

// GetRules 获取所有告警规则
func (am *AlertManager) GetRules() map[string]*AlertRule {
	am.mu.RLock()
	defer am.mu.RUnlock()
	
	rules := make(map[string]*AlertRule)
	for name, rule := range am.rules {
		rules[name] = rule
	}
	
	return rules
}

// UpdateRule 更新告警规则
func (am *AlertManager) UpdateRule(name string, rule *AlertRule) error {
	am.mu.Lock()
	defer am.mu.Unlock()
	
	if _, exists := am.rules[name]; !exists {
		return fmt.Errorf("rule not found: %s", name)
	}
	
	am.rules[name] = rule
	log.Printf("Alert rule updated: %s", name)
	
	return nil
}

// DeleteRule 删除告警规则
func (am *AlertManager) DeleteRule(name string) error {
	am.mu.Lock()
	defer am.mu.Unlock()
	
	if _, exists := am.rules[name]; !exists {
		return fmt.Errorf("rule not found: %s", name)
	}
	
	delete(am.rules, name)
	log.Printf("Alert rule deleted: %s", name)
	
	return nil
}