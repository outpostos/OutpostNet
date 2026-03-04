package config

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// HotReloadManager 配置热重载管理器
type HotReloadManager struct {
	configPath   string
	watcher      *fsnotify.Watcher
	reloadFunc   func(*ServerConfig) error
	currentConfig *ServerConfig
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	debounceTime time.Duration
	lastReload   time.Time
}

// NewHotReloadManager 创建配置热重载管理器
func NewHotReloadManager(configPath string, reloadFunc func(*ServerConfig) error) (*HotReloadManager, error) {
	if configPath == "" {
		return nil, fmt.Errorf("config path is required for hot reload")
	}

	// 确保配置文件存在
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file does not exist: %s", configPath)
	}

	// 获取绝对路径
	absPath, err := filepath.Abs(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %v", err)
	}

	// 创建文件监视器
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create file watcher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	hrm := &HotReloadManager{
		configPath:   absPath,
		watcher:      watcher,
		reloadFunc:   reloadFunc,
		ctx:          ctx,
		cancel:       cancel,
		debounceTime: 1 * time.Second, // 防抖时间，避免频繁重载
	}

	return hrm, nil
}

// Start 启动配置热重载监控
func (hrm *HotReloadManager) Start() error {
	// 加载初始配置
	config, err := LoadServerConfig(hrm.configPath)
	if err != nil {
		return fmt.Errorf("failed to load initial config: %v", err)
	}

	hrm.mu.Lock()
	hrm.currentConfig = config
	hrm.mu.Unlock()

	// 监控配置文件
	if err := hrm.watcher.Add(hrm.configPath); err != nil {
		return fmt.Errorf("failed to watch config file: %v", err)
	}

	// 监控配置文件所在目录（处理编辑器重命名文件的情况）
	configDir := filepath.Dir(hrm.configPath)
	if err := hrm.watcher.Add(configDir); err != nil {
		log.Printf("warning: failed to watch config directory %s: %v", configDir, err)
	}

	log.Printf("hot reload manager started, watching config file: %s", hrm.configPath)

	// 启动监控goroutine
	go hrm.watchLoop()

	return nil
}

// Stop 停止配置热重载监控
func (hrm *HotReloadManager) Stop() error {
	hrm.cancel()
	return hrm.watcher.Close()
}

// GetCurrentConfig 获取当前配置
func (hrm *HotReloadManager) GetCurrentConfig() *ServerConfig {
	hrm.mu.RLock()
	defer hrm.mu.RUnlock()
	
	if hrm.currentConfig == nil {
		return nil
	}
	
	// 返回配置的副本
	configCopy := *hrm.currentConfig
	return &configCopy
}

// watchLoop 监控文件变化的主循环
func (hrm *HotReloadManager) watchLoop() {
	for {
		select {
		case <-hrm.ctx.Done():
			log.Println("hot reload manager stopped")
			return

		case event, ok := <-hrm.watcher.Events:
			if !ok {
				log.Println("file watcher closed")
				return
			}

			// 只处理写入和创建事件
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				hrm.handleFileChange(event)
			}

		case err, ok := <-hrm.watcher.Errors:
			if !ok {
				log.Println("file watcher error channel closed")
				return
			}
			log.Printf("file watcher error: %v", err)
		}
	}
}

// handleFileChange 处理文件变化事件
func (hrm *HotReloadManager) handleFileChange(event fsnotify.Event) {
	// 检查是否是我们关心的配置文件
	if event.Name != hrm.configPath {
		// 可能是目录中的其他文件，检查是否是配置文件的重命名
		if filepath.Base(event.Name) != filepath.Base(hrm.configPath) {
			return
		}
	}

	// 防抖处理，避免频繁重载
	now := time.Now()
	if now.Sub(hrm.lastReload) < hrm.debounceTime {
		return
	}

	log.Printf("config file changed: %s, reloading...", event.Name)

	// 延迟一小段时间，确保文件写入完成
	time.Sleep(100 * time.Millisecond)

	if err := hrm.reloadConfig(); err != nil {
		log.Printf("failed to reload config: %v", err)
	} else {
		hrm.lastReload = now
		log.Println("config reloaded successfully")
	}
}

// reloadConfig 重新加载配置
func (hrm *HotReloadManager) reloadConfig() error {
	// 加载新配置
	newConfig, err := LoadServerConfig(hrm.configPath)
	if err != nil {
		return fmt.Errorf("failed to load new config: %v", err)
	}

	// 验证新配置
	if err := ValidateServerConfig(newConfig); err != nil {
		return fmt.Errorf("invalid new config: %v", err)
	}

	// 检查关键配置是否发生变化（某些配置不支持热重载）
	hrm.mu.RLock()
	oldConfig := hrm.currentConfig
	hrm.mu.RUnlock()

	if oldConfig != nil {
		if err := hrm.validateConfigChange(oldConfig, newConfig); err != nil {
			return fmt.Errorf("config change validation failed: %v", err)
		}
	}

	// 调用重载函数
	if hrm.reloadFunc != nil {
		if err := hrm.reloadFunc(newConfig); err != nil {
			return fmt.Errorf("failed to apply new config: %v", err)
		}
	}

	// 更新当前配置
	hrm.mu.Lock()
	hrm.currentConfig = newConfig
	hrm.mu.Unlock()

	return nil
}

// validateConfigChange 验证配置变更是否支持热重载
func (hrm *HotReloadManager) validateConfigChange(oldConfig, newConfig *ServerConfig) error {
	// 检查不支持热重载的配置项
	if oldConfig.Server.ListenAddr != newConfig.Server.ListenAddr {
		return fmt.Errorf("listen_addr cannot be changed via hot reload (old: %s, new: %s)", 
			oldConfig.Server.ListenAddr, newConfig.Server.ListenAddr)
	}

	if oldConfig.Server.UnixSockPath != newConfig.Server.UnixSockPath {
		return fmt.Errorf("unix_sock_path cannot be changed via hot reload (old: %s, new: %s)", 
			oldConfig.Server.UnixSockPath, newConfig.Server.UnixSockPath)
	}

	if oldConfig.K8sAPI.Port != newConfig.K8sAPI.Port {
		return fmt.Errorf("k8s_api.port cannot be changed via hot reload (old: %d, new: %d)", 
			oldConfig.K8sAPI.Port, newConfig.K8sAPI.Port)
	}

	if oldConfig.Monitoring.Port != newConfig.Monitoring.Port {
		return fmt.Errorf("monitoring.port cannot be changed via hot reload (old: %d, new: %d)", 
			oldConfig.Monitoring.Port, newConfig.Monitoring.Port)
	}

	return nil
}

// ConfigChangeEvent 配置变更事件
type ConfigChangeEvent struct {
	Type      string        `json:"type"`
	OldConfig *ServerConfig `json:"old_config,omitempty"`
	NewConfig *ServerConfig `json:"new_config"`
	Timestamp time.Time     `json:"timestamp"`
}

// ConfigReloadCallback 配置重载回调函数类型
type ConfigReloadCallback func(*ServerConfig) error

// HotReloadConfig 热重载配置选项
type HotReloadConfig struct {
	Enabled      bool          `yaml:"enabled" json:"enabled"`
	DebounceTime time.Duration `yaml:"debounce_time" json:"debounce_time"`
	WatchDir     bool          `yaml:"watch_dir" json:"watch_dir"`
}

// DefaultHotReloadConfig 返回默认热重载配置
func DefaultHotReloadConfig() *HotReloadConfig {
	return &HotReloadConfig{
		Enabled:      true,
		DebounceTime: 1 * time.Second,
		WatchDir:     true,
	}
}