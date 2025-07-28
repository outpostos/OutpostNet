package config

import (
	"fmt"
	"net"
	"os"
	"strings"
)

// AddressResolver 地址解析器，支持多种连接协议
type AddressResolver struct {
	defaultProtocol string
}

// NewAddressResolver 创建地址解析器
func NewAddressResolver(defaultProto string) *AddressResolver {
	if defaultProto == "" {
		defaultProto = "tcp"
	}
	return &AddressResolver{
		defaultProtocol: defaultProto,
	}
}

// ParseAddress 解析地址，支持多种格式
// 支持的格式:
// - unix:///tmp/outpostnet.sock
// - tcp://127.0.0.1:7629
// - 127.0.0.1:7629 (默认tcp)
// - /tmp/outpostnet.sock (绝对路径默认unix)
func (ar *AddressResolver) ParseAddress(addr string) (network, address string, err error) {
	switch {
	case strings.HasPrefix(addr, "unix://"):
		return "unix", strings.TrimPrefix(addr, "unix://"), nil
	case strings.HasPrefix(addr, "tcp://"):
		return "tcp", strings.TrimPrefix(addr, "tcp://"), nil
	case strings.Contains(addr, ":") && !strings.Contains(addr, "/"):
		// IP:Port格式，默认TCP
		return "tcp", addr, nil
	case strings.HasPrefix(addr, "/"):
		// 绝对路径，默认Unix Socket
		return "unix", addr, nil
	default:
		// 使用默认协议
		return ar.defaultProtocol, addr, nil
	}
}

// Listen 根据协议创建监听器
func (ar *AddressResolver) Listen(listenProto, listenAddr, unixSockPath string) ([]net.Listener, error) {
	var listeners []net.Listener
	
	switch listenProto {
	case "tcp":
		listener, err := net.Listen("tcp", listenAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to create TCP listener: %v", err)
		}
		listeners = append(listeners, listener)
		
	case "unix":
		if unixSockPath == "" {
			return nil, fmt.Errorf("unix socket path is required for unix protocol")
		}
		// 清理可能存在的旧socket文件
		_ = removeUnixSocket(unixSockPath)
		
		listener, err := net.Listen("unix", unixSockPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create Unix socket listener: %v", err)
		}
		listeners = append(listeners, listener)
		
	case "dual", "":
		// 同时监听TCP和Unix Socket
		// TCP监听器
		tcpListener, err := net.Listen("tcp", listenAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to create TCP listener: %v", err)
		}
		listeners = append(listeners, tcpListener)
		
		// Unix Socket监听器
		if unixSockPath != "" {
			_ = removeUnixSocket(unixSockPath)
			unixListener, err := net.Listen("unix", unixSockPath)
			if err != nil {
				// TCP监听器创建成功但Unix Socket失败，关闭TCP监听器
				tcpListener.Close()
				return nil, fmt.Errorf("failed to create Unix socket listener: %v", err)
			}
			listeners = append(listeners, unixListener)
		}
		
	default:
		return nil, fmt.Errorf("unsupported listen protocol: %s", listenProto)
	}
	
	return listeners, nil
}

// Dial 根据地址创建连接
func (ar *AddressResolver) Dial(addr string) (net.Conn, error) {
	network, address, err := ar.ParseAddress(addr)
	if err != nil {
		return nil, err
	}
	
	return net.Dial(network, address)
}

// removeUnixSocket 移除可能存在的旧Unix Socket文件
func removeUnixSocket(path string) error {
	// 尝试连接，如果连接失败则认为socket文件可以安全删除
	conn, err := net.Dial("unix", path)
	if err == nil {
		conn.Close()
		return fmt.Errorf("unix socket %s is still in use", path)
	}
	
	// 删除旧的socket文件
	return removeFile(path)
}

// removeFile 删除文件，忽略文件不存在的错误
func removeFile(path string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}