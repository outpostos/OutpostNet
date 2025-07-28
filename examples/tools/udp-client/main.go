package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

// 简单的 UDP 客户端，用于测试 UDP 代理功能
func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run test_udp_client.go <port>")
	}
	
	port := os.Args[1]
	serverAddr := fmt.Sprintf("127.0.0.1:%s", port)
	
	// 解析服务器地址
	addr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		log.Fatalf("Failed to resolve UDP address %s: %v", serverAddr, err)
	}

	// 连接到服务器
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", serverAddr, err)
	}
	defer conn.Close()

	log.Printf("Connected to UDP server at %s", serverAddr)

	// 发送测试消息
	testMessages := []string{
		"Hello UDP server",
		"Test message 1",
		"Test message 2",
		"UDP proxy test",
	}

	for i, message := range testMessages {
		log.Printf("Sending message %d: %s", i+1, message)
		
		// 发送消息
		_, err := conn.Write([]byte(message))
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}

		// 设置读取超时
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		
		// 读取响应
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			log.Printf("Failed to read response: %v", err)
			continue
		}

		response := string(buffer[:n])
		log.Printf("Received response: %s", response)
		
		// 间隔一秒
		time.Sleep(1 * time.Second)
	}

	log.Println("UDP client test completed")
}