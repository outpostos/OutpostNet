package main

import (
	"fmt"
	"log"
	"net"
	"time"
)

// 简单的 UDP 回显服务器，用于测试 UDP 代理功能
func main() {
	// 监听 UDP 端口 18001
	addr, err := net.ResolveUDPAddr("udp", ":18001")
	if err != nil {
		log.Fatalf("Failed to resolve UDP address: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on UDP port 18001: %v", err)
	}
	defer conn.Close()

	log.Println("UDP test server listening on :18001")

	buffer := make([]byte, 1024)
	
	for {
		// 读取数据包
		n, clientAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("Failed to read UDP packet: %v", err)
			continue
		}

		message := string(buffer[:n])
		log.Printf("Received from %s: %s", clientAddr, message)

		// 回显响应，添加时间戳
		response := fmt.Sprintf("Echo: %s (received at %s)", message, time.Now().Format("15:04:05"))
		
		_, err = conn.WriteToUDP([]byte(response), clientAddr)
		if err != nil {
			log.Printf("Failed to send UDP response: %v", err)
		}
	}
}