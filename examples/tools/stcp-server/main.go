package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

// 简单的 HTTP 服务器，用于测试 STCP 加密隧道功能
func main() {
	// 创建简单的 HTTP 处理器
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		timestamp := time.Now().Format("15:04:05")
		response := fmt.Sprintf(`{
  "message": "Hello from STCP encrypted service!",
  "timestamp": "%s",
  "method": "%s",
  "path": "%s",
  "remote_addr": "%s",
  "security": "This connection is encrypted via STCP tunnel"
}`, timestamp, r.Method, r.URL.Path, r.RemoteAddr)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, response)

		log.Printf("Served request: %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
	})

	// 监听端口 18002
	log.Println("STCP test server listening on :18002")
	log.Println("This service will be accessible via STCP encrypted tunnel")
	
	if err := http.ListenAndServe(":18002", nil); err != nil {
		log.Fatalf("Failed to start STCP test server: %v", err)
	}
}