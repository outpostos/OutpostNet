package transport

import "time"

// Transport package provides benchmarking utilities for network data transfer performance
// This package is specifically designed to test the throughput, latency, and scalability
// of outpostnet's TCP, UDP, STCP, and SUDP proxy implementations.

// TransferMetrics 传输指标
type TransferMetrics struct {
	BytesTransferred int64         // 传输字节数
	Duration         time.Duration // 传输持续时间
	ConnectionCount  int           // 连接数
	ErrorCount       int           // 错误数
}

// CalculateThroughput 计算吞吐量 (bytes/second)
func (m *TransferMetrics) CalculateThroughput() float64 {
	if m.Duration == 0 {
		return 0
	}
	return float64(m.BytesTransferred) / m.Duration.Seconds()
}

// CalculateLatency 计算平均延迟
func (m *TransferMetrics) CalculateLatency() time.Duration {
	if m.ConnectionCount == 0 {
		return 0
	}
	return m.Duration / time.Duration(m.ConnectionCount)
}