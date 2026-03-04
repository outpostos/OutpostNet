#!/usr/bin/env python3
"""
OutpostNet UDP 代理测试脚本
独立测试 UDP 代理功能
"""

import socket
import time
import json
import subprocess
import signal
import os
import sys
from datetime import datetime

def log_info(msg):
    print(f"[INFO] {msg}")

def log_success(msg):
    print(f"[SUCCESS] {msg}")

def log_error(msg):
    print(f"[ERROR] {msg}")

def test_udp_connection(host, port, message, timeout=5):
    """测试UDP连接"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(timeout)
        
        log_info(f"发送UDP数据到 {host}:{port}")
        sock.sendto(message.encode('utf-8'), (host, port))
        
        response, addr = sock.recvfrom(4096)
        sock.close()
        
        response_text = response.decode('utf-8')
        log_success(f"收到来自 {addr} 的响应: {len(response_text)} 字节")
        
        # 尝试解析JSON响应
        try:
            response_data = json.loads(response_text)
            if response_data.get('status') == 'success':
                log_success("✅ UDP 响应状态成功")
                log_info(f"   服务器: {response_data.get('server', 'unknown')}")
                log_info(f"   时间戳: {response_data.get('timestamp', 'unknown')}")
                return True
            else:
                log_error(f"❌ UDP 响应状态失败: {response_data.get('status')}")
                return False
        except json.JSONDecodeError:
            # 非JSON响应也算成功
            log_success("✅ 收到非JSON响应（可能是原始UDP数据）")
            log_info(f"   响应内容: {response_text[:100]}...")
            return True
            
    except socket.timeout:
        log_error(f"❌ UDP连接超时 ({timeout}秒)")
        return False
    except Exception as e:
        log_error(f"❌ UDP连接失败: {e}")
        return False

def check_port_listening(port, protocol='udp'):
    """检查端口是否在监听"""
    try:
        result = subprocess.run(['lsof', '-i', f':{port}'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0 and result.stdout.strip():
            log_success(f"✅ 端口 {port} 正在监听")
            return True
        else:
            log_error(f"❌ 端口 {port} 未监听")
            return False
    except Exception as e:
        log_error(f"检查端口 {port} 时出错: {e}")
        return False

def main():
    log_info("=== OutpostNet UDP 代理独立测试 ===")
    log_info("")
    
    # 检查端口监听状态
    log_info("1. 检查端口监听状态")
    
    ports_to_check = [
        (18001, "后端UDP服务"),
        (17001, "OutpostNet服务端"),
        (15353, "UDP代理")
    ]
    
    all_listening = True
    for port, desc in ports_to_check:
        if check_port_listening(port):
            log_success(f"   {desc} (端口 {port}) ✅")
        else:
            log_error(f"   {desc} (端口 {port}) ❌")
            all_listening = False
    
    if not all_listening:
        log_error("部分服务未正常启动，请先运行 ./run.sh")
        return False
    
    log_info("")
    log_info("2. 直接测试后端UDP服务 (端口 18001)")
    
    # 测试直接连接后端UDP服务
    direct_success = test_udp_connection('127.0.0.1', 18001, 'Direct UDP test message')
    
    log_info("")
    log_info("3. 测试通过代理的UDP连接 (端口 15353)")
    
    # 测试通过代理的UDP连接
    proxy_success = test_udp_connection('127.0.0.1', 15353, 'Proxy UDP test message')
    
    log_info("")
    log_info("4. 测试结果总结")
    
    if direct_success and proxy_success:
        log_success("🎉 所有UDP测试通过！")
        log_info("   - 直接后端连接: ✅")
        log_info("   - 代理连接: ✅")
        return True
    elif direct_success and not proxy_success:
        log_error("⚠️  后端服务正常，但代理连接失败")
        log_info("   - 直接后端连接: ✅")
        log_info("   - 代理连接: ❌")
        log_info("")
        log_info("可能的问题:")
        log_info("   1. 服务发现问题 - 代理无法找到后端服务")
        log_info("   2. 服务端配置问题 - 服务选择器不匹配")
        log_info("   3. 网络路由问题 - 代理到后端的连接失败")
        return False
    elif not direct_success:
        log_error("❌ 后端UDP服务本身有问题")
        log_info("   - 直接后端连接: ❌")
        log_info("   - 代理连接: %s" % ("✅" if proxy_success else "❌"))
        return False
    else:
        log_error("❌ 所有测试失败")
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        log_info("\n测试被用户中断")
        sys.exit(1)
    except Exception as e:
        log_error(f"测试过程中发生异常: {e}")
        sys.exit(1)