#!/usr/bin/env python3
import http.server
import socketserver
import datetime

class CustomHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain; charset=utf-8')
        self.end_headers()
        
        response = f"""🎉 OutpostNet TCP 代理测试成功！

服务信息:
- 后端服务: Python HTTP Server  
- 监听端口: 18000
- 代理端口: 19999
- 请求时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- 请求路径: {self.path}

🔗 连接路径:
你的请求 → OutpostNet代理(19999) → OutpostNet服务端(17000) → 后端服务(18000)

✅ 代理功能正常工作！
"""
        self.wfile.write(response.encode('utf-8'))

    def log_message(self, format, *args):
        pass  # 禁用访问日志

PORT = 18000
with socketserver.TCPServer(("0.0.0.0", PORT), CustomHandler) as httpd:
    httpd.serve_forever()
