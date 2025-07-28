# 使用 kubectl 管理 OutpostNet 服务

OutpostNet 提供完全兼容 K8s 的 REST API，可以直接使用 `kubectl` 进行管理。

## 配置 kubectl

1. 确保 OutpostNet-server 已启动并启用 K8s API：

```bash
# 启动 OutpostNet-server
./bin/OutpostNet-server -config examples/configs/server-with-k8s-api.yaml
```

2. 配置 OutpostNet 作为 kubectl 的集群：

```bash
# 添加 OutpostNet 集群
kubectl config set-cluster OutpostNet \
  --server=http://localhost:8080 \
  --insecure-skip-tls-verify=true

# 添加用户（OutpostNet 目前不需要认证）
kubectl config set-credentials OutpostNet-user

# 添加上下文
kubectl config set-context OutpostNet \
  --cluster=OutpostNet \
  --user=OutpostNet-user \
  --namespace=default

# 切换到 OutpostNet 上下文
kubectl config use-context OutpostNet
```

3. 验证连接：

```bash
# 测试连接
kubectl cluster-info

# 查看 API 版本
kubectl version --short
```

## 管理服务

### 查看服务列表

```bash
# 查看所有服务
kubectl get services

# 查看指定命名空间的服务
kubectl get services -n default

# 查看服务详情
kubectl describe service web-service
```

### 创建服务

目前 kubectl apply 存在协议兼容性问题，推荐使用 API 直接创建服务：

```bash
# 使用 curl 创建服务
curl -X POST "http://localhost:8080/api/v1/namespaces/default/services" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "kind": "Service",
    "metadata": {
      "name": "my-app-service",
      "namespace": "default",
      "labels": {
        "app": "my-app",
        "version": "v1"
      }
    },
    "spec": {
      "selector": {
        "app": "my-app"
      },
      "ports": [
        {
          "name": "http",
          "port": 80,
          "targetPort": 8080,
          "protocol": "TCP"
        }
      ]
    }
  }'
```

或者在 OutpostNet-server 配置文件中预定义服务。

### 查看端点

```bash
# 查看所有端点
kubectl get endpoints

# 查看指定服务的端点
kubectl get endpoints web-service

# 查看端点详情
kubectl describe endpoints web-service
```

### 使用标签选择器

```bash
# 根据标签查看服务
kubectl get services -l app=web
kubectl get services -l env=prod

# 查看多个标签
kubectl get services -l "app=web,env=prod"
```

### 输出格式

```bash
# JSON 格式输出
kubectl get services -o json

# YAML 格式输出
kubectl get services -o yaml

# 自定义列输出
kubectl get services -o custom-columns=NAME:.metadata.name,NAMESPACE:.metadata.namespace,PORTS:.spec.ports[*].port
```

## 监控和调试

### 实时监控

```bash
# 监控服务变化
kubectl get services -w

# 监控端点变化
kubectl get endpoints -w
```

### API 服务器信息

```bash
# 查看 API 版本
kubectl api-versions

# 查看 API 资源
kubectl api-resources

# 查看集群信息
kubectl cluster-info
```

## 高级功能

### 使用 JSON Patch

```bash
# 添加标签
kubectl patch service web-service -p '{"metadata":{"labels":{"environment":"production"}}}'

# 修改端口
kubectl patch service web-service -p '{"spec":{"ports":[{"name":"http","port":8080,"targetPort":8080,"protocol":"TCP"}]}}'
```

### 服务发现

查看服务的选择器和端点：

```bash
# 查看服务选择器
kubectl get service web-service -o jsonpath='{.spec.selector}'

# 查看服务端口
kubectl get service web-service -o jsonpath='{.spec.ports[*].port}'

# 查看端点 IP
kubectl get endpoints web-service -o jsonpath='{.subsets[*].addresses[*].ip}'
```

## 示例：完整的服务生命周期

```bash
# 1. 创建服务
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: nginx-service
  labels:
    app: nginx
spec:
  selector:
    app: nginx
  ports:
    - name: http
      port: 80
      targetPort: 80
      protocol: TCP
EOF

# 2. 查看创建的服务
kubectl get service nginx-service

# 3. 查看服务详情
kubectl describe service nginx-service

# 4. 查看端点状态
kubectl get endpoints nginx-service

# 5. 删除服务（注意：OutpostNet 目前可能不支持删除）
# kubectl delete service nginx-service
```

## 与 OutpostNet 特有功能的映射

| kubectl 概念 | OutpostNet 概念 | 说明 |
|-------------|-------------|------|
| Service | Service | 逻辑服务定义，支持选择器 |
| Endpoints | Endpoint | 具体的后端实例 |
| Labels | Labels | 用于服务发现和选择 |
| Namespace | Namespace | 服务命名空间隔离 |
| Selector | Selector | 端点选择器 |

## 注意事项

1. OutpostNet 的 K8s API 是只读的，主要用于查看和监控
2. 某些 kubectl 功能（如删除、更新）可能需要 OutpostNet 后续版本支持
3. OutpostNet 专注于网络代理，不支持 Pod、Deployment 等计算资源
4. 使用前确保 OutpostNet-server 已启动并启用了 K8s API（k8s_api.enabled: true）

通过这种方式，运维人员可以使用熟悉的 kubectl 工具来管理 OutpostNet 的网络服务，完全符合云原生的操作习惯。