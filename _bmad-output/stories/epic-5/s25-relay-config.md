# Story S25: Relay 配置模型

**Epic:** Epic 5 — Relay 中转 (Growth)
**Status:** Done

## 目标
扩展 Agent 配置格式，新增 RelayConfig 结构体和验证逻辑，支持 relay 规则的解析与校验。

## 内容
- `pkg/agentconfig/` — 扩展配置格式：
  - 新增 `RelayConfig` 结构体：
    ```go
    type RelayConfig struct {
        Service    string `yaml:"service"`      // 被中继的服务名
        FromRegion string `yaml:"from_region"`  // 请求来源区域
        ToRegion   string `yaml:"to_region"`    // 转发目标区域
        MaxHops    int    `yaml:"max_hops"`     // 最大跳数（防环）
    }
    ```
  - `Config.Relays []RelayConfig` — Agent 的 relay 规则列表
- 配置验证：
  - `from_region` != `to_region`（不能自己转自己）
  - `to_region` 必须对应一个已配置的 tunnel（client 角色）
  - `max_hops` 默认 3，最大 5
  - 检测循环引用
- YAML 序列化/反序列化
- 单元测试：解析、验证、边界值

## 验收标准
- Agent 可解析含 relay 规则的配置，验证通过，测试覆盖正常和异常场景

## 依赖
- S24 (Epic 4)

## 代码位置
- `pkg/agentconfig/` (relay extensions)
