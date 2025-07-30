# CLAUDE.md - OutpostNet

This file provides guidance to Claude Code (claude.ai/code) when working with the OutpostNet module.

## Overview

OutpostNet 是 OutpostOS 项目的核心网络代理组件，提供轻量级的 TCP/UDP 隧道功能，专为边缘计算环境设计。


## Developing

### Task

使用 taskStatus 工具来记录和更新任务状态以便于和 kiro 协同。

在项目生命周期中修改 .kiro/specs/outpostos-service/tasks.md 文件中的任务状态标记。

>  outpostos-service 仅仅只是示例，根据项目不同会有区别

任务状态标记格式：

- [ ] 表示 not_started
- [-] 表示 in_progress
- [x] 表示 completed

