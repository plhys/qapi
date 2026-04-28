# Qapi 开发笔记

## 项目概述

Qapi 是一个 Go 语言编写的 QQ 机器人 + LLM API 反向代理网关。单文件 7MB，运行内存 ~2MB，专为容器/Pod 环境设计。

**核心能力**：QQ 私聊 Agent + API 透明代理 + 网关管理 + 系统运维。

## 开发时间线

### v0.1 — 基础骨架

- Go 项目初始化，依赖仅 `gorilla/websocket` + `yaml.v3`
- QQ Bot WSS 长连接：Token 自动获取/缓存/续期、IDENTIFY 鉴权、心跳保持、断线重连
- API 反向代理：`httputil.ReverseProxy` 透传 `/v1/` `/v2/` 路径
- 加权轮询负载均衡 + 故障上游自动摘除 + 30s 健康检查
- 对话记忆：内存缓存 + JSON 文件持久化，按 chat 隔离
- 私聊/群聊/频道消息处理
- YAML 配置文件

### v0.2 — Agent 化 + 生产加固

#### Agent 工具调用
- Bot 通过 OpenAI function calling 获得 11 个真实工具
- 工具覆盖：上游管理（查/增/删）、模型列表查询、健康检查、配置摘要、代理统计
- 系统运维：进程列表、进程终止、系统状态、服务重启
- 工具执行结果回注 LLM 完成最终回复

#### 防幻觉机制
- **问题**：Bot 编造了不存在的模型列表（SiliconFlow），并虚构了"OneAPI 反代"等不实信息
- **修法**：双层防御
  1. 系统提示词硬约束："你没有内置知识，必须调用工具，禁止编造数据"
  2. 回复关键词拦截：含 "SiliconFlow"/"OneAPI"/"反代地址" 等词且未调工具 → 强制重试

#### 路径路由
- 上游 URL 含路径前缀（如 `/openai-custom-cn/v1`）时，原 `req.URL.Path = r.URL.Path` 会丢弃前缀
- 改为 `overlapPath()` 智能去重拼接，避免 `/v1/v1/models` 双段问题
- 支持 `proxy.routes` 按路径前缀分流到独立上游池

#### 负载均衡改造
- 加权轮询 → 加权随机：不可预测，上游看不出负载均衡痕迹
- 连接池复用：`sync.Map` 按上游 URL 缓存 `ReverseProxy`

#### 双向隐身
- Director 清 `X-Forwarded-*`、`Forwarded` 等代理痕迹头
- ModifyResponse 清 `Server`、`Via`、`X-Powered-By` 等上游信息头

#### SSE 流式保活
- `FlushInterval = 100ms` 确保流式响应逐块刷新
- `WriteTimeout = 600s` 覆盖长图生成

#### 故障通知
- 上游挂掉时 QQ Bot 自动发消息给 `notify.qq_user`
- 含 URL、时间、状态信息

#### Webhook 支持
- `/qq/callback` 端点 + ED25519 签名校验（沙箱模式跳过校验）
- 与 WSS 双模共存：Webhook 主接收、WSS 备用
- 回调地址验证（op=13 握手）

#### 其他
- OpenID 首次告知：新用户第一条消息回复其 OpenID
- `./qapi setup` 交互式配置向导
- 启动校验：占位 AppID/AppSecret 直接 `log.Fatalf` 退出
- Token 解析兼容 `expires_in` 字符串/整数

### v0.3 — 智能运维体 + 用户体验

#### QQ 机器人智能化
- **解除限制**：系统提示词从"只能调工具"改为"常识直接答，运维调工具"，超出范围也能正常对话
- **新增 4 个工具**：
  - `web_search` — DuckDuckGo 搜索互联网
  - `run_shell` — 执行任意 shell 命令（部署、杀进程、清内存、git 操作等）
  - `read_file` — 读取服务器文件（日志、配置、代码）
  - `manage_service` — systemd 服务管理（启停/重启/状态查询）
- **输出格式化**：工具结果先转可读文本再喂给 LLM，杜绝原始 JSON 出现在回复中
- **假数据检测优化**：不再拦截常识回答，只标记"虚构服务器信息"的情况

#### 代理功能改进
- **同 URL 不同 Key 可添加**：去重键从 URL 改为 URL+Key，同一上游多 Key 轮询
- **上游列表格式化**：`/admin/upstreams` 返回更友好的统计摘要

### 审计修复 (v0.2-hotfix)

- 工具层 `adminURL` 从硬编码 `:18885` 改为 `cfg.Proxy.Listen` 动态读取
- setup 向导默认值清零（去掉内网 IP 和假 Key）
- `config.example.yaml` 全部改为通用占位符
- GitHub Release 上传预编译二进制 `qapi-linux-amd64`

## 踩过的坑

### 1. 上游路径拼接
上游 URL `http://10.10.10.233:3000/openai-custom-cn/v1`，请求路径 `/v1/models`。
简单拼接得到 `/openai-custom-cn/v1/v1/models`（双 `/v1`），上游返回 404。
解决方案：`overlapPath()` 检测目标路径末段与请求路径首段重叠，去重。

### 2. WSS 断连
`conn.SetReadDeadline(90s)` 在 `connect()` 中设置一次后不再刷新。
90 秒后读超时，WSS 断开。QQ 心跳周期 45 秒，心跳 ACK 触发了读事件但 deadline 不更新。
解决方案：移除 `SetReadDeadline`，依赖 QQ 网关自身的心跳超时机制。

### 3. LLM 失败沉默
`callLLM()` 返回空字符串时，`handlePrivateMsg()` 直接 `return`，用户看不到任何回复。
解决方案：失败时发送兜底回复 + 打日志。

### 4. Docker 镜像源
`docker.io` 被墙，`10.10.10.40:7890` 代理对 Docker Hub 有 TLS 握手问题。
解决方案：使用飞牛 NAS 自带镜像源 `docker.fnnas.com`，不需要代理。

### 5. 内存模型
`MemoryStore` 使用 `sync.RWMutex` 保护，`save()` 通过写临时文件 + `os.Rename()` 原子替换。
首次加载时可能的数据文件不存在，已做静默容错。

### 6. function calling 递归深度
LLM 可能在有 tools 的情况下无限制地调用工具，导致 token 消耗失控。
解决方案：在 `chatWithTools` 中 `len(messages) > 60` 时强制截断。

### 7. QQ 鉴权 Token `expires_in` 类型
QQ 官方 API 返回的 `expires_in` 有时是 int (`7200`)，有时是 string (`"7200"`)。
Go 的 `json.Unmarshal` 对类型严格匹配，用 `int` 解析 string 会报错。
解决方案：改用 `json.Number`，再 `Int64()` 取值，失败时回退默认值。

## 架构决策

### 为什么选 Go
- 单文件部署，无运行时依赖
- 协程天然支持并发（QQ WSS + HTTP 代理 + 健康检查 + 心跳）
- 编译后 7MB，运行内存 ~2MB
- 用户环境不需要额外安装运行时

### 为什么不用 Webhook 做主通道
- Webhook 需要公网 IP，Pod 没有
- 甲骨文可以做转发但 VPN 是依赖项，多一跳多一个故障点
- WSS 代码已稳定，Webhook 作为备用

### 为什么只保留私聊
- 群聊/频道消息需要 Content 解析（去除 @标签）
- 群聊有频率限制（被动回复 5 分钟有效期、每消息最多 5 次）
- 用户需求聚焦私聊场景

### 为什么不用 OneAPI/NewAPI
- 用户自建了 `aiclient-2-api`（Docker 部署，3000 端口）做上游聚合
- Qapi 定位是"QQ Bot + 轻量代理网关"，不替代上游聚合
- 两层分工：Octopus/上游聚合管模型路由和计费，Qapi 管 Bot 交互和透明转发

## 文件结构

```
qapi/
├── main.go          # 入口：setup 向导、进程协调、信号处理
├── config.go        # YAML 配置加载、默认值
├── qq.go            # QQ Bot：WSS 连接、消息处理、LLM 调用、工具执行
├── proxy.go         # API 代理：负载均衡、路径路由、健康检查、管理 API
├── webhook.go       # Webhook 端点：签名校验、事件派发
├── config.yaml      # 生产配置（gitignore）
├── config.example.yaml  # 示例配置（通用占位符）
├── go.mod / go.sum  # Go 模块依赖
├── README.md        # 项目文档
├── DEVLOG.md        # 本文档
└── .gitignore
```

## 部署参考

### 直接运行
```bash
wget https://github.com/plhys/qapi/releases/download/v0.3/qapi-linux-amd64
chmod +x qapi-linux-amd64
./qapi-linux-amd64 setup
./qapi-linux-amd64 config.yaml
```

### 从源码编译
```bash
git clone https://github.com/plhys/qapi.git
cd qapi
go build -ldflags="-s -w" -o qapi .
```

### Docker
```dockerfile
FROM alpine:latest
COPY qapi config.yaml /app/
WORKDIR /app
EXPOSE 18885
ENTRYPOINT ["./qapi", "config.yaml"]
```

### systemd
```ini
[Unit]
Description=Qapi QQ Bot + API Proxy
After=network.target
[Service]
Type=simple
WorkingDirectory=/opt/qapi
ExecStart=/opt/qapi/qapi config.yaml
ExecStop=/bin/kill -TERM $MAINPID
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
```
