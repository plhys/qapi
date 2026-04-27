# Qapi

Go 语言编写的 QQ 机器人 + LLM API 反向代理网关，单文件部署，专为容器/Pod 环境设计。

## 技术栈

| 项 | 值 |
|---|-----|
| 语言 | Go 1.25 |
| 依赖 | gorilla/websocket、yaml.v3（仅 2 个） |
| 编译产物 | 单文件二进制 7.0MB |
| 运行内存 | ~2MB |
| 启动时间 | < 1 秒 |

## 功能

### QQ 机器人（私聊）

- 通过 QQ 官方 WebSocket 网关实时接收私聊消息
- 自动获取 AccessToken（含缓存）、建立 WSS 长连接、心跳保持
- 内置系统提示词，知道自己能管理 API 网关
- 连接断开自动重连（指数退避 3s/5s）
- 调用 LLM API 生成回复（支持任意 OpenAI 兼容接口）
- 对话记忆：内存缓存 + JSON 文件定时持久化，按会话隔离，自动截断

### LLM API 反向代理

- 透明转发 `/v1/` `/v2/` 路径下的 LLM API 请求（支持流式 SSE、Query 参数无损传递）
- 加权轮询负载均衡，多上游自动分发
- 故障上游自动摘除 + 30s 定时健康检查自动恢复
- 双向隐匿：上游看不到下游来源，下游看不到真实上游服务器
- 管理 API：`GET/POST/DELETE /admin/upstreams` 动态增删上游
- 健康检查端点：`GET /health`

### 部署形态

- 单进程、无运行时依赖
- 支持沙箱/生产环境自动切换 API 端点
- SIGTERM/SIGINT 信号触发优雅退出：断开 QQ → 落盘记忆 → 退出
- 一键配置：`./qapi setup` 交互式问答生成 `config.yaml`

## 运行负载

| 指标 | 值 |
|------|-----|
| 二进制体积 | **7.0 MB** |
| 空载内存 | **~2 MB** |
| 单并发 LLM 调用 | 内存 < 10MB |
| 代理吞吐 | 受限于上游延迟，本身无瓶颈 |
| QQ WSS 断线重连 | 对 HTTP 代理零影响（独立协程） |

## 快速开始

```bash
# 一键配置
./qapi setup

# 运行
./qapi config.yaml
```

`setup` 会交互式引导你填写 QQ AppID/AppSecret、LLM API 地址与 Key、代理上游列表，生成配置文件。

## 配置示例

```yaml
qq:
  app_id: "123456"
  app_secret: "your-secret"
  sandbox: true          # 开发阶段用沙箱

llm:
  provider: "openai"
  api_key: "sk-xxx"
  base_url: "https://api.openai.com/v1"
  model: "gpt-4o"

proxy:
  listen: ":8080"
  upstreams:
    - url: "https://api.openai.com"
      key: "sk-upstream-1"
      weight: 2
    - url: "https://api2.openai.com"
      key: "sk-upstream-2"
      weight: 1

memory:
  path: "./data/memory.json"
  max_turns: 20
```

## 部署

### 方式一：直接运行（容器/Pod/无 systemd）

```bash
# 上传二进制和 config.yaml，直接启动
chmod +x qapi
./qapi config.yaml &

# 后台运行，日志输出到文件
nohup ./qapi config.yaml > qapi.log 2>&1 &
```

保活可通过外部 supervisor（如 K8s liveness probe 调用 `/health` 端点）或 cron 定时检测。

### 方式二：systemd（有 systemd 的 VPS）

```ini
# /etc/systemd/system/qapi.service
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

```bash
systemctl daemon-reload
systemctl enable --now qapi
```

### 方式三：Docker

```dockerfile
FROM scratch
COPY qapi config.yaml /app/
WORKDIR /app
EXPOSE 8080
ENTRYPOINT ["./qapi", "config.yaml"]
```

```bash
docker build -t qapi .
docker run -d --name qapi -p 8080:8080 -v ./data:/app/data qapi
```

### 健康检查

部署后验证：

```bash
curl http://localhost:8080/health
# → ok
```
