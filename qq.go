package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type QQBot struct {
	cfg      *Config
	conn     *websocket.Conn
	connMu   sync.Mutex
	token    string
	tokenExp time.Time
	tokenMu  sync.Mutex
	seq      atomic.Int64
	alive    atomic.Bool
	closeCh  chan struct{}
	memory   *MemoryStore
	client   *http.Client
}

type wsPayload struct {
	Op int                `json:"op"`
	D  json.RawMessage    `json:"d,omitempty"`
	S  int64              `json:"s,omitempty"`
	T  string             `json:"t,omitempty"`
}

type identifyPayload struct {
	Token   string `json:"token"`
	Intents int    `json:"intents"`
	Shard   [2]int `json:"shard"`
}

type messageEvent struct {
	ID          string `json:"id"`
	Author      struct {
		ID         string `json:"id"`
		UserOpenID string `json:"user_openid"`
	} `json:"author"`
	Content     string `json:"content"`
	Timestamp   string `json:"timestamp"`
	GroupOpenID string `json:"group_openid"`
}

type sendMsgReq struct {
	Content string `json:"content"`
	MsgType int    `json:"msg_type"`
	MsgID   string `json:"msg_id,omitempty"`
	MsgSeq  int    `json:"msg_seq"`
}

func NewQQBot(cfg *Config, memory *MemoryStore) *QQBot {
	return &QQBot{
		cfg:    cfg,
		closeCh: make(chan struct{}),
		memory: memory,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (b *QQBot) Run() {
	for {
		select {
		case <-b.closeCh:
			return
		default:
		}

		if err := b.connect(); err != nil {
			log.Printf("[QQ] 连接失败: %v, 5秒后重试", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if err := b.listen(); err != nil {
			log.Printf("[QQ] 连接断开: %v, 3秒后重连", err)
			time.Sleep(3 * time.Second)
		}
	}
}

func (b *QQBot) Shutdown() {
	b.alive.Store(false)
	close(b.closeCh)
	b.connMu.Lock()
	if b.conn != nil {
		b.conn.Close()
	}
	b.connMu.Unlock()
}

func (b *QQBot) connect() error {
	token, err := b.getToken()
	if err != nil {
		return fmt.Errorf("获取token失败: %w", err)
	}

	gatewayURL, err := b.getGatewayURL(token)
	if err != nil {
		return fmt.Errorf("获取gateway失败: %w", err)
	}

	header := http.Header{}
	header.Set("Authorization", fmt.Sprintf("Bot %s.%s", b.cfg.QQ.AppID, token))

	conn, _, err := websocket.DefaultDialer.Dial(gatewayURL, header)
	if err != nil {
		return fmt.Errorf("ws连接失败: %w", err)
	}

	b.connMu.Lock()
	if b.conn != nil {
		b.conn.Close()
	}
	b.conn = conn
	b.connMu.Unlock()

	b.alive.Store(true)
	log.Printf("[QQ] 已连接到网关")
	return nil
}

func (b *QQBot) listen() error {
	go b.heartbeat()
	defer b.alive.Store(false)

	for {
		_, msg, err := b.conn.ReadMessage()
		if err != nil {
			return err
		}

		var p wsPayload
		if err := json.Unmarshal(msg, &p); err != nil {
			continue
		}

		if p.S > 0 {
			b.seq.Store(p.S)
		}

		switch p.Op {
		case 10: // HELLO
			go b.handleHello(p.D)
		case 11: // HEARTBEAT_ACK
		case 0: // DISPATCH
			go b.dispatch(p.T, p.D)
		}
	}
}

func (b *QQBot) heartbeat() {
	ticker := time.NewTicker(40 * time.Second)
	defer ticker.Stop()

	for b.alive.Load() {
		select {
		case <-ticker.C:
			b.connMu.Lock()
			if b.conn != nil {
				b.conn.WriteJSON(wsPayload{Op: 1, D: json.RawMessage(fmt.Sprintf(`%d`, b.seq.Load()))})
			}
			b.connMu.Unlock()
		case <-b.closeCh:
			return
		}
	}
}

func (b *QQBot) handleHello(d json.RawMessage) {
	var hello struct {
		HeartbeatInterval int `json:"heartbeat_interval"`
	}
	json.Unmarshal(d, &hello)

	identify := identifyPayload{
		Token:   fmt.Sprintf("QQBot %s", b.mustToken()),
		Intents: 33554432,
		Shard:   [2]int{0, 1},
	}

	b.connMu.Lock()
	if err := b.conn.WriteJSON(wsPayload{Op: 2, D: mustMarshal(identify)}); err != nil {
		log.Printf("[QQ] 发送 IDENTIFY 失败: %v", err)
	}
	b.connMu.Unlock()

	log.Printf("[QQ] 已鉴权")
}

func (b *QQBot) dispatch(t string, d json.RawMessage) {
	switch t {
	case "C2C_MESSAGE_CREATE":
		var ev messageEvent
		if json.Unmarshal(d, &ev) != nil {
			return
		}
		b.handlePrivateMsg(ev)

	case "GROUP_AT_MESSAGE_CREATE":
		var ev messageEvent
		if json.Unmarshal(d, &ev) != nil {
			return
		}
		b.handleGroupMsg(ev)

	case "AT_MESSAGE_CREATE":
		var ev messageEvent
		if json.Unmarshal(d, &ev) != nil {
			return
		}
		b.handleChannelMsg(ev)
	}
}

func (b *QQBot) handlePrivateMsg(ev messageEvent) {
	chatID := "c2c:" + ev.Author.UserOpenID
	log.Printf("[QQ] 私聊消息 from=%s: %s", ev.Author.UserOpenID, truncate(ev.Content, 100))

	isNew := len(b.memory.Get(chatID)) == 0
	b.memory.Add(chatID, "user", ev.Content)

	reply := b.callLLM(chatID)

	if isNew {
		onboard := fmt.Sprintf("你的 OpenID: %s\n（配 notify.qq_user 时填此值以接收故障通知）\n\n---\n", ev.Author.UserOpenID)
		if reply == "" {
			reply = onboard + "你好！我是 Qapi 网关管家。有什么可以帮你的？"
		} else {
			reply = onboard + reply
		}
	}

	if reply == "" {
		reply = "抱歉，LLM 调用暂时失败，请稍后重试。"
		log.Printf("[QQ] LLM 调用失败，发送兜底回复给 %s", ev.Author.UserOpenID)
	}

	b.memory.Add(chatID, "assistant", reply)
	b.sendC2C(ev.Author.UserOpenID, reply, ev.ID)
	log.Printf("[QQ] 已回复 %s (%d 字)", ev.Author.UserOpenID, len([]rune(reply)))
}

func (b *QQBot) handleGroupMsg(ev messageEvent) {}

func (b *QQBot) handleChannelMsg(ev messageEvent) {
	log.Printf("[QQ] 频道消息: %s", truncate(ev.Content, 100))
}

func (b *QQBot) callLLM(chatID string) string {
	history := b.memory.Get(chatID)

	messages := make([]map[string]interface{}, 0, len(history)+2)
	messages = append(messages, map[string]interface{}{
		"role": "system",
		"content": `你是 Qapi，一个 API 网关管家 + 运维机器人。

## 核心规则
1. 你没有内置知识，所有数据必须调用 tool 获取。
2. 禁止编造任何信息——不要猜测提供商、框架名、配置细节。你不知道用户用什么软件。
3. QQ 消息不支持 markdown，禁止使用代码块、表格、加粗等格式。用纯文本分行。

## 行为准则
- 用户说"查看"→ 直接调工具，不要反问。
- 工具返回什么就报告什么，不要额外解释来源。
- 可以连续调多个工具。
- 简洁，只报真实数据。`,
	})
	messages = append(messages, map[string]interface{}{
		"role": "system",
		"content": `Qapi 是 Go 语言编写的 API 反向代理网关，7MB 单文件，运行内存 ~2MB。

功能：LLM API 透视转发 + 加权随机负载均衡 + 故障自动摘除 + 路径路由 + QQ 故障通知。

配置：config.yaml → proxy.pool.upstreams（上游列表）、proxy.pool.fallback（降级模型）、proxy.routes（路径路由）、notify.qq_user（故障通知接收人）。

管理 API：
- GET /admin/upstreams → 查看所有上游（请求数、失败数、健康状态）
- POST /admin/upstreams body: {"url":"...","key":"...","weight":1} → 添加上游
- DELETE /admin/upstreams body: {"url":"..."} → 移除上游
- GET /health → "ok"`,
	})

	for _, h := range history {
		messages = append(messages, map[string]interface{}{"role": h.Role, "content": h.Content})
	}

	tools := []map[string]interface{}{
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "list_upstreams",
				"description": "列出所有上游及其状态：URL、健康否、请求数、失败数",
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "list_models",
				"description": "列出所有可用的LLM模型。用户问'有哪些模型'、'支持什么模型'时调用",
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "add_upstream",
				"description": "添加一个新的API上游到网关池",
				"parameters": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"url":    map[string]string{"type": "string", "description": "上游API地址，如 https://api.xxx.com/v1"},
						"key":    map[string]string{"type": "string", "description": "API密钥"},
						"weight": map[string]string{"type": "integer", "description": "权重，默认1"},
					},
					"required": []string{"url", "key"},
				},
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "remove_upstream",
				"description": "从网关池中移除一个上游",
				"parameters": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"url": map[string]string{"type": "string", "description": "要移除的上游URL"},
					},
					"required": []string{"url"},
				},
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "check_health",
				"description": "检查网关服务健康状态",
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "proxy_stats",
				"description": "查看代理总览统计：总请求数、总失败数、上游数量、运行状态",
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "config_summary",
				"description": "查看当前Qapi配置摘要：监听端口、上游数量、降级链、路由规则、通知配置",
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "restart_service",
				"description": "重启Qapi服务。当用户要求重启、服务异常需要恢复时调用。重启后进程会自动恢复",
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "system_status",
				"description": "查看系统状态：CPU负载、内存使用、磁盘空间、运行时间",
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "list_processes",
				"description": "列出系统进程，可按名称过滤。用户问'有什么进程'、'查看进程'、'xxx在运行吗'时调用",
				"parameters": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"filter": map[string]string{"type": "string", "description": "进程名关键字过滤，不填则显示占用最高的进程"},
					},
				},
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "kill_process",
				"description": "终止一个进程。用户说'杀掉xxx进程'、'干掉xxx'时调用",
				"parameters": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"target": map[string]string{"type": "string", "description": "进程PID或进程名"},
						"force":  map[string]string{"type": "boolean", "description": "是否强制终止(SIGKILL)，默认优雅终止(SIGTERM)"},
					},
					"required": []string{"target"},
				},
			},
		},
	}

	body := map[string]interface{}{
		"model":    b.cfg.LLM.Model,
		"messages": messages,
		"tools":    tools,
		"tool_choice": "auto",
	}

	hadToolCall := false
	return b.chatWithTools(body, messages, &hadToolCall)
}

func (b *QQBot) chatWithTools(body map[string]interface{}, messages []map[string]interface{}, hadToolCall *bool) string {
	if len(messages) > 60 {
		return "对话过长，请发送新问题"
	}
	payload, _ := json.Marshal(body)
	url := fmt.Sprintf("%s/chat/completions", b.cfg.LLM.BaseURL)
	log.Printf("[LLM] 请求 %s model=%s msgs=%d", url, b.cfg.LLM.Model, len(messages))

	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		log.Printf("[LLM] 请求构建失败: %v", err)
		return ""
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+b.cfg.LLM.APIKey)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resp, err := b.client.Do(req.WithContext(ctx))
	if err != nil {
		log.Printf("[LLM] 调用失败: %v", err)
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		log.Printf("[LLM] 非200: %d %s", resp.StatusCode, string(respBody))
		return ""
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					ID   string `json:"id"`
					Type string `json:"type"`
					Func struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("[LLM] 解析失败: %v", err)
		return ""
	}

	if len(result.Choices) == 0 {
		return ""
	}

	msg := result.Choices[0].Message

	if len(msg.ToolCalls) == 0 {
		if !*hadToolCall && looksLikeFabricatedData(msg.Content) {
			messages = append(messages, map[string]interface{}{
				"role": "user", "content": "你必须调用工具获取真实数据，禁止编造。请重新执行。",
			})
			body["messages"] = messages
			return b.chatWithTools(body, messages, hadToolCall)
		}
		return msg.Content
	}

	*hadToolCall = true

	toolResults := make([]string, 0, len(msg.ToolCalls))
	for _, tc := range msg.ToolCalls {
		result := b.executeTool(tc.Func.Name, tc.Func.Arguments)
		toolResults = append(toolResults, result)
		messages = append(messages, map[string]interface{}{
			"role":         "tool",
			"tool_call_id": tc.ID,
			"content":      result,
		})
	}

	nextBody := map[string]interface{}{
		"model":    b.cfg.LLM.Model,
		"messages": messages,
	}
	final := b.chatWithTools(nextBody, messages, hadToolCall)
	if final == "" {
		return strings.Join(toolResults, "\n")
	}
	return final
}

func looksLikeFabricatedData(content string) bool {
	indicators := []string{
		"模型列表", "可用模型", "表格",
		"SiliconFlow", "硅基流动",
		"OneAPI", "NewAPI", "Ollama", "vLLM",
		"反代地址", "反向代理",
		"路由", "routes",
	}
	for _, kw := range indicators {
		if strings.Contains(content, kw) {
			return true
		}
	}
	return false
}

func (b *QQBot) executeTool(name, args string) string {
	adminURL := fmt.Sprintf("http://127.0.0.1%s", b.cfg.Proxy.Listen)
	log.Printf("[Tools] 执行 %s %s", name, truncate(args, 100))

	switch name {
	case "list_upstreams", "check_health":
		endpoint := "/admin/upstreams"
		if name == "check_health" {
			endpoint = "/health"
		}
		resp, err := b.client.Get(adminURL + endpoint)
		if err != nil {
			return fmt.Sprintf(`{"error":"%v"}`, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)

	case "list_models":
		resp, err := b.client.Get(adminURL + "/v1/models")
		if err != nil {
			return fmt.Sprintf(`{"error":"%v"}`, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)

	case "proxy_stats":
		resp, err := b.client.Get(adminURL + "/admin/upstreams")
		if err != nil {
			return fmt.Sprintf(`{"error":"%v"}`, err)
		}
		defer resp.Body.Close()
		var upstreams []map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&upstreams)
		totalReq, totalFail, healthy, unhealthy := uint64(0), uint64(0), 0, 0
		for _, u := range upstreams {
			if r, ok := u["requests"].(float64); ok { totalReq += uint64(r) }
			if f, ok := u["fails"].(float64); ok { totalFail += uint64(f) }
			if h, ok := u["healthy"].(bool); ok && h { healthy++ } else { unhealthy++ }
		}
		summary := map[string]interface{}{
			"total_upstreams":    len(upstreams),
			"healthy_upstreams":  healthy,
			"unhealthy_upstreams": unhealthy,
			"total_requests":     totalReq,
			"total_failures":     totalFail,
			"port":               b.cfg.Proxy.Listen,
			"wss_connected":      b.alive.Load(),
		}
		data, _ := json.Marshal(summary)
		return string(data)

	case "config_summary":
		summary := map[string]interface{}{
			"qq_mode":    "WSS",
			"qq_sandbox": b.cfg.QQ.Sandbox,
			"llm_model":  b.cfg.LLM.Model,
			"llm_url":    b.cfg.LLM.BaseURL,
			"proxy_port": b.cfg.Proxy.Listen,
			"upstreams":  len(b.cfg.Proxy.Pool.Upstreams),
			"fallback":   b.cfg.Proxy.Pool.Fallback,
			"routes":     len(b.cfg.Proxy.Routes),
			"notify_user": func() string {
				if b.cfg.Notify.QQUser != "" { return b.cfg.Notify.QQUser }
				return "(未配置)"
			}(),
		}
		data, _ := json.MarshalIndent(summary, "", "  ")
		return string(data)

	case "restart_service":
		log.Printf("[Tools] 收到重启指令，2秒后退出")
		go func() {
			time.Sleep(2 * time.Second)
			os.Exit(0)
		}()
		return `{"status":"restarting","message":"Qapi 将在2秒后重启。如使用 systemd/Docker 请确保 restart policy 为 always"}`

	case "system_status":
		return b.runCmd("sh", "-c", "echo '{\"hostname\":\"'$(hostname)'\",\"uptime\":\"'$(uptime -p | cut -d' ' -f2-)'\",\"load\":\"'$(cat /proc/loadavg | cut -d' ' -f1-3)'\",\"mem\":\"'$(free -m | awk '/Mem/{printf \"%d/%dMB %.0f%%\", $3, $2, $3*100/$2}')'\",\"disk\":\"'$(df -h / | awk 'NR==2{printf \"%s/%s %s\", $3, $2, $5}')'\",\"cpu_count\":'$(nproc)'}'")

	case "list_processes":
		filter := ""
		if args != "" {
			var p struct{ Filter string }
			json.Unmarshal([]byte(args), &p)
			filter = p.Filter
		}
		cmd := "ps aux --sort=-%mem | head -15"
		if filter != "" {
			cmd = fmt.Sprintf("ps aux | grep -i '%s' | grep -v grep | head -10", filter)
		}
		return b.runCmd("sh", "-c", cmd)

	case "kill_process":
		var p struct {
			Target string `json:"target"`
			Force  bool   `json:"force"`
		}
		json.Unmarshal([]byte(args), &p)
		if p.Target == "" {
			return `{"error":"需要指定进程名或PID"}`
		}
		signal := "TERM"
		if p.Force {
			signal = "KILL"
		}
		// 先查找再杀
		_, err := strconv.Atoi(p.Target)
		if err == nil {
			return b.runCmd("kill", "-"+signal, p.Target)
		}
		return b.runCmd("pkill", "-"+signal, "-f", p.Target)

	case "add_upstream":
		return b.adminPost("/admin/upstreams", args)

	case "remove_upstream":
		return b.adminPost("/admin/upstreams", "DELETE", strings.NewReader(args))

	default:
		return `{"error":"unknown tool"}`
	}
}

func (b *QQBot) adminPost(endpoint string, args ...interface{}) string {
	method := "POST"
	body := ""
	var reader io.Reader

	if len(args) > 0 {
		if s, ok := args[0].(string); ok && s == "DELETE" {
			method = "DELETE"
		}
	}
	if len(args) > 1 {
		if r, ok := args[1].(io.Reader); ok {
			reader = r
		}
	}
	if reader == nil && body != "" {
		reader = strings.NewReader(body)
	}
	if reader == nil {
		reader = strings.NewReader(argsStr(args))
	}

	req, err := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1%s%s", b.cfg.Proxy.Listen, endpoint), reader)
	if err != nil {
		return fmt.Sprintf(`{"error":"%v"}`, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Sprintf(`{"error":"%v"}`, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return string(respBody)
}

func argsStr(args []interface{}) string {
	if len(args) > 0 {
		if s, ok := args[0].(string); ok {
			return s
		}
	}
	return ""
}

func (b *QQBot) runCmd(name string, arg ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, arg...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Sprintf(`{"error":"%v","output":"%s"}`, err, strings.TrimSpace(string(output)))
	}
	result := strings.TrimSpace(string(output))
	if result == "" {
		return `{"output":"(empty)"}`
	}
	return result
}

func (b *QQBot) SendMessage(openID, content string) {
	b.sendHTTP(fmt.Sprintf("%s/v2/users/%s/messages", b.apiBase(), openID), content, "")
}

func (b *QQBot) sendC2C(openID, content, msgID string) {
	b.sendHTTP(fmt.Sprintf("%s/v2/users/%s/messages", b.apiBase(), openID), content, msgID)
}

func (b *QQBot) sendGroup(groupOpenID, content, msgID string) {
	b.sendHTTP(fmt.Sprintf("%s/v2/groups/%s/messages", b.apiBase(), groupOpenID), content, msgID)
}

func (b *QQBot) sendHTTP(url, content, msgID string) {
	reqBody := sendMsgReq{
		Content: content,
		MsgType: 0,
		MsgID:   msgID,
		MsgSeq:  1,
	}
	payload, _ := json.Marshal(reqBody)

	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		log.Printf("[QQ] 发送请求构建失败: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("QQBot %s", b.mustToken()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := b.client.Do(req.WithContext(ctx))
	if err != nil {
		log.Printf("[QQ] 发送消息失败: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		log.Printf("[QQ] 发送返回非200: %d %s", resp.StatusCode, string(respBody))
	}
}

func (b *QQBot) apiBase() string {
	if b.cfg.QQ.Sandbox {
		return "https://sandbox.api.sgroup.qq.com"
	}
	return "https://api.sgroup.qq.com"
}

func (b *QQBot) getToken() (string, error) {
	b.tokenMu.Lock()
	defer b.tokenMu.Unlock()

	if b.token != "" && time.Now().Before(b.tokenExp) {
		return b.token, nil
	}

	reqBody := map[string]string{
		"appId":        b.cfg.QQ.AppID,
		"clientSecret": b.cfg.QQ.AppSecret,
	}
	payload, _ := json.Marshal(reqBody)

	resp, err := b.client.Post(
		"https://bots.qq.com/app/getAppAccessToken",
		"application/json",
		bytes.NewReader(payload),
	)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		AccessToken string      `json:"access_token"`
		ExpiresIn   json.Number `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	expiresSec, _ := result.ExpiresIn.Int64()
	if expiresSec == 0 {
		expiresSec = 7200
	}
	b.token = result.AccessToken
	b.tokenExp = time.Now().Add(time.Duration(expiresSec-300) * time.Second)

	return b.token, nil
}

func (b *QQBot) mustToken() string {
	t, _ := b.getToken()
	return t
}

func (b *QQBot) getGatewayURL(token string) (string, error) {
	req, _ := http.NewRequest("GET", b.apiBase()+"/gateway", nil)
	req.Header.Set("Authorization", fmt.Sprintf("QQBot %s", token))

	resp, err := b.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if result.URL == "" {
		return "wss://api.sgroup.qq.com/websocket", nil
	}
	return result.URL, nil
}

func mustMarshal(v interface{}) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func truncate(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "..."
}
