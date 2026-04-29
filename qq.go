package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

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
		"content": `你是 Qapi，一个运行在 Linux 服务器上的运维智能体。

## 行为模式
1. 理解意图：分析用户想做什么，确定目标。
2. 制定计划：需要几步？用到哪些工具？
3. 执行：调用工具获取信息或执行操作。
4. 验证：看结果是否符合预期，不符合就换方法。
5. 报告：用简洁的纯文本告诉用户做了什么、结果如何。

## 核心规则
- 实时数据（系统状态、进程、日志等）必须调工具获取，禁止编造。
- 通用知识问题直接回答，不用调工具。
- 一个请求可以连续调用多个工具，比如先检查再修改。
- 工具返回错误时，尝试换个参数或换个工具，不要直接放弃。
- QQ 消息不支持 markdown。纯文本分行，用 - 列表，缩进层级区分。
- 不确定时搜索或直接告诉用户，不要编造。

## 自我保护
- 禁止 rm -rf / 等破坏性命令。
- 被要求执行危险操作时，先警告用户后果并确认。`,
	})
	messages = append(messages, map[string]interface{}{
		"role": "system",
		"content": `你的环境：Qapi (Go) 反向代理网关 + Bot。代理端口 ` + b.cfg.Proxy.Listen + `，` + fmt.Sprintf("%d", len(b.cfg.Proxy.Pool.Upstreams)) + ` 个上游。

你能用的管理接口：
- GET /admin/upstreams → 上游状态
- GET /v1/models → 模型列表
- GET /health → "ok"`,
	})

	for _, h := range history {
		messages = append(messages, map[string]interface{}{"role": h.Role, "content": h.Content})
	}

	tools := []map[string]interface{}{
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "web_search",
				"description": "搜索互联网获取最新信息、技术问题答案、文档、新闻等。用户问你不确定的事时优先调用",
				"parameters": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"query": map[string]string{"type": "string", "description": "搜索关键词，英文搜索效果更好"},
					},
					"required": []string{"query"},
				},
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "update_qapi",
				"description": "在线更新 Qapi 到最新版本（从 GitHub Release 下载并自动重启）",
			},
		},
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
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "run_shell",
				"description": "在服务器执行shell命令。用于部署、安装、文件操作、git、docker、清理内存(sync && echo 3 > /proc/sys/vm/drop_caches)、杀进程(pkill -f xxx)、启停服务等",
				"parameters": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"command": map[string]string{"type": "string", "description": "要执行的shell命令"},
						"workdir":  map[string]string{"type": "string", "description": "工作目录，可选"},
					},
					"required": []string{"command"},
				},
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "read_file",
				"description": "读取服务器上文件内容。查看配置、日志、代码时调用",
				"parameters": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"path": map[string]string{"type": "string", "description": "文件路径"},
					},
					"required": []string{"path"},
				},
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "manage_service",
				"description": "管理系统服务。用户说'启动nginx'、'重启docker'、'看看xx服务状态'时调用",
				"parameters": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"service": map[string]string{"type": "string", "description": "服务名，如 nginx, docker, axonhub, qapi"},
						"action":  map[string]string{"type": "string", "description": "start/stop/restart/status/reload"},
					},
					"required": []string{"service", "action"},
				},
			},
		},
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "write_file",
				"description": "写入或覆盖文件内容。用于修改配置、创建脚本、编辑文件等。会先备份原文件(.bak)",
				"parameters": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"path":    map[string]string{"type": "string", "description": "文件路径，如 /opt/qapi/config.yaml"},
						"content": map[string]string{"type": "string", "description": "要写入的完整文件内容"},
					},
					"required": []string{"path", "content"},
				},
			},
		},
	}

	body := map[string]interface{}{
		"model":       b.cfg.LLM.Model,
		"messages":    messages,
		"tools":       tools,
		"tool_choice": "auto",
		"max_tokens":  b.cfg.LLM.MaxTokens,
	}

	hadToolCall := false
	return b.chatWithTools(body, messages, &hadToolCall)
}

func (b *QQBot) chatWithTools(body map[string]interface{}, messages []map[string]interface{}, hadToolCall *bool) string {
	if len(messages) > 60 {
		return "对话过长，请发送新问题"
	}

	body["stream"] = true
	body["max_tokens"] = b.cfg.LLM.MaxTokens

	payload, _ := json.Marshal(body)
	apiURL := fmt.Sprintf("%s/chat/completions", b.cfg.LLM.BaseURL)
	log.Printf("[LLM] 请求 %s model=%s msgs=%d stream=true", apiURL, b.cfg.LLM.Model, len(messages))

	req, err := http.NewRequest("POST", apiURL, bytes.NewReader(payload))
	if err != nil {
		log.Printf("[LLM] 请求构建失败: %v", err)
		return ""
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+b.cfg.LLM.APIKey)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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

	sr := parseSSEStream(resp.Body)
	if sr.err != nil {
		log.Printf("[LLM] SSE 解析失败: %v", sr.err)
		return ""
	}

	if len(sr.toolCalls) > 0 {
		*hadToolCall = true

		messages = append(messages, map[string]interface{}{
			"role": "assistant",
			"tool_calls": sr.toolCalls,
		})

		toolResults := make([]string, 0, len(sr.toolCalls))
		for _, tc := range sr.toolCallData {
			result := b.executeTool(tc.name, tc.arguments)
			formatted := b.formatToolResult(result)
			toolResults = append(toolResults, formatted)
			messages = append(messages, map[string]interface{}{
				"role":         "tool",
				"tool_call_id": tc.id,
				"content":      formatted,
			})
		}

		nextBody := map[string]interface{}{
			"model":       b.cfg.LLM.Model,
			"messages":    messages,
			"tools":       body["tools"],
			"tool_choice": "auto",
			"max_tokens":  b.cfg.LLM.MaxTokens,
		}
		// 防止无限循环，最多 10 轮工具调用
		maxDepth := 10
		if d, ok := body["_tool_depth"].(int); ok {
			if d >= maxDepth {
				return "工具调用已达上限，请尝试简化你的问题。\n部分结果：\n" + strings.Join(toolResults, "\n---\n")
			}
			nextBody["_tool_depth"] = d + 1
		} else {
			nextBody["_tool_depth"] = 1
		}
		final := b.chatWithTools(nextBody, messages, hadToolCall)
		if final == "" {
			return "工具执行结果：\n" + strings.Join(toolResults, "\n---\n")
		}
		return final
	}

	if sr.content == "" {
		return ""
	}

	if !*hadToolCall && looksLikeFabricatedData(sr.content) {
		messages = append(messages, map[string]interface{}{
			"role": "user", "content": "你必须调用工具获取真实数据，禁止编造。请重新执行。",
		})
		body["messages"] = messages
		return b.chatWithTools(body, messages, hadToolCall)
	}

	return sr.content
}

type sseStreamResult struct {
	content      string
	toolCalls    []map[string]interface{}
	toolCallData []toolCallEntry
	err          error
}

type toolCallEntry struct {
	index     int
	id        string
	name      string
	arguments string
}

func parseSSEStream(body io.Reader) sseStreamResult {
	sr := sseStreamResult{}
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	tcAcc := make(map[int]*toolCallEntry)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}

		var ev struct {
			Choices []struct {
				Delta struct {
					Content   string `json:"content"`
					ToolCalls []struct {
						Index    int    `json:"index"`
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			continue
		}

		for _, choice := range ev.Choices {
			if choice.Delta.Content != "" {
				sr.content += choice.Delta.Content
			}
			for _, tc := range choice.Delta.ToolCalls {
				acc, ok := tcAcc[tc.Index]
				if !ok {
					acc = &toolCallEntry{index: tc.Index}
					tcAcc[tc.Index] = acc
				}
				if tc.ID != "" {
					acc.id = tc.ID
				}
				if tc.Function.Name != "" {
					acc.name = tc.Function.Name
				}
				acc.arguments += tc.Function.Arguments
			}
		}
	}

	sr.err = scanner.Err()

	for _, tc := range tcAcc {
		sr.toolCallData = append(sr.toolCallData, *tc)
		sr.toolCalls = append(sr.toolCalls, map[string]interface{}{
			"id":   tc.id,
			"type": "function",
			"function": map[string]interface{}{
				"name":      tc.name,
				"arguments": tc.arguments,
			},
		})
	}

	return sr
}

func (b *QQBot) formatToolResult(raw string) string {
	// 已经是普通文本，直接返回
	if !strings.HasPrefix(strings.TrimSpace(raw), "{") && !strings.HasPrefix(strings.TrimSpace(raw), "[") {
		// 截断过长输出
		lines := strings.Split(raw, "\n")
		if len(lines) > 60 {
			return strings.Join(lines[:60], "\n") + fmt.Sprintf("\n... 共 %d 行输出，已截断", len(lines))
		}
		return raw
	}

	// 尝试解析上游列表 JSON
	var upstreams []struct {
		URL      string  `json:"url"`
		Key      string  `json:"key"`
		Weight   int     `json:"weight"`
		Healthy  bool    `json:"healthy"`
		Requests float64 `json:"requests"`
		Fails    float64 `json:"fails"`
	}
	if err := json.Unmarshal([]byte(raw), &upstreams); err == nil && len(upstreams) > 0 {
		healthy, total := 0, len(upstreams)
		var lines []string
		for i, u := range upstreams {
			if i >= 50 {
				lines = append(lines, fmt.Sprintf("... 共 %d 个上游", total))
				break
			}
			if u.Healthy {
				healthy++
			}
			status := "异常"
			if u.Healthy {
				status = "正常"
			}
			name := u.URL
			if len(name) > 40 {
				name = "..." + name[len(name)-40:]
			}
			lines = append(lines, fmt.Sprintf("%s | %s | 请求%.0f 失败%.0f", name, status, u.Requests, u.Fails))
		}
		return fmt.Sprintf("%d个上游(%d正常 %d异常):\n%s", total, healthy, total-healthy, strings.Join(lines, "\n"))
	}

	// 模型列表
	var modelsResp struct {
		Data []struct{ ID string `json:"id"` } `json:"data"`
	}
	if err := json.Unmarshal([]byte(raw), &modelsResp); err == nil && len(modelsResp.Data) > 0 {
		lines := make([]string, 0, len(modelsResp.Data)+2)
		for i, m := range modelsResp.Data {
			if i >= 60 {
				lines = append(lines, fmt.Sprintf("... 共 %d 个模型", len(modelsResp.Data)))
				break
			}
			lines = append(lines, "- "+m.ID)
		}
		return fmt.Sprintf("可用模型(%d个):\n%s", len(modelsResp.Data), strings.Join(lines, "\n"))
	}

	// 系统状态/proxy统计/config摘要(JSON对象)
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &obj); err == nil {
		var lines []string
		for k, v := range obj {
			val := fmt.Sprintf("%v", v)
			if len(val) > 80 {
				val = val[:80] + "..."
			}
			lines = append(lines, fmt.Sprintf("%s: %s", k, val))
		}
		return strings.Join(lines, "\n")
	}

	// 无法解析的 JSON，截断返回
	if len(raw) > 3000 {
		return raw[:3000] + "\n... 已截断"
	}
	return raw
}

func (b *QQBot) webSearch(query string) string {
	apiURL := fmt.Sprintf("https://html.duckduckgo.com/html/?q=%s", url.QueryEscape(query))
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return fmt.Sprintf(`{"error":"%v"}`, err)
	}
	req.Header.Set("User-Agent", "QapiBot/1.0")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := b.client.Do(req.WithContext(ctx))
	if err != nil {
		return fmt.Sprintf(`{"error":"搜索失败: %v"}`, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 65536))
	html := string(body)

	// Extract snippets from DuckDuckGo HTML results
	type result struct {
		title, snippet, link string
	}
	var results []result
	parts := strings.Split(html, `class="result"`)
	for _, p := range parts[1:] {
		if len(results) >= 5 {
			break
		}
		// Extract title
		titleStart := strings.Index(p, `class="result__a"`)
		if titleStart == -1 {
			continue
		}
		titleEnd := strings.Index(p[titleStart:], "</a>")
		title := ""
		if titleEnd != -1 {
			raw := p[titleStart : titleStart+titleEnd]
			title = stripTags(raw)
		}

		// Extract snippet
		snipStart := strings.Index(p, `class="result__snippet"`)
		snippet := ""
		if snipStart != -1 {
			snipEnd := strings.Index(p[snipStart:], "</a>")
			if snipEnd == -1 {
				snipEnd = strings.Index(p[snipStart:], "</td>")
			}
			if snipEnd != -1 {
				snippet = stripTags(p[snipStart : snipStart+snipEnd])
			}
		}

		if title != "" || snippet != "" {
			results = append(results, result{title: title, snippet: snippet})
		}
	}

	if len(results) == 0 {
		return "未找到相关搜索结果"
	}

	lines := make([]string, 0, len(results))
	for _, r := range results {
		line := ""
		if r.title != "" {
			line = r.title
			if r.snippet != "" {
				line += ": " + r.snippet
			}
		} else {
			line = r.snippet
		}
		lines = append(lines, strings.TrimSpace(line))
	}
	return fmt.Sprintf("搜索结果（%s）：\n%s", query, strings.Join(lines, "\n"))
}

func stripTags(s string) string {
	result := ""
	inTag := false
	for _, c := range s {
		if c == '<' {
			inTag = true
			continue
		}
		if c == '>' {
			inTag = false
			continue
		}
		if !inTag {
			result += string(c)
		}
	}
	return strings.TrimSpace(result)
}

func looksLikeFabricatedData(content string) bool {
	// Only flag data when it's clearly making up specific server info
	// Don't flag general knowledge answers
	indicators := []string{
		"上游列表如下", "当前代理池中有",
	}
	hasIndicator := false
	for _, kw := range indicators {
		if strings.Contains(content, kw) {
			hasIndicator = true
			break
		}
	}
	if !hasIndicator {
		return false
	}
	// Check if it's listing specific servers without tool calls
	madeUp := []string{
		"SiliconFlow", "硅基流动",
		"OneAPI", "NewAPI", "Ollama", "vLLM",
		"反代地址", "反向代理",
	}
	for _, kw := range madeUp {
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
	case "web_search":
		var p struct{ Query string }
		json.Unmarshal([]byte(args), &p)
		if p.Query == "" {
			return `{"error":"需要搜索关键词"}`
		}
		return b.webSearch(p.Query)

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
		_, err := strconv.Atoi(p.Target)
		if err == nil {
			return b.runCmd("kill", "-"+signal, p.Target)
		}
		return b.runCmd("pkill", "-"+signal, "-f", p.Target)

	case "run_shell":
		var p struct {
			Command string `json:"command"`
			Workdir string `json:"workdir"`
		}
		json.Unmarshal([]byte(args), &p)
		if p.Command == "" {
			return "错误：需要执行的命令"
		}
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "sh", "-c", p.Command)
		if p.Workdir != "" {
			cmd.Dir = p.Workdir
		}
		output, err := cmd.CombinedOutput()
		if err != nil {
			errStr := strings.TrimSpace(string(output))
			if errStr == "" {
				return fmt.Sprintf("命令失败: %v (无更多输出)", err)
			}
			return fmt.Sprintf("命令失败: %v\n%s", err, errStr)
		}
		result := strings.TrimSpace(string(output))
		if result == "" {
			return "命令成功，无输出"
		}
		if len(result) > 3000 {
			return result[:3000] + "\n... 输出已截断"
		}
		return result

	case "read_file":
		var p struct{ Path string }
		json.Unmarshal([]byte(args), &p)
		if p.Path == "" {
			return `{"error":"需要文件路径"}`
		}
		data, err := os.ReadFile(p.Path)
		if err != nil {
			return fmt.Sprintf("读取失败: %v", err)
		}
		if len(data) > 16384 {
			return string(data[:16384]) + "\n... (文件过长，已截断)"
		}
		return string(data)

	case "write_file":
		var p struct {
			Path    string `json:"path"`
			Content string `json:"content"`
		}
		json.Unmarshal([]byte(args), &p)
		if p.Path == "" {
			return "错误：需要文件路径"
		}
		if p.Content == "" {
			return "错误：需要文件内容"
		}
		backup := p.Path + ".bak"
		origData, _ := os.ReadFile(p.Path)
		if len(origData) > 0 {
			if err := os.WriteFile(backup, origData, 0644); err != nil {
				return fmt.Sprintf("备份失败: %v", err)
			}
		}
		if err := os.WriteFile(p.Path, []byte(p.Content), 0644); err != nil {
			return fmt.Sprintf("写入失败: %v", err)
		}
		if len(origData) > 0 {
			return fmt.Sprintf("已写入 %d 字节到 %s (原文件备份到 %s)", len(p.Content), p.Path, backup)
		}
		return fmt.Sprintf("已创建文件 %s (%d 字节)", p.Path, len(p.Content))

	case "manage_service":
		var p struct {
			Service string `json:"service"`
			Action  string `json:"action"`
		}
		json.Unmarshal([]byte(args), &p)
		if p.Service == "" || p.Action == "" {
			return `{"error":"需要服务名和操作"}`
		}
		return b.runCmd("systemctl", p.Action, p.Service)

	case "update_qapi":
		return b.selfUpdate()

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

func (b *QQBot) selfUpdate() string {
	resp, err := http.Post("http://127.0.0.1"+b.cfg.Proxy.Listen+"/admin/update", "application/json", nil)
	if err != nil {
		return fmt.Sprintf("更新失败: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Sprintf("更新失败(%d): %s", resp.StatusCode, string(body))
	}
	return "正在更新到最新版本，2秒后自动重启..."
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

const qqMaxMsgLen = 2000

func (b *QQBot) SendMessage(openID, content string) {
	b.sendMsgSplitted(fmt.Sprintf("%s/v2/users/%s/messages", b.apiBase(), openID), content, "")
}

func (b *QQBot) sendC2C(openID, content, msgID string) {
	b.sendMsgSplitted(fmt.Sprintf("%s/v2/users/%s/messages", b.apiBase(), openID), content, msgID)
}

func (b *QQBot) sendGroup(groupOpenID, content, msgID string) {
	b.sendMsgSplitted(fmt.Sprintf("%s/v2/groups/%s/messages", b.apiBase(), groupOpenID), content, msgID)
}

func (b *QQBot) sendMsgSplitted(url, content, msgID string) {
	if utf8.RuneCountInString(content) <= qqMaxMsgLen {
		b.sendHTTP(url, content, msgID)
		return
	}

	runes := []rune(content)
	parts := (utf8.RuneCountInString(content) + qqMaxMsgLen - 1) / qqMaxMsgLen
	for i := 0; i < len(runes); i += qqMaxMsgLen {
		end := i + qqMaxMsgLen
		if end > len(runes) {
			end = len(runes)
		}
		part := fmt.Sprintf("[%d/%d]\n%s", (i/qqMaxMsgLen)+1, parts, string(runes[i:end]))
		b.sendHTTP(url, part, msgID)
		time.Sleep(200 * time.Millisecond)
	}
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
