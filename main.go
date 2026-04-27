package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Time    string `json:"time"`
}

type MemoryStore struct {
	mu       sync.RWMutex
	data     map[string][]Message
	cfg      *Config
	filePath string
}

func NewMemoryStore(cfg *Config) *MemoryStore {
	ms := &MemoryStore{
		data:     make(map[string][]Message),
		cfg:      cfg,
		filePath: cfg.Memory.Path,
	}
	ms.load()
	return ms
}

func (m *MemoryStore) Add(chatID, role, content string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[chatID] = append(m.data[chatID], Message{
		Role:    role,
		Content: content,
		Time:    time.Now().Format(time.RFC3339),
	})

	if len(m.data[chatID]) > m.cfg.Memory.MaxTurns*2 {
		m.data[chatID] = m.data[chatID][2:]
	}
}

func (m *MemoryStore) Get(chatID string) []Message {
	m.mu.RLock()
	defer m.mu.RUnlock()

	msgs := m.data[chatID]
	max := m.cfg.Memory.MaxTurns * 2
	if len(msgs) <= max {
		return msgs
	}
	return msgs[len(msgs)-max:]
}

func (m *MemoryStore) load() {
	if m.filePath == "" {
		return
	}
	data, err := os.ReadFile(m.filePath)
	if err != nil {
		return
	}
	json.Unmarshal(data, &m.data)
	log.Printf("[Memory] 从 %s 加载了 %d 个对话", m.filePath, len(m.data))
}

func (m *MemoryStore) save() {
	if m.filePath == "" {
		return
	}
	m.mu.RLock()
	data, _ := json.MarshalIndent(m.data, "", "  ")
	m.mu.RUnlock()

	dir := filepath.Dir(m.filePath)
	os.MkdirAll(dir, 0755)

	tmp := m.filePath + ".tmp"
	os.WriteFile(tmp, data, 0644)
	os.Rename(tmp, m.filePath)
}

func runSetup() {
	reader := bufio.NewReader(os.Stdin)
	read := func(prompt, def string) string {
		if def != "" {
			fmt.Printf("%s [%s]: ", prompt, def)
		} else {
			fmt.Printf("%s: ", prompt)
		}
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "" {
			return def
		}
		return input
	}

	fmt.Println("╔══════════════════════════════════╗")
	fmt.Println("║   Qapi · 一键配置向导            ║")
	fmt.Println("╚══════════════════════════════════╝")
	fmt.Println()

	qqAppID := read("QQ Bot AppID (在 https://q.qq.com 获取)", "")
	qqSecret := read("QQ Bot AppSecret", "")
	sandbox := strings.ToLower(read("沙箱模式? (y/n)", "y"))
	sandboxVal := "true"
	if sandbox == "n" || sandbox == "no" {
		sandboxVal = "false"
	}

	llmURL := read("LLM API 地址 (例: https://api.openai.com/v1)", "")
	llmKey := read("LLM API Key", "")
	llmModel := read("LLM 模型名", "gpt-4o")

	proxyPort := read("代理监听端口", "8080")

	fmt.Println()
	fmt.Println("配置 LLM API 代理上游 (自动轮询):")
	var upstreams []string
	for i := 1; i <= 5; i++ {
		uURL := read(fmt.Sprintf("  上游 #%d URL (留空结束)", i), "")
		if uURL == "" {
			break
		}
		uKey := read(fmt.Sprintf("  上游 #%d API Key", i), "")
		uWeight := read(fmt.Sprintf("  上游 #%d 权重", i), "1")
		upstreams = append(upstreams, fmt.Sprintf("    - url: \"%s\"\n      key: \"%s\"\n      weight: %s", uURL, uKey, uWeight))
	}

	configPath := read("保存到", "config.yaml")

	yaml := fmt.Sprintf(`# Qapi 配置文件
# ================

# QQ Bot 配置
qq:
  app_id: "%s"
  app_secret: "%s"
  sandbox: %s

# LLM 配置（Bot 对话用）
llm:
  provider: "openai"
  api_key: "%s"
  base_url: "%s"
  model: "%s"

# API 代理配置
proxy:
  listen: ":%s"
  pool:
    upstreams:
%s
    max_retries: 2

# 对话记忆
memory:
  path: "./data/memory.json"
  max_turns: 20
`, qqAppID, qqSecret, sandboxVal,
		llmKey, llmURL, llmModel, proxyPort,
		strings.Join(upstreams, "\n"))

	os.WriteFile(configPath, []byte(yaml), 0644)
	fmt.Printf("\n✅ 配置已写入 %s\n", configPath)
	fmt.Println("现在运行: ./qapi", configPath)
}

func runServer(configPath string) {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("加载配置失败 %s: %v", configPath, err)
	}

	log.Printf("启动中... QQ=%s LLM=%s @ %s Proxy=%s",
		cfg.QQ.AppID, cfg.LLM.Model, cfg.LLM.BaseURL, cfg.Proxy.Listen)

	memory := NewMemoryStore(cfg)
	bot := NewQQBot(cfg, memory)

	var notifyFn func(string)
	if cfg.Notify.QQUser != "" {
		notifyFn = func(msg string) {
			bot.SendMessage(cfg.Notify.QQUser, msg)
		}
	}

	proxy := NewProxy(cfg, notifyFn)
	proxy.webhookHandler = bot.HandleWebhook

	done := make(chan struct{})

	go bot.Run()

	go func() {
		if err := proxy.ListenAndServe(); err != nil {
			log.Printf("[Proxy] 服务退出: %v", err)
		}
		close(done)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Qapi 已启动")

	select {
	case sig := <-sigCh:
		log.Printf("收到信号 %v, 优雅退出中...", sig)
	case <-done:
	}

	proxy.Shutdown()
	bot.Shutdown()
	memory.save()
	log.Printf("Qapi 已退出")
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix("[Qapi] ")

	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "setup", "--setup":
			runSetup()
			return
		case "version", "-v", "--version":
			fmt.Println("qapi version 0.2")
			return
		}
	}

	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("加载配置失败 %s: %v", configPath, err)
	}

	if cfg.QQ.AppID == "" || cfg.QQ.AppID == "YOUR_QQ_APP_ID" {
		log.Fatalf("QQ AppID 未配置，请编辑 %s 或在 https://q.qq.com 创建机器人后用 ./qapi setup 配置", configPath)
	}
	if cfg.QQ.AppSecret == "" || cfg.QQ.AppSecret == "YOUR_QQ_APP_SECRET" {
		log.Fatalf("QQ AppSecret 未配置，请编辑 %s 或在 https://q.qq.com 创建机器人后用 ./qapi setup 配置", configPath)
	}

	runServer(configPath)
}
