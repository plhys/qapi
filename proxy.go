package main

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type UpstreamState struct {
	URL     string
	Key     string
	Weight  int
	Healthy bool
}

type UpstreamStats struct {
	Requests  uint64 `json:"requests"`
	Fails     uint64 `json:"fails"`
	LastCheck string `json:"last_check"`
}

type routeEntry struct {
	path string
	pool *UpstreamPool
}

type Proxy struct {
	cfg            *Config
	mainPool       *UpstreamPool
	routes         []routeEntry
	notifyFn       func(string)
	webhookHandler http.HandlerFunc
}

type UpstreamPool struct {
	mu          sync.Mutex
	upstreams   atomic.Value
	proxies     sync.Map
	fallback    []string
	maxRetries  int
	stats       map[string]*UpstreamStats
	healthDone  chan struct{}
}

func newUpstreamPool(cfg ProxyPool) *UpstreamPool {
	p := &UpstreamPool{
		fallback:   cfg.Fallback,
		maxRetries: cfg.MaxRetries,
		stats:      make(map[string]*UpstreamStats),
		healthDone: make(chan struct{}),
	}

	states := make([]UpstreamState, len(cfg.Upstreams))
	for i, u := range cfg.Upstreams {
		states[i] = UpstreamState{URL: u.URL, Key: u.Key, Weight: u.Weight, Healthy: true}
		p.stats[u.URL] = &UpstreamStats{}
	}
	p.upstreams.Store(states)
	go p.healthCheckLoop()
	return p
}

func (p *UpstreamPool) stop() {
	close(p.healthDone)
}

func NewProxy(cfg *Config, notifyFn func(string)) *Proxy {
	prx := &Proxy{
		cfg:      cfg,
		mainPool: newUpstreamPool(cfg.Proxy.Pool),
		notifyFn: notifyFn,
	}
	for _, r := range cfg.Proxy.Routes {
		prx.routes = append(prx.routes, routeEntry{
			path: strings.TrimSuffix(r.Path, "/"),
			pool: newUpstreamPool(r.Pool),
		})
	}
	return prx
}

func (p *Proxy) Shutdown() {
	p.mainPool.stop()
	for _, r := range p.routes {
		r.pool.stop()
	}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pool := p.pickPool(r.URL.Path)
	up := p.pickUpstream(pool)
	if up == nil {
		http.Error(w, `{"error":"no healthy upstream"}`, http.StatusServiceUnavailable)
		return
	}

	pool.mu.Lock()
	pool.stats[up.URL].Requests++
	pool.mu.Unlock()

	target, err := url.Parse(up.URL)
	if err != nil {
		log.Printf("[Proxy] 上游 URL 解析失败 %s: %v", up.URL, err)
		http.Error(w, `{"error":"upstream url parse error"}`, http.StatusBadGateway)
		return
	}

	rp := p.getOrCreateProxy(up.URL, target)
	rp.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.URL.Path = overlapPath(target.Path, r.URL.Path)
		req.URL.RawQuery = r.URL.RawQuery
		req.Host = target.Host
		if up.Key != "" {
			req.Header.Set("Authorization", "Bearer "+up.Key)
		}
		delete(req.Header, "X-Forwarded-For")
		delete(req.Header, "X-Forwarded-Proto")
		delete(req.Header, "X-Real-Ip")
		delete(req.Header, "Forwarded")
		delete(req.Header, "X-Forwarded-Host")
	}

	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[Proxy] 上游错误 %s: %v", up.URL, err)
		p.markDown(pool, up.URL)
		http.Error(w, `{"error":"upstream unavailable"}`, http.StatusBadGateway)
	}

	rp.ServeHTTP(w, r)
}

func (p *Proxy) getOrCreateProxy(upstreamURL string, target *url.URL) *httputil.ReverseProxy {
	if cached, ok := p.mainPool.proxies.Load(upstreamURL); ok {
		return cached.(*httputil.ReverseProxy)
	}
	// also check route pools
	for _, r := range p.routes {
		if cached, ok := r.pool.proxies.Load(upstreamURL); ok {
			return cached.(*httputil.ReverseProxy)
		}
	}

	rp := httputil.NewSingleHostReverseProxy(target)
	rp.FlushInterval = 100 * time.Millisecond
	rp.ModifyResponse = func(resp *http.Response) error {
		resp.Header.Del("Server")
		resp.Header.Del("Via")
		resp.Header.Del("X-Powered-By")
		resp.Header.Del("X-Request-Id")
		resp.Header.Del("X-Runtime")
		resp.Header.Del("X-Ratelimit-Remaining")
		resp.Header.Del("X-Ratelimit-Limit")
		resp.Header.Del("X-Ratelimit-Reset")
		return nil
	}
	return rp
}

func (p *Proxy) pickPool(path string) *UpstreamPool {
	for _, r := range p.routes {
		if strings.HasPrefix(path, r.path) {
			return r.pool
		}
	}
	return p.mainPool
}

func (p *Proxy) pickUpstream(pool *UpstreamPool) *UpstreamState {
	states := pool.upstreams.Load().([]UpstreamState)
	healthy := make([]*UpstreamState, 0)
	for i := range states {
		if states[i].Healthy {
			healthy = append(healthy, &states[i])
		}
	}
	if len(healthy) == 0 {
		return nil
	}

	totalWeight := 0
	for _, s := range healthy {
		totalWeight += s.Weight
	}
	r := rand.Intn(totalWeight)
	for _, s := range healthy {
		if r < s.Weight {
			return s
		}
		r -= s.Weight
	}
	return healthy[0]
}

func (p *Proxy) markDown(pool *UpstreamPool, url string) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	states := pool.upstreams.Load().([]UpstreamState)
	wasHealthy := false
	for i, s := range states {
		if s.URL == url {
			wasHealthy = s.Healthy
			states[i].Healthy = false
			if st, ok := pool.stats[url]; ok {
				st.Fails++
			}
			break
		}
	}
	pool.upstreams.Store(states)

	if wasHealthy && p.notifyFn != nil {
		msg := "⚠️ 上游故障\n" +
			"URL: " + url + "\n" +
			"时间: " + time.Now().Format("2006-01-02 15:04:05") + "\n" +
			"状态: 已自动摘除，30秒后重检"
		go p.notifyFn(msg)
	}
}

func (p *UpstreamPool) healthCheckLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-p.healthDone:
			return
		}

		p.mu.Lock()
		states := p.upstreams.Load().([]UpstreamState)
		p.mu.Unlock()

		for _, s := range states {
			go func(u UpstreamState) {
				resp, err := http.Get(u.URL + "/health")
				p.mu.Lock()
				defer p.mu.Unlock()

				now := time.Now().Format(time.RFC3339)
				if st, ok := p.stats[u.URL]; ok {
					st.LastCheck = now
				}

				current := p.upstreams.Load().([]UpstreamState)
				for i := range current {
					if current[i].URL == u.URL {
						recovered := !current[i].Healthy && err == nil && resp != nil && resp.StatusCode < 500
						current[i].Healthy = err == nil && resp != nil && resp.StatusCode < 500
						if recovered {
							log.Printf("[Proxy] 上游恢复: %s", u.URL)
						}
						break
					}
				}
				p.upstreams.Store(current)
			}(s)
		}
	}
}

func (p *Proxy) ServeAdmin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case "GET":
		p.adminList(p.mainPool, w)
	case "POST":
		p.adminAdd(p.mainPool, w, r)
	case "DELETE":
		p.adminRemove(p.mainPool, w, r)
	default:
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

func (p *Proxy) adminList(pool *UpstreamPool, w http.ResponseWriter) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	type item struct {
		URL      string `json:"url"`
		Key      string `json:"key"`
		Weight   int    `json:"weight"`
		Healthy  bool   `json:"healthy"`
		Requests uint64 `json:"requests"`
		Fails    uint64 `json:"fails"`
	}

	states := pool.upstreams.Load().([]UpstreamState)
	items := make([]item, len(states))
	for i, s := range states {
		items[i] = item{URL: s.URL, Weight: s.Weight, Healthy: s.Healthy}
		if st, ok := pool.stats[s.URL]; ok {
			items[i].Requests = st.Requests
			items[i].Fails = st.Fails
		}
		if s.Key != "" {
			items[i].Key = maskKey(s.Key)
		}
	}
	json.NewEncoder(w).Encode(items)
}

func (p *Proxy) adminAdd(pool *UpstreamPool, w http.ResponseWriter, r *http.Request) {
	var add struct {
		URL    string `json:"url"`
		Key    string `json:"key"`
		Weight int    `json:"weight"`
	}
	if json.NewDecoder(r.Body).Decode(&add) != nil || add.URL == "" {
		http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
		return
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	states := pool.upstreams.Load().([]UpstreamState)
	for _, s := range states {
		if s.URL == add.URL && s.Key == add.Key {
			http.Error(w, `{"error":"already exists"}`, http.StatusConflict)
			return
		}
	}
	if add.Weight <= 0 {
		add.Weight = 1
	}
	newState := UpstreamState{URL: add.URL, Key: add.Key, Weight: add.Weight, Healthy: true}
	pool.upstreams.Store(append(states, newState))
	pool.stats[add.URL] = &UpstreamStats{}
	log.Printf("[Proxy] 添加上游: %s", add.URL)
	json.NewEncoder(w).Encode(map[string]string{"status": "added"})
}

func (p *Proxy) adminRemove(pool *UpstreamPool, w http.ResponseWriter, r *http.Request) {
	var remove struct {
		URL string `json:"url"`
	}
	if json.NewDecoder(r.Body).Decode(&remove) != nil || remove.URL == "" {
		http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
		return
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	states := pool.upstreams.Load().([]UpstreamState)
	found := false
	var new []UpstreamState
	for _, s := range states {
		if s.URL == remove.URL {
			found = true
			continue
		}
		new = append(new, s)
	}
	if !found {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	pool.upstreams.Store(new)
	delete(pool.stats, remove.URL)
	log.Printf("[Proxy] 移除上游: %s", remove.URL)
	json.NewEncoder(w).Encode(map[string]string{"status": "removed"})
}

func (p *Proxy) ListenAndServe() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/", p.ServeHTTP)
	mux.HandleFunc("/v2/", p.ServeHTTP)
	mux.HandleFunc("/admin/upstreams", p.ServeAdmin)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok")
	})
	if p.webhookHandler != nil {
		mux.HandleFunc("/qq/callback", p.webhookHandler)
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "qapi proxy", http.StatusNotFound)
	})

	srv := &http.Server{
		Addr:         p.cfg.Proxy.Listen,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 600 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("[Proxy] 监听 %s", p.cfg.Proxy.Listen)
	return srv.ListenAndServe()
}

func maskKey(k string) string {
	if len(k) <= 8 {
		return "****"
	}
	return k[:4] + "****" + k[len(k)-4:]
}

func overlapPath(base, req string) string {
	base = strings.TrimRight(base, "/")
	if base == "" {
		return req
	}
	idx := strings.LastIndex(base, "/")
	lastSeg := base
	prefix := ""
	if idx >= 0 {
		lastSeg = base[idx+1:]
		prefix = base[:idx]
	}
	trimmed := strings.TrimLeft(req, "/")
	if strings.HasPrefix(trimmed, lastSeg+"/") || trimmed == lastSeg {
		remaining := strings.TrimPrefix(trimmed, lastSeg)
		remaining = strings.TrimLeft(remaining, "/")
		if remaining == "" {
			return prefix + "/" + lastSeg
		}
		return prefix + "/" + lastSeg + "/" + remaining
	}
	return base + "/" + trimmed
}

