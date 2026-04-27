package main

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
)

type webhookPayload struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d,omitempty"`
	S  int64           `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}

func (b *QQBot) HandleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	// op=13 是回调地址验证
	var p webhookPayload
	if err := json.Unmarshal(body, &p); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if p.Op == 13 {
		b.handleWebhookVerify(w, body)
		return
	}

	// 签名校验（非调试模式）
	if !b.cfg.QQ.Sandbox {
		if !b.verifyWebhookSignature(r, body) {
			http.Error(w, "signature mismatch", http.StatusUnauthorized)
			return
		}
	}

	if p.Op == 0 && p.T != "" {
		log.Printf("[Webhook] 事件: %s", p.T)
		b.dispatch(p.T, p.D)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"code":0}`))
}

func (b *QQBot) handleWebhookVerify(w http.ResponseWriter, body []byte) {
	var p struct {
		Op int `json:"op"`
		D  struct {
			PlainToken string `json:"plain_token"`
			EventTs    string `json:"event_ts"`
		} `json:"d"`
	}
	if json.Unmarshal(body, &p) != nil {
		http.Error(w, "invalid", http.StatusBadRequest)
		return
	}

	seed := b.cfg.QQ.AppSecret
	for len(seed) < ed25519.SeedSize {
		seed = strings.Repeat(seed, 2)
	}
	seed = seed[:ed25519.SeedSize]

	_, privateKey, err := ed25519.GenerateKey(strings.NewReader(seed))
	if err != nil {
		http.Error(w, "keygen error", http.StatusInternalServerError)
		return
	}

	var msgBuf strings.Builder
	msgBuf.WriteString(p.D.EventTs)
	msgBuf.WriteString(p.D.PlainToken)

	sig := hex.EncodeToString(ed25519.Sign(privateKey, []byte(msgBuf.String())))

	resp := map[string]string{
		"plain_token": p.D.PlainToken,
		"signature":   sig,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (b *QQBot) verifyWebhookSignature(r *http.Request, body []byte) bool {
	botSig := r.Header.Get("X-Signature-Ed25519")
	if botSig == "" {
		return false
	}
	timestamp := r.Header.Get("X-Signature-Timestamp")
	if timestamp == "" {
		return false
	}

	// 用 AppSecret 派生 ED25519 私钥，验证签名
	seed := b.cfg.QQ.AppSecret
	for len(seed) < ed25519.SeedSize {
		seed = strings.Repeat(seed, 2)
	}
	seed = seed[:ed25519.SeedSize]
	_, privateKey, err := ed25519.GenerateKey(strings.NewReader(seed))
	if err != nil {
		return false
	}
	pubKey := privateKey.Public().(ed25519.PublicKey)

	msg := timestamp + string(body)
	hash := sha256.Sum256([]byte(msg))

	sigBytes, err := hex.DecodeString(botSig)
	if err != nil {
		return false
	}

	return ed25519.Verify(pubKey, hash[:], sigBytes)
}

// Webhook 被动回复：不需要 sendHTTP，直接在 webhook handler 返回 body 即可
// 但 QQ Bot webhook 模式下回复要走主动消息 API，保留现有 sendC2C 逻辑
