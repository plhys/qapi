package main

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	QQ struct {
		AppID     string `yaml:"app_id"`
		AppSecret string `yaml:"app_secret"`
		Sandbox   bool   `yaml:"sandbox"`
	} `yaml:"qq"`

	LLM struct {
		Provider  string `yaml:"provider"`
		APIKey    string `yaml:"api_key"`
		BaseURL   string `yaml:"base_url"`
		Model     string `yaml:"model"`
		MaxTokens int    `yaml:"max_tokens"`
	} `yaml:"llm"`

	Proxy struct {
		Listen   string     `yaml:"listen"`
		Pool     ProxyPool  `yaml:"pool"`
		Routes   []PathRoute `yaml:"routes"`
	} `yaml:"proxy"`

	Notify struct {
		QQUser string `yaml:"qq_user"`
	} `yaml:"notify"`

	Memory struct {
		Path     string `yaml:"path"`
		MaxTurns int    `yaml:"max_turns"`
	} `yaml:"memory"`
}

type ProxyPool struct {
	Upstreams  []Upstream `yaml:"upstreams"`
	Fallback   []string   `yaml:"fallback"`
	MaxRetries int        `yaml:"max_retries"`
}

type PathRoute struct {
	Path    string     `yaml:"path"`
	Pool    ProxyPool  `yaml:"pool"`
}

type Upstream struct {
	URL    string `yaml:"url"`
	Key    string `yaml:"key"`
	Weight int    `yaml:"weight"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	c := &Config{}
	if err := yaml.Unmarshal(data, c); err != nil {
		return nil, err
	}

	if c.Proxy.Listen == "" {
		c.Proxy.Listen = ":8080"
	}
	if c.Memory.MaxTurns == 0 {
		c.Memory.MaxTurns = 20
	}
	if c.LLM.Provider == "" {
		c.LLM.Provider = "openai"
	}
	if c.LLM.Model == "" {
		c.LLM.Model = "gpt-4o"
	}
	if c.LLM.MaxTokens == 0 {
		c.LLM.MaxTokens = 4096
	}

	setDefaults := func(p *ProxyPool) {
		if p.MaxRetries == 0 {
			p.MaxRetries = 1
		}
		for i := range p.Upstreams {
			if p.Upstreams[i].Weight == 0 {
				p.Upstreams[i].Weight = 1
			}
		}
	}

	setDefaults(&c.Proxy.Pool)
	for i := range c.Proxy.Routes {
		setDefaults(&c.Proxy.Routes[i].Pool)
	}

	return c, nil
}
