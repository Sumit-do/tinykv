package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sumit-do/tinykv"
)

var errNotFound = errors.New("key not found")

// client is a minimal HTTP client for the tinykv server API.
type client struct {
	base string
	hc   *http.Client
}

func newClient(addr string, timeout time.Duration) *client {
	return &client{base: normalizeAddr(addr), hc: &http.Client{Timeout: timeout}}
}

// normalizeAddr accepts ":7700", "host:7700" or a full URL and returns a
// base URL without a trailing slash.
func normalizeAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if !strings.Contains(addr, "://") {
		if strings.HasPrefix(addr, ":") {
			addr = "localhost" + addr
		}
		addr = "http://" + addr
	}
	return strings.TrimRight(addr, "/")
}

func (c *client) keyURL(key string) string {
	return c.base + "/kv/" + url.PathEscape(key)
}

// do performs a request and converts non-2xx responses into errors using
// the server's JSON error body.
func (c *client) do(method, u string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequest(method, u, body)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot reach server at %s: %w", c.base, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return data, nil
	}
	msg := strings.TrimSpace(string(data))
	var je struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(data, &je) == nil && je.Error != "" {
		msg = je.Error
	}
	if resp.StatusCode == http.StatusNotFound && strings.Contains(msg, "not found") {
		return nil, errNotFound
	}
	return nil, fmt.Errorf("server returned %d: %s", resp.StatusCode, msg)
}

func (c *client) get(key string) ([]byte, error) {
	return c.do(http.MethodGet, c.keyURL(key), nil)
}

func (c *client) set(key string, value []byte, ttl time.Duration) error {
	u := c.keyURL(key)
	if ttl > 0 {
		u += "?ttl=" + url.QueryEscape(ttl.String())
	}
	_, err := c.do(http.MethodPut, u, bytes.NewReader(value))
	return err
}

func (c *client) del(key string) error {
	_, err := c.do(http.MethodDelete, c.keyURL(key), nil)
	return err
}

func (c *client) keys(prefix string) ([]string, error) {
	data, err := c.do(http.MethodGet, c.base+"/kv?prefix="+url.QueryEscape(prefix), nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("bad response: %w", err)
	}
	return out.Keys, nil
}

func (c *client) stats() (tinykv.Stats, error) {
	var st tinykv.Stats
	data, err := c.do(http.MethodGet, c.base+"/stats", nil)
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return st, fmt.Errorf("bad response: %w", err)
	}
	return st, nil
}

func (c *client) compact() (before, after tinykv.Stats, err error) {
	data, err := c.do(http.MethodPost, c.base+"/compact", nil)
	if err != nil {
		return before, after, err
	}
	var out struct {
		Before tinykv.Stats `json:"before"`
		After  tinykv.Stats `json:"after"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return before, after, fmt.Errorf("bad response: %w", err)
	}
	return out.Before, out.After, nil
}
