// Package client is a small HTTP client for the KV API, shared by
// the CLI so the command itself stays thin.
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	base string
	http *http.Client
}

func New(base string) *Client {
	return &Client{
		base: strings.TrimSuffix(base, "/"),
		http: &http.Client{Timeout: 10 * time.Second},
	}
}

type apiError struct {
	Error string `json:"error"`
}

func (c *Client) do(req *http.Request) ([]byte, int, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if resp.StatusCode >= 400 {
		var apiErr apiError
		if json.Unmarshal(body, &apiErr) == nil && apiErr.Error != "" {
			return nil, resp.StatusCode, fmt.Errorf("%s", apiErr.Error)
		}
		return nil, resp.StatusCode, fmt.Errorf("server returned %d", resp.StatusCode)
	}
	return body, resp.StatusCode, nil
}

func (c *Client) Set(key, value string, ttlSeconds int) error {
	body, _ := json.Marshal(map[string]any{"value": value, "ttl_seconds": ttlSeconds})
	req, err := http.NewRequest(http.MethodPut, c.base+"/kv/"+url.PathEscape(key), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	_, _, err = c.do(req)
	return err
}

func (c *Client) Get(key string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, c.base+"/kv/"+url.PathEscape(key), nil)
	if err != nil {
		return "", err
	}
	body, _, err := c.do(req)
	if err != nil {
		return "", err
	}
	var out struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", err
	}
	return out.Value, nil
}

func (c *Client) Delete(key string) error {
	req, err := http.NewRequest(http.MethodDelete, c.base+"/kv/"+url.PathEscape(key), nil)
	if err != nil {
		return err
	}
	_, _, err = c.do(req)
	return err
}

func (c *Client) List() ([]string, error) {
	req, err := http.NewRequest(http.MethodGet, c.base+"/kv", nil)
	if err != nil {
		return nil, err
	}
	body, _, err := c.do(req)
	if err != nil {
		return nil, err
	}
	var out struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return out.Keys, nil
}

func (c *Client) Health() error {
	req, err := http.NewRequest(http.MethodGet, c.base+"/health", nil)
	if err != nil {
		return err
	}
	_, _, err = c.do(req)
	return err
}
