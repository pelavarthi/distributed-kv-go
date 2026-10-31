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

// Member is one node from the /cluster/members endpoint.
type Member struct {
	ID    string `json:"id"`
	Addr  string `json:"addr"`
	Alive bool   `json:"alive"`
}

func (c *Client) Members() ([]Member, error) {
	req, err := http.NewRequest(http.MethodGet, c.base+"/cluster/members", nil)
	if err != nil {
		return nil, err
	}
	body, _, err := c.do(req)
	if err != nil {
		return nil, err
	}
	var out struct {
		Members []Member `json:"members"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return out.Members, nil
}

// Leader is one node's answer from the /cluster/leader endpoint.
type Leader struct {
	ID   string `json:"leader"`
	Term int    `json:"term"`
}

func (c *Client) Leader() (Leader, error) {
	req, err := http.NewRequest(http.MethodGet, c.base+"/cluster/leader", nil)
	if err != nil {
		return Leader{}, err
	}
	body, _, err := c.do(req)
	if err != nil {
		return Leader{}, err
	}
	var out Leader
	if err := json.Unmarshal(body, &out); err != nil {
		return Leader{}, err
	}
	return out, nil
}

// CatchupResult is the leader's answer from /cluster/catchup.
type CatchupResult struct {
	Synced      int    `json:"synced_followers"`
	CommitIndex uint64 `json:"commit_index"`
}

// Catchup asks the leader to push missing log entries to its
// followers and re-drive any uncommitted tail.
func (c *Client) Catchup() (CatchupResult, error) {
	req, err := http.NewRequest(http.MethodPost, c.base+"/cluster/catchup", nil)
	if err != nil {
		return CatchupResult{}, err
	}
	body, _, err := c.do(req)
	if err != nil {
		return CatchupResult{}, err
	}
	var out CatchupResult
	if err := json.Unmarshal(body, &out); err != nil {
		return CatchupResult{}, err
	}
	return out, nil
}

func (c *Client) Health() error {
	req, err := http.NewRequest(http.MethodGet, c.base+"/health", nil)
	if err != nil {
		return err
	}
	_, _, err = c.do(req)
	return err
}
