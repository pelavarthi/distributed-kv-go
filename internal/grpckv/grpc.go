// Package grpckv serves the store over gRPC, as a second transport
// beside the HTTP API. A client that already speaks gRPC can use
// it without learning the HTTP routes, and a node can serve both:
// the HTTP API stays the replicated-write path, while gRPC reads
// serve from the same store, so a replicated write is visible
// over gRPC as soon as it commits.
//
// Messages are encoded as JSON instead of protobuf. That keeps
// the service free of a code-generation step: the wire format is
// still gRPC (HTTP/2 framing, one method per RPC), only the
// payload codec differs, and a protobuf codec can replace it
// later without changing the service shape.
package grpckv

import (
	"context"
	"encoding/json"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"

	"github.com/pelavarthi/distributed-kv-go/internal/store"
)

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                       { return "json" }

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

// Requests and responses for the KV service. TTLSeconds is 0 for
// no expiry, matching the HTTP API's ttl_seconds field.
type GetRequest struct {
	Key string `json:"key"`
}

type GetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type SetRequest struct {
	Key        string `json:"key"`
	Value      string `json:"value"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

type SetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DeleteRequest struct {
	Key string `json:"key"`
}

type DeleteResponse struct {
	Key string `json:"key"`
}

type ListRequest struct{}

type ListResponse struct {
	Keys []string `json:"keys"`
}

// KVServer is the service interface the handlers dispatch to.
type KVServer interface {
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Set(context.Context, *SetRequest) (*SetResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
	List(context.Context, *ListRequest) (*ListResponse, error)
}

// Server is a KVServer backed by one node's store. A write here
// touches only this node's memory: it is not written to the
// WAL and not replicated, so replicated writes still go through
// the HTTP leader path, and a gRPC Set on a follower does not
// reach the rest of the cluster.
type Server struct {
	store *store.Store
}

// NewServer builds a gRPC server for s.
func NewServer(s *store.Store) *Server {
	return &Server{store: s}
}

func (s *Server) Get(_ context.Context, req *GetRequest) (*GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	value, ok := s.store.Get(req.Key)
	if !ok {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	return &GetResponse{Key: req.Key, Value: value}, nil
}

func (s *Server) Set(_ context.Context, req *SetRequest) (*SetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	if req.TTLSeconds < 0 {
		return nil, status.Error(codes.InvalidArgument, "ttl_seconds must not be negative")
	}
	s.store.Set(req.Key, req.Value, time.Duration(req.TTLSeconds)*time.Second)
	return &SetResponse{Key: req.Key, Value: req.Value}, nil
}

func (s *Server) Delete(_ context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	if !s.store.Delete(req.Key) {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	return &DeleteResponse{Key: req.Key}, nil
}

func (s *Server) List(_ context.Context, _ *ListRequest) (*ListResponse, error) {
	return &ListResponse{Keys: s.store.Keys()}, nil
}

func handlerFor[Req any, Resp any](call func(KVServer, context.Context, *Req) (*Resp, error)) func(any, context.Context, func(any) error, grpc.UnaryServerInterceptor) (any, error) {
	return func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		req := new(Req)
		if err := dec(req); err != nil {
			return nil, err
		}
		return call(srv.(KVServer), ctx, req)
	}
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "kv.KV",
	HandlerType: (*KVServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Get", Handler: handlerFor(KVServer.Get)},
		{MethodName: "Set", Handler: handlerFor(KVServer.Set)},
		{MethodName: "Delete", Handler: handlerFor(KVServer.Delete)},
		{MethodName: "List", Handler: handlerFor(KVServer.List)},
	},
	Streams: []grpc.StreamDesc{},
}

// Register adds the KV service to srv.
func Register(srv *grpc.Server, s *store.Store) {
	srv.RegisterService(&serviceDesc, NewServer(s))
}

// Client talks to a KV gRPC service.
type Client struct {
	conn *grpc.ClientConn
}

// Dial connects to the service at target, for example
// "localhost:9090". The connection is plaintext, like the HTTP
// API, and is not encrypted.
func Dial(target string) (*Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) invoke(ctx context.Context, method string, req, resp any) error {
	return c.conn.Invoke(ctx, "/kv.KV/"+method, req, resp, grpc.CallContentSubtype("json"))
}

// Get returns the value for key, or a NotFound error.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	var out GetResponse
	if err := c.invoke(ctx, "Get", &GetRequest{Key: key}, &out); err != nil {
		return "", err
	}
	return out.Value, nil
}

// Set stores value under key, with a TTL in seconds (0 = none).
func (c *Client) Set(ctx context.Context, key, value string, ttlSeconds int) error {
	var out SetResponse
	return c.invoke(ctx, "Set", &SetRequest{Key: key, Value: value, TTLSeconds: ttlSeconds}, &out)
}

// Delete removes key, or returns NotFound.
func (c *Client) Delete(ctx context.Context, key string) error {
	var out DeleteResponse
	return c.invoke(ctx, "Delete", &DeleteRequest{Key: key}, &out)
}

// List returns the node's keys in sorted order.
func (c *Client) List(ctx context.Context) ([]string, error) {
	var out ListResponse
	if err := c.invoke(ctx, "List", &ListRequest{}, &out); err != nil {
		return nil, err
	}
	return out.Keys, nil
}
