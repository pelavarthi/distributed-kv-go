package grpckv

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pelavarthi/distributed-kv-go/internal/store"
)

func newTestClient(t *testing.T) *Client {
	t.Helper()
	s := store.New()
	t.Cleanup(s.Close)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	Register(srv, s)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	c, err := Dial(lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestGRPCSetGetDeleteList(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	if err := c.Set(ctx, "name", "pranav", 0); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, "name")
	if err != nil {
		t.Fatal(err)
	}
	if got != "pranav" {
		t.Fatalf("Get = %q, want pranav", got)
	}

	if err := c.Set(ctx, "a", "1", 0); err != nil {
		t.Fatal(err)
	}
	keys, err := c.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "name" {
		t.Fatalf("List = %v, want [a name]", keys)
	}

	if err := c.Delete(ctx, "name"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "name"); status.Code(err) != codes.NotFound {
		t.Fatalf("Get deleted = %v, want NotFound", err)
	}
	if err := c.Delete(ctx, "name"); status.Code(err) != codes.NotFound {
		t.Fatalf("Delete missing = %v, want NotFound", err)
	}
}

func TestGRPCRejectsBadRequests(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	if _, err := c.Get(ctx, ""); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Get empty = %v, want InvalidArgument", err)
	}
	if err := c.Set(ctx, "k", "v", -1); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Set negative TTL = %v, want InvalidArgument", err)
	}
}
