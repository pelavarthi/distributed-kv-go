// Command kvgrpc talks to a node's gRPC service: get, set,
// delete, list. It mirrors kvctl, for clients that speak gRPC
// instead of HTTP.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/pelavarthi/distributed-kv-go/internal/grpckv"
)

func main() {
	addr := flag.String("addr", "localhost:9090", "gRPC server address")
	ttl := flag.Int("ttl", 0, "ttl in seconds for set")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: kvgrpc [-addr HOST:PORT] [-ttl SECONDS] <get|set|delete|list> [key] [value]")
		os.Exit(2)
	}
	c, err := grpckv.Dial(*addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	switch args[0] {
	case "get":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: kvgrpc get <key>")
			os.Exit(2)
		}
		value, err := c.Get(ctx, args[1])
		if err == nil {
			fmt.Println(value)
		} else {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "set":
		if len(args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: kvgrpc set <key> <value>")
			os.Exit(2)
		}
		if err := c.Set(ctx, args[1], args[2], *ttl); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "delete":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: kvgrpc delete <key>")
			os.Exit(2)
		}
		if err := c.Delete(ctx, args[1]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "list":
		keys, err := c.List(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		for _, key := range keys {
			fmt.Println(key)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", args[0])
		os.Exit(2)
	}
}
