// Command kvctl talks to a running server: get, set, delete, list.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/pelavarthi/distributed-kv-go/internal/client"
)

func main() {
	addr := flag.String("addr", "http://localhost:8080", "server address")
	ttl := flag.Int("ttl", 0, "ttl in seconds for set")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl [-addr URL] [-ttl SECONDS] <get|set|delete|list> [key] [value]")
		os.Exit(2)
	}
	c := client.New(*addr)
	var err error
	switch args[0] {
	case "get":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: kvctl get <key>")
			os.Exit(2)
		}
		var value string
		value, err = c.Get(args[1])
		if err == nil {
			fmt.Println(value)
		}
	case "set":
		if len(args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: kvctl set <key> <value>")
			os.Exit(2)
		}
		err = c.Set(args[1], args[2], *ttl)
	case "delete":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: kvctl delete <key>")
			os.Exit(2)
		}
		err = c.Delete(args[1])
	case "list":
		var keys []string
		keys, err = c.List()
		if err == nil {
			for _, key := range keys {
				fmt.Println(key)
			}
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", args[0])
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
