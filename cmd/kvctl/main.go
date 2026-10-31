// Command kvctl talks to a running server: get, set, delete, list, members, leader.
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
		fmt.Fprintln(os.Stderr, "usage: kvctl [-addr URL] [-ttl SECONDS] <get|set|delete|list|members|leader|catchup> [key] [value]")
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
	case "leader":
		var leader client.Leader
		leader, err = c.Leader()
		if err == nil {
			fmt.Printf("%s term %d\n", leader.ID, leader.Term)
		}
	case "catchup":
		var res client.CatchupResult
		res, err = c.Catchup()
		if err == nil {
			fmt.Printf("synced %d followers, commit index %d\n", res.Synced, res.CommitIndex)
		}
	case "members":
		var members []client.Member
		members, err = c.Members()
		if err == nil {
			for _, m := range members {
				status := "dead"
				if m.Alive {
					status = "alive"
				}
				fmt.Printf("%s %s %s\n", m.ID, m.Addr, status)
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
