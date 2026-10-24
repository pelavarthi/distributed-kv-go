// Command server runs the store behind the HTTP API on :8080.
package main

import (
	"log"
	"net/http"

	"github.com/pelavarthi/distributed-kv-go/internal/api"
	"github.com/pelavarthi/distributed-kv-go/internal/store"
)

func main() {
	s := store.New()
	defer s.Close()
	log.Println("listening on :8080")
	if err := http.ListenAndServe(":8080", api.New(s)); err != nil {
		log.Fatal(err)
	}
}
