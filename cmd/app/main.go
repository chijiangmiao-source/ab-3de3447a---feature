// Command app runs the vacuum-valve interlock service.
package main

import (
	"log"
	"net/http"
	"os"

	"vacuum-interlock/internal/causality"
	"vacuum-interlock/internal/server"
)

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	srv := server.New(causality.NewEngine())
	log.Printf("vacuum interlock service listening on %s (health: /healthz)", addr)
	log.Fatal(http.ListenAndServe(addr, srv))
}
