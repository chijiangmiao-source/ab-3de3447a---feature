// Command app runs the vacuum-valve interlock service.
//
// State is durable: rounds, waiting events and verdicts are written to a
// write-ahead log under DATA_DIR (default ./data) and fsynced before a
// success response is sent. On startup the logs are verified frame by
// frame; only provably complete commits are admitted and a torn or
// corrupted tail is rolled back to the last consistent state.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"vacuum-interlock/internal/causality"
	"vacuum-interlock/internal/server"
	"vacuum-interlock/internal/store"
)

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "./data"
	}

	// Replay the durable logs into a fresh engine; corrupted tails are
	// rolled back to the last consistent frame during Open.
	eng := causality.NewEngine()
	st, err := store.Open(dataDir, eng)
	if err != nil {
		log.Fatalf("open data directory %q: %v", dataDir, err)
	}
	eng.AttachStore(st)
	log.Printf("recovery complete; data directory %q", dataDir)

	srv := &http.Server{Addr: addr, Handler: server.New(eng)}
	go func() {
		log.Printf("vacuum interlock service listening on %s (health: /healthz, data: %s)", addr, dataDir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	log.Printf("shutdown signal received; closing WAL handles")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown: %v", err)
	}
	if err := st.Close(); err != nil {
		log.Printf("close store: %v", err)
	}
}
