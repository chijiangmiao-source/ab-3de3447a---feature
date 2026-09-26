// Command app runs the vacuum-valve interlock service.
//
// With DATA_DIR set, the causal state of every round is durable: each
// accepted round creation and event consumption is committed to the
// append-only log in that directory (and fsynced) before its success
// response is returned, so a process exit or container rebuild recovers
// to the last consistent state. Without DATA_DIR the service runs
// in-memory.
//
// ENABLE_ADMIN_RESTART=1 registers POST /admin/restart, an acceptance
// hook that re-executes the process image in place: every byte of
// in-memory state is dropped and recovery runs from the commit log,
// while the container keeps running.
package main

import (
	"log"
	"net/http"
	"os"
	"syscall"
	"time"

	"vacuum-interlock/internal/causality"
	"vacuum-interlock/internal/server"
)

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	var eng *causality.Engine
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		log.Printf("DATA_DIR not set: running with in-memory state only")
		eng = causality.NewEngine()
	} else {
		var err error
		eng, err = causality.OpenEngine(dataDir)
		if err != nil {
			log.Fatalf("recover causal state from %s: %v", dataDir, err)
		}
		log.Printf("causal state recovered from %s (rounds: %v)", dataDir, eng.ListRounds())
	}

	srv := server.New(eng)
	handler := http.Handler(srv)
	if os.Getenv("ENABLE_ADMIN_RESTART") == "1" {
		handler = withRestart(handler)
		log.Printf("admin restart hook enabled at POST /admin/restart")
	}

	log.Printf("vacuum interlock service listening on %s (health: /healthz)", addr)
	log.Fatal(http.ListenAndServe(addr, handler))
}

// withRestart adds POST /admin/restart: the response is flushed first,
// then the process re-executes itself. No shutdown flush is needed (or
// done): every commit was already fsynced when it was accepted, so the
// new process image recovers the complete causal state from the log.
func withRestart(next http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", next)
	mux.HandleFunc("POST /admin/restart", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"restarting"}` + "\n"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		go func() {
			time.Sleep(200 * time.Millisecond)
			exe, err := os.Executable()
			if err != nil {
				log.Printf("restart: locate executable: %v", err)
				return
			}
			log.Printf("re-executing process; recovering causal state from the commit log")
			if err := syscall.Exec(exe, os.Args, os.Environ()); err != nil {
				log.Printf("restart: re-exec failed: %v", err)
			}
		}()
	})
	return mux
}
