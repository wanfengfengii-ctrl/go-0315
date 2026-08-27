// Command server is the executable entry point for the archival replica
// integrity and recovery service. It opens the SQLite persistence layer,
// performs startup recovery, wires the application service to the versioned
// HTTP interface and listens on the configured address.
package main

import (
	"log"
	"net/http"
	"os"

	"archival-replica-integrity-recovery/internal/httpapi"
	"archival-replica-integrity-recovery/internal/service"
	"archival-replica-integrity-recovery/internal/store"
)

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "archival.db"
	}

	st, err := store.Open(dbPath)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()

	svc := service.NewService(st, service.SuccessAdapter{})
	srv := &http.Server{
		Addr:    addr,
		Handler: httpapi.NewServer(svc),
	}

	log.Printf("listening on %s (db=%s)", addr, dbPath)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server: %v", err)
	}
}
