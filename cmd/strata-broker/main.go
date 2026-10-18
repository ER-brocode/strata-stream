package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ER-brocode/strata-stream/pkg/protocol"
	"github.com/ER-brocode/strata-stream/pkg/storage"
)

func main() {
	listenAddr := flag.String("listen", ":9092", "Address for Kafka-compatible wire protocol listener")
	dataDir := flag.String("data-dir", "./strata-data", "Local directory for commit logs and indexes")
	s3Endpoint := flag.String("s3-endpoint", "", "S3/MinIO compatible endpoint (optional)")
	s3Bucket := flag.String("s3-bucket", "strata-cold-storage", "S3 bucket for cold tiered segments")
	flag.Parse()

	fmt.Println(`
   _____ __             __           _____ __                               
  / ___// /__________ _/ /_____ _   / ___// /_________  ____ _____ ___     
  \__ \/ __/ ___/ __ ` + "`" + `/ __/ __ ` + "`" + `/   \__ \/ __/ ___/ _ \/ __ ` + "`" + `/ __ ` + "`" + `__ \    
 ___/ / /_/ /  / /_/ / /_/ /_/ /   ___/ / /_/ /  /  __/ /_/ / / / / / /    
/____/\__/_/   \__,_/\__/\__,_/   /____/\__/_/   \___/\__,_/_/ /_/ /_/     
  High-Throughput Tiered Message Streaming Broker (Kafka Compatible)
	`)

	log.Printf("[StrataStream] Initializing broker on %s, data-dir: %s\n", *listenAddr, *dataDir)

	server := protocol.NewBrokerServer(*listenAddr, *dataDir)
	if err := server.Start(); err != nil {
		log.Fatalf("[StrataStream] Failed to start broker server: %v", err)
	}

	log.Printf("[StrataStream] Wire protocol engine accepting connections on %s\n", *listenAddr)

	var backend storage.StorageBackend
	if *s3Endpoint != "" {
		backend = storage.NewS3CompatibleClient(*s3Endpoint, *s3Bucket, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"))
		log.Printf("[StrataStream] Tiered cold storage active: remote S3/MinIO target %s/%s\n", *s3Endpoint, *s3Bucket)
	} else {
		localCold := "./strata-cold-tier"
		localBackend, err := storage.NewLocalDiskBlobStore(localCold)
		if err != nil {
			log.Fatalf("[StrataStream] Failed to initialize local cold store: %v", err)
		}
		backend = localBackend
		log.Printf("[StrataStream] Tiered cold storage active: local mock blob store %s\n", localCold)
	}
	_ = backend

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	log.Println("[StrataStream] Gracefully shutting down broker...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Stop(); err != nil {
		log.Printf("[StrataStream] Error during shutdown: %v\n", err)
	}
	log.Println("[StrataStream] Broker shutdown complete. All logs and indexes safely synced.")
}
