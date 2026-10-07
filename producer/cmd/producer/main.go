// Command producer runs the local HTTP API that publishes tracking events to Kinesis.
// With EMIT_INTERVAL set it also emits simulated events on its own (see internal/simulator).
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

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	"producer/internal/publisher"
	"producer/internal/server"
	"producer/internal/simulator"
)

func main() {
	streamName := os.Getenv("STREAM_NAME")
	if streamName == "" {
		log.Fatal("STREAM_NAME is required (see .env.example)")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	emit, err := simulator.ParseConfig(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}

	// Ctrl+C / SIGTERM cancel ctx: the HTTP server shuts down and the emitter stops.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Credentials come from the SDK default chain (AWS_PROFILE, SSO, env vars).
	// For production use a dedicated role assumed via AssumeRole instead.
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}
	if cfg.Region == "" {
		log.Fatal("AWS_REGION (or AWS_DEFAULT_REGION) is required")
	}

	pub := &publisher.Publisher{API: kinesis.NewFromConfig(cfg), StreamName: streamName}

	// The API has no authentication, so it only listens on the loopback interface.
	srv := &http.Server{
		Addr:              "127.0.0.1:" + port,
		Handler:           server.New(pub),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("producer listening on http://%s, stream=%s region=%s", srv.Addr, streamName, cfg.Region)

	if emit.Enabled {
		log.Printf("simulator enabled: %d packages, one batch every %s (Ctrl+C to stop)", emit.Packages, emit.Interval)
		go simulator.New(pub, emit.Packages, time.Now).Run(ctx, emit.Interval, log.Printf)
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	log.Print("producer stopped")
}
