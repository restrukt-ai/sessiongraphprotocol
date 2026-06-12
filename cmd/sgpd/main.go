package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/restrukt-ai/sessiongraphprotocol/gen/sgp/v1/sgpv1connect"
	pg "github.com/restrukt-ai/sessiongraphprotocol/pkg/store/pg"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "sgpd: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Pool: every connection installs AGE.
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `LOAD 'age'; SET search_path = ag_catalog, "$user", public`)
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("create pool: %w", err)
	}
	defer pool.Close()

	// Migrations (SQL via goose + AGE graph init via pool).
	if err := pg.Migrate(ctx, cfg.DatabaseURL, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	// Notify broker + store.
	broker, err := pg.NewNotifyBroker(ctx, cfg.DatabaseURL, pool)
	if err != nil {
		return fmt.Errorf("notify broker: %w", err)
	}
	defer broker.Close(context.Background()) //nolint:errcheck

	go func() {
		if err := broker.Run(ctx); err != nil {
			slog.Error("notify broker exited", "err", err)
		}
	}()

	store := pg.NewPGStore(pool, broker)

	// Harness service: HTTP/2 cleartext (h2c), bearer token.
	harnessOpts := []connect.HandlerOption{connect.WithInterceptors(newBearerInterceptor(cfg.HarnessToken))}
	hMux := http.NewServeMux()
	hMux.Handle(sgpv1connect.NewSGPHarnessServiceHandler(&harnessHandler{store: store}, harnessOpts...))

	hServer := &http.Server{
		Addr:    cfg.HarnessAddr,
		Handler: h2c.NewHandler(hMux, &http2.Server{}),
	}
	go func() {
		slog.Info("harness listener", "addr", cfg.HarnessAddr)
		if err := hServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("harness server", "err", err)
		}
	}()

	// Management service: TLS, bearer token.
	mgmtOpts := []connect.HandlerOption{connect.WithInterceptors(newBearerInterceptor(cfg.ManagementToken))}
	mMux := http.NewServeMux()
	mMux.Handle(sgpv1connect.NewSGPManagementServiceHandler(&managementHandler{store: store}, mgmtOpts...))

	mServer := &http.Server{
		Addr:    cfg.ManagementAddr,
		Handler: mMux,
	}
	go func() {
		slog.Info("management listener", "addr", cfg.ManagementAddr, "tls", cfg.TLSCert != "")
		var serveErr error
		if cfg.TLSCert != "" && cfg.TLSKey != "" {
			serveErr = mServer.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey)
		} else {
			serveErr = mServer.ListenAndServe()
		}
		if serveErr != nil && serveErr != http.ErrServerClosed {
			slog.Error("management server", "err", serveErr)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	hServer.Shutdown(context.Background()) //nolint:errcheck
	mServer.Shutdown(context.Background()) //nolint:errcheck
	return nil
}
