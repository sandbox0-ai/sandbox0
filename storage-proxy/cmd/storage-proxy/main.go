package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sandbox0-ai/infra/pkg/env"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/audit"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/auth"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/config"
	grpcserver "github.com/sandbox0-ai/infra/storage-proxy/pkg/grpc"
	httpserver "github.com/sandbox0-ai/infra/storage-proxy/pkg/http"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/infra/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Load environment variables from .env file
	env.Load()

	// Setup logger
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetOutput(os.Stdout)

	// Load configuration
	cfg := config.LoadFromEnv()
	if err := cfg.Validate(); err != nil {
		logger.WithError(err).Fatal("Invalid configuration")
	}

	// Set log level
	level, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		logger.WithError(err).Warn("Invalid log level, using info")
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)

	logger.WithFields(logrus.Fields{
		"grpc_port": cfg.GRPCPort,
		"http_port": cfg.HTTPPort,
		"log_level": cfg.LogLevel,
	}).Info("Starting storage-proxy")

	// Create volume manager
	volMgr := volume.NewManager(logger)

	// Create audit logger
	var auditor *audit.Logger
	if cfg.AuditLog {
		auditor, err = audit.NewLogger(cfg.AuditFile, logger)
		if err != nil {
			logger.WithError(err).Fatal("Failed to create audit logger")
		}
		defer auditor.Close()
	} else {
		auditor, _ = audit.NewLogger("", logger)
	}

	// Create authenticator
	authenticator := auth.NewAuthenticator(cfg.JWTSecret)

	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(authenticator.UnaryInterceptor()),
	)

	// Register FileSystem service
	fsServer := grpcserver.NewFileSystemServer(volMgr, auditor, logger)
	pb.RegisterFileSystemServer(grpcServer, fsServer)

	// Register health service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// Enable reflection for grpcurl
	reflection.Register(grpcServer)

	// Start gRPC server
	grpcAddr := fmt.Sprintf("%s:%d", cfg.GRPCAddr, cfg.GRPCPort)
	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.WithError(err).Fatal("Failed to listen for gRPC")
	}

	go func() {
		logger.WithField("address", grpcAddr).Info("Starting gRPC server")
		if err := grpcServer.Serve(grpcListener); err != nil {
			logger.WithError(err).Fatal("Failed to serve gRPC")
		}
	}()

	// Create HTTP server
	httpSrv := httpserver.NewServer(logger)
	httpAddr := fmt.Sprintf("%s:%d", cfg.HTTPAddr, cfg.HTTPPort)
	httpServer := &http.Server{
		Addr:         httpAddr,
		Handler:      httpSrv,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.WithField("address", httpAddr).Info("Starting HTTP server")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Fatal("Failed to serve HTTP")
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down gracefully...")

	// Shutdown HTTP server
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.WithError(err).Error("HTTP server shutdown error")
	}

	// Stop gRPC server
	grpcServer.GracefulStop()

	// Unmount all volumes
	for _, volumeID := range volMgr.ListVolumes() {
		logger.WithField("volume_id", volumeID).Info("Unmounting volume")
		if err := volMgr.UnmountVolume(context.Background(), volumeID); err != nil {
			logger.WithError(err).WithField("volume_id", volumeID).Error("Failed to unmount volume")
		}
	}

	logger.Info("Shutdown complete")
}
