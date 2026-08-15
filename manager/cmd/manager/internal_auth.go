package main

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

type managerInternalAuth struct {
	generator           *internalauth.Generator
	procdTokenGenerator service.TokenGenerator
}

func buildManagerInternalAuth(logger *zap.Logger) managerInternalAuth {
	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		logger.Warn("Failed to load internal auth private key; procd calls will not work",
			zap.String("path", internalauth.DefaultInternalJWTPrivateKeyPath),
			zap.Error(err),
		)
		return managerInternalAuth{}
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "manager",
		PrivateKey: privateKey,
		TTL:        30 * time.Second,
	})
	logger.Info("Internal auth generator initialized for procd communication")
	return managerInternalAuth{
		generator:           generator,
		procdTokenGenerator: runtimeauth.NewInternalTokenGenerator(generator),
	}
}
