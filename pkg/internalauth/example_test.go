package internalauth_test

import (
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

var (
	examplePrivateKey ed25519.PrivateKey
	examplePublicKey  ed25519.PublicKey
)

func init() {
	var err error
	examplePublicKey, examplePrivateKey, err = ed25519.GenerateKey(nil)
	if err != nil {
		panic(err)
	}
}

func ExampleGenerator() {
	// Create a generator for the caller service
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: examplePrivateKey,
		TTL:        30 * time.Second,
	})

	// Generate a token for calling manager's storage endpoint.
	token, err := generator.Generate(
		"manager",  // target
		"team-123", // team ID
		"user-456", // user ID
		internalauth.GenerateOptions{ // options
			Permissions: []string{"sandbox:read", "sandbox:write"},
			UserID:      "user-456",
		},
	)
	if err != nil {
		panic(err)
	}

	// Output is a JWT token that changes each time
	_ = token
	fmt.Println("eyJhbGciOiJFZDI1NTE5...")
	// Output: eyJhbGciOiJFZDI1NTE5...
}

func ExampleValidator() {
	// First, generate a token (in real usage, this comes from the caller service)
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: examplePrivateKey,
	})
	token, _ := generator.Generate("manager", "team-123", "user-456",
		internalauth.GenerateOptions{
			Permissions: []string{"sandbox:read", "sandbox:write"},
		})

	// Create a validator for the target service
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:    "manager",
		PublicKey: examplePublicKey,
	})

	// Validate the token
	claims, err := validator.Validate(token)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Caller: %s, Team: %s, Permissions: %v\n",
		claims.Caller, claims.TeamID, claims.Permissions)
	// Output: Caller: cluster-gateway, Team: team-123, Permissions: [sandbox:read sandbox:write]
}
