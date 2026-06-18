package handlers

import (
	"strings"

	"github.com/google/uuid"
)

func isValidUUID(value string) bool {
	_, err := uuid.Parse(strings.TrimSpace(value))
	return err == nil
}
