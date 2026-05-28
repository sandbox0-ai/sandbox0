package rediscache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestJSONCacheSetGetDelete(t *testing.T) {
	redisServer := miniredis.RunT(t)
	cache, err := NewJSONCache[testJSONValue](context.Background(), JSONConfig{
		Config: Config{
			URL:       "redis://" + redisServer.Addr() + "/0",
			KeyPrefix: "sandbox0:test",
		},
		TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("new json cache: %v", err)
	}
	defer cache.Close()

	if _, ok, err := cache.Get(context.Background(), "sb-1"); err != nil || ok {
		t.Fatalf("empty Get() ok=%v err=%v, want miss", ok, err)
	}
	if err := cache.Set(context.Background(), "sb-1", testJSONValue{ID: "sb-1", Count: 7}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	value, ok, err := cache.Get(context.Background(), "sb-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatal("Get() miss, want hit")
	}
	if value.ID != "sb-1" || value.Count != 7 {
		t.Fatalf("Get() = %+v", value)
	}
	if err := cache.Delete(context.Background(), "sb-1"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, ok, err := cache.Get(context.Background(), "sb-1"); err != nil || ok {
		t.Fatalf("Get() after delete ok=%v err=%v, want miss", ok, err)
	}
}

type testJSONValue struct {
	ID    string `json:"id"`
	Count int    `json:"count"`
}
