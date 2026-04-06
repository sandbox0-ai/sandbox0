package apikey

import (
	"strings"
	"testing"
)

func TestNewKeyValueIncludesRegionID(t *testing.T) {
	keyValue, err := NewKeyValue("aws-us-east-1")
	if err != nil {
		t.Fatalf("NewKeyValue() error = %v", err)
	}
	if !strings.HasPrefix(keyValue, "s0_aws-us-east-1_") {
		t.Fatalf("key prefix = %q, want region-routable prefix", keyValue)
	}

	regionID, err := ParseRegionIDFromKey(keyValue)
	if err != nil {
		t.Fatalf("ParseRegionIDFromKey() error = %v", err)
	}
	if regionID != "aws-us-east-1" {
		t.Fatalf("regionID = %q, want aws-us-east-1", regionID)
	}
}

func TestParseRegionIDFromKeyRejectsLegacyKeyFormat(t *testing.T) {
	if _, err := ParseRegionIDFromKey("s0_deadbeef_0123456789abcdef0123456789abcdef"); err == nil {
		t.Fatal("expected legacy key format to be rejected")
	}
}

func TestNewKeyValueRejectsInvalidRegionID(t *testing.T) {
	if _, err := NewKeyValue("aws_us_east_1"); err == nil {
		t.Fatal("expected invalid region id to be rejected")
	}
}
