// Package nodebootstrap installs and renews disposable Nomad sandbox workers.
package nodebootstrap

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const configSchemaVersion = 1

var safeRegionID = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$`)

// Config is the non-secret, image-independent worker bootstrap contract.
// Cloud-init writes it before enrollment. All durable identity and runtime
// material is issued by manager after Alibaba Cloud identity verification.
type Config struct {
	SchemaVersion      int      `json:"schema_version"`
	EnrollmentURL      string   `json:"enrollment_url"`
	EnrollmentCAFile   string   `json:"enrollment_ca_file"`
	NomadServers       []string `json:"nomad_servers"`
	RegionID           string   `json:"region_id"`
	ClusterID          string   `json:"cluster_id"`
	ReservedCPUMHz     int      `json:"reserved_cpu_mhz"`
	ReservedMemoryMB   int      `json:"reserved_memory_mb"`
	ReservedDiskMB     int      `json:"reserved_disk_mb"`
	RuntimeRoot        string   `json:"runtime_root,omitempty"`
	DataMount          string   `json:"data_mount,omitempty"`
	NomadKeyFile       string   `json:"nomad_key_file,omitempty"`
	AuthorityKeyFile   string   `json:"authority_key_file,omitempty"`
	ExactIdentityFile  string   `json:"exact_identity_file,omitempty"`
	RenewBeforeSeconds int      `json:"renew_before_seconds,omitempty"`
}

func LoadConfig(file string) (Config, error) {
	payload, err := os.ReadFile(file)
	if err != nil {
		return Config{}, err
	}
	if len(payload) == 0 || len(payload) > 64<<10 {
		return Config{}, errors.New("node bootstrap config size is invalid")
	}
	var config Config
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode node bootstrap config: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("node bootstrap config contains trailing data")
	}
	if err := config.normalize(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c *Config) normalize() error {
	if c.SchemaVersion != configSchemaVersion {
		return errors.New("node bootstrap config schema version is invalid")
	}
	c.EnrollmentURL = strings.TrimRight(strings.TrimSpace(c.EnrollmentURL), "/")
	endpoint, err := url.Parse(c.EnrollmentURL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil ||
		endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return errors.New("node enrollment URL must be an HTTPS origin")
	}
	c.RegionID = strings.TrimSpace(c.RegionID)
	if len(c.RegionID) > 64 || !safeRegionID.MatchString(c.RegionID) {
		return errors.New("node bootstrap region ID is invalid")
	}
	c.ClusterID = strings.TrimSpace(c.ClusterID)
	if len(c.ClusterID) > 64 || !safeRegionID.MatchString(c.ClusterID) {
		return errors.New("node bootstrap cluster ID is invalid")
	}
	if !filepath.IsAbs(c.EnrollmentCAFile) {
		return errors.New("node enrollment CA file must be absolute")
	}
	if len(c.NomadServers) == 0 || len(c.NomadServers) > 7 {
		return errors.New("node bootstrap requires one to seven Nomad servers")
	}
	seen := make(map[string]struct{}, len(c.NomadServers))
	for index, server := range c.NomadServers {
		server = strings.TrimSpace(server)
		host, port, err := splitHostPort(server)
		address, parseErr := netip.ParseAddr(host)
		if err != nil || parseErr != nil || !address.Is4() || !address.IsPrivate() || port != 4647 {
			return fmt.Errorf("nomad server %q must be a private IPv4 address on port 4647", server)
		}
		canonical := address.String() + ":4647"
		if _, exists := seen[canonical]; exists {
			return errors.New("nomad bootstrap servers must be unique")
		}
		seen[canonical] = struct{}{}
		c.NomadServers[index] = canonical
	}
	if c.ReservedCPUMHz < 1000 || c.ReservedMemoryMB < 4096 || c.ReservedDiskMB < 10240 {
		return errors.New("node bootstrap host reservations are below the safe minimum")
	}
	if c.RuntimeRoot == "" {
		c.RuntimeRoot = "/opt/sandbox0"
	}
	if c.DataMount == "" {
		c.DataMount = "/var/lib/sandbox0"
	}
	if c.NomadKeyFile == "" {
		c.NomadKeyFile = "/etc/sandbox0/node-enrollment/nomad-key.pem"
	}
	if c.AuthorityKeyFile == "" {
		c.AuthorityKeyFile = "/etc/sandbox0/node-enrollment/authority-key.pem"
	}
	if c.ExactIdentityFile == "" {
		c.ExactIdentityFile = "/etc/sandbox0/nomad-exact-node.json"
	}
	if c.RenewBeforeSeconds == 0 {
		c.RenewBeforeSeconds = 12 * 60 * 60
	}
	for _, path := range []string{c.RuntimeRoot, c.DataMount, c.NomadKeyFile, c.AuthorityKeyFile, c.ExactIdentityFile} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return errors.New("node bootstrap paths must be canonical and absolute")
		}
	}
	if c.RuntimeRoot != "/opt/sandbox0" || c.DataMount != "/var/lib/sandbox0" ||
		c.RenewBeforeSeconds < 60*60 || c.RenewBeforeSeconds > 7*24*60*60 {
		return errors.New("node bootstrap fixed paths or renewal policy are invalid")
	}
	return nil
}

func splitHostPort(value string) (string, int, error) {
	index := strings.LastIndexByte(value, ':')
	if index <= 0 || index == len(value)-1 {
		return "", 0, errors.New("address has no port")
	}
	port, err := strconv.Atoi(value[index+1:])
	return value[:index], port, err
}
