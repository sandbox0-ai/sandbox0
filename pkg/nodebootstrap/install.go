package nodebootstrap

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"crypto"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeenrollment"
)

func (b *Bootstrapper) validateRuntimeRelease(artifact nodeenrollment.RuntimeArtifact) (string, error) {
	current := filepath.Join(b.config.RuntimeRoot, "current")
	release, err := filepath.EvalSymlinks(current)
	if err != nil {
		return "", err
	}
	releasesRoot := filepath.Join(b.config.RuntimeRoot, "releases") + string(filepath.Separator)
	if !strings.HasPrefix(release+string(filepath.Separator), releasesRoot) {
		return "", errors.New("current runtime release is outside the immutable release root")
	}
	payload, err := os.ReadFile(filepath.Join(release, "metadata.json"))
	if err != nil {
		return "", err
	}
	var metadata struct {
		SourceCommit string `json:"source_commit"`
		Target       struct {
			OS           string `json:"os"`
			Architecture string `json:"architecture"`
		} `json:"target"`
		Runsc struct {
			Distribution string `json:"distribution"`
		} `json:"runsc"`
	}
	if err := json.Unmarshal(payload, &metadata); err != nil {
		return "", err
	}
	if metadata.SourceCommit != artifact.SourceCommit || metadata.Target.OS != "linux" ||
		metadata.Target.Architecture != "amd64" || metadata.Runsc.Distribution != "official-stock" {
		return "", errors.New("installed runtime release differs from the enrolled immutable artifact")
	}
	for _, executable := range []string{"nomad", "node-bootstrap", "ctld", "runsc", "sandbox0-gvisor"} {
		info, err := os.Stat(filepath.Join(release, "bin", executable))
		if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
			return "", fmt.Errorf("runtime release executable %s is invalid", executable)
		}
	}
	return release, nil
}

func (b *Bootstrapper) prepareNomadHost(ctx context.Context, release string) error {
	if !isMounted(b.config.DataMount) {
		return errors.New("disposable sandbox data disk is not mounted")
	}
	for directory, mode := range map[string]fs.FileMode{
		"/etc/nomad.d": 0o700, "/etc/sandbox0/node-enrollment": 0o700,
		"/var/lib/sandbox0/nomad": 0o750, "/opt/nomad": 0o755,
		"/opt/cni/bin": 0o755, "/opt/cni/config": 0o755,
	} {
		if err := ensureCanonicalDirectory(directory, mode); err != nil {
			return err
		}
		if err := os.Chmod(directory, mode); err != nil {
			return err
		}
	}
	if !isMounted("/opt/nomad") {
		if err := ensureFSTabEntry("/var/lib/sandbox0/nomad /opt/nomad none bind 0 0"); err != nil {
			return err
		}
		if err := b.runner.Run(ctx, "mount", "--bind", "/var/lib/sandbox0/nomad", "/opt/nomad"); err != nil {
			return err
		}
	}
	for directory, mode := range map[string]fs.FileMode{
		"/opt/nomad/data": 0o700, "/opt/nomad/plugins": 0o755,
		"/opt/nomad/alloc": 0o711, "/opt/nomad/alloc_mounts": 0o711,
	} {
		if err := ensureCanonicalDirectory(directory, mode); err != nil {
			return err
		}
		if err := os.Chmod(directory, mode); err != nil {
			return err
		}
	}
	hostAssets := filepath.Join(release, "share/sandbox0/deploy/nomad/host")
	for _, asset := range []struct {
		source, destination string
		mode                fs.FileMode
	}{
		{filepath.Join(hostAssets, "sandbox0-nomad-agent"), "/usr/local/sbin/sandbox0-nomad-agent", 0o755},
		{filepath.Join(hostAssets, "nomad.service"), "/etc/systemd/system/nomad.service", 0o644},
	} {
		if err := copyRegularFile(asset.source, asset.destination, asset.mode); err != nil {
			return err
		}
	}
	return installCNIPlugins(release)
}

func (b *Bootstrapper) verifyExactCertificates(
	response *nodeenrollment.FinalizeResponse,
	nomadSigner, authoritySigner crypto.Signer,
	nodeName, nodeID, privateIP string,
	managerCA []byte,
) error {
	nomadCA, err := os.ReadFile(exactNomadCAFile)
	if err != nil {
		return err
	}
	datacenter := strings.ReplaceAll(b.config.RegionID, "-", "_")
	if err := verifyIssuedCertificate(response.NomadCertificatePEM, nomadCA, nomadSigner,
		"client."+datacenter+".nomad", privateIP,
		"spiffe://sandbox0.internal/"+b.config.RegionID+"/nomad/client/"+nodeID,
		x509.ExtKeyUsageClientAuth); err != nil {
		return fmt.Errorf("verify exact Nomad identity: %w", err)
	}
	if response.AuthorityCommonName != "ctld-"+strings.TrimPrefix(nodeName, "s0-") {
		return errors.New("manager returned an unexpected node authority common name")
	}
	if err := verifyIssuedCertificate(response.NodeAuthorityCertificatePEM, managerCA, authoritySigner,
		response.AuthorityCommonName, "", "", x509.ExtKeyUsageClientAuth); err != nil {
		return fmt.Errorf("verify exact ctld identity: %w", err)
	}
	return nil
}

func (b *Bootstrapper) installExactIdentity(
	bootstrap *nodeenrollment.BootstrapResponse,
	finalized *nodeenrollment.FinalizeResponse,
	nodeID, privateIP string,
	nomadKeyPayload []byte,
	authoritySigner crypto.Signer,
) error {
	authorityKeyPayload, err := os.ReadFile(b.config.AuthorityKeyFile)
	if err != nil {
		return err
	}
	nomadCA, err := os.ReadFile(exactNomadCAFile)
	if err != nil {
		return err
	}
	files := []struct {
		name    string
		payload []byte
		mode    fs.FileMode
	}{
		{exactNomadCertificateFile, finalized.NomadCertificatePEM, 0o644},
		{exactNomadKeyFile, nomadKeyPayload, 0o600},
		{ctldCertificateFile, finalized.NodeAuthorityCertificatePEM, 0o644},
		{ctldKeyFile, authorityKeyPayload, 0o600},
		{managerTokenFile, []byte(finalized.AuthorityCommonName + "\n"), 0o600},
		{nomadRuntimeCAFile, nomadCA, 0o644},
		{nomadRuntimeCertificateFile, finalized.NomadCertificatePEM, 0o644},
		{nomadRuntimeKeyFile, nomadKeyPayload, 0o600},
	}
	for _, file := range files {
		if err := atomicWriteFile(file.name, file.payload, file.mode); err != nil {
			return err
		}
	}
	identity := exactIdentity{
		SchemaVersion: 1, ProviderInstanceID: bootstrap.ProviderInstanceID,
		PrivateIP: privateIP, NodeName: bootstrap.NodeName, NodeID: nodeID,
		NodeUID: finalized.NodeUID, AgentUID: finalized.AgentUID,
		AuthorityCommonName: finalized.AuthorityCommonName, AllocationCIDR: bootstrap.AllocationCIDR,
		RuntimeSourceCommit:   bootstrap.RuntimeArtifact.SourceCommit,
		RuntimeBundleSHA256:   bootstrap.RuntimeArtifact.SHA256,
		EnrollmentCompletedAt: b.now().UTC().Format(time.RFC3339),
	}
	payload, err := json.MarshalIndent(identity, "", "  ")
	if err != nil {
		return err
	}
	if err := atomicWriteFile(b.config.ExactIdentityFile, append(payload, '\n'), 0o600); err != nil {
		return err
	}
	if err := os.Remove(nomadIntroductionTokenFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Keep this reference intentional: loading the key before mutation proves
	// the installed private material still represents the CSR signer.
	if authoritySigner == nil {
		return errors.New("node authority signer is unavailable")
	}
	return nil
}

func (b *Bootstrapper) installCTLD(
	ctx context.Context,
	release string,
	staged *stagedRuntimeConfig,
) error {
	installer := filepath.Join(release, "share/sandbox0/deploy/nomad/ctld/install-node.sh")
	args := []string{
		"--ctld", filepath.Join(release, "bin/ctld"),
		"--driver", filepath.Join(release, "bin/sandbox0-gvisor"),
		"--runsc", filepath.Join(release, "bin/runsc"),
		"--config", staged.path("etc/sandbox0/ctld.yaml"),
		"--network-config", staged.path("etc/sandbox0/ctld-networking.yaml"),
		"--nomad-config", staged.path("etc/nomad.d/30-sandbox0-gvisor.hcl"),
		"--env", staged.path("etc/sandbox0/ctld.env"),
		"--start",
	}
	return b.runner.Run(ctx, installer, args...)
}

func (b *Bootstrapper) installRenewalTimer(ctx context.Context, release string) error {
	hostAssets := filepath.Join(release, "share/sandbox0/deploy/nomad/host")
	for _, name := range []string{"sandbox0-node-bootstrap.service", "sandbox0-node-bootstrap.timer"} {
		if err := copyRegularFile(filepath.Join(hostAssets, name), filepath.Join("/etc/systemd/system", name), 0o644); err != nil {
			return err
		}
	}
	if err := b.runner.Run(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	return b.runner.Run(ctx, "systemctl", "enable", "--now", "sandbox0-node-bootstrap.timer")
}

func copyRegularFile(source, destination string, mode fs.FileMode) error {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("runtime asset %s is not a regular file", source)
	}
	payload, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return atomicWriteFile(destination, payload, mode)
}

func isMounted(path string) bool {
	handle, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	defer handle.Close()
	scanner := bufio.NewScanner(handle)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 5 && unescapeMountInfo(fields[4]) == path {
			return true
		}
	}
	return false
}

func unescapeMountInfo(value string) string {
	replacer := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return replacer.Replace(value)
}

func ensureFSTabEntry(entry string) error {
	payload, err := os.ReadFile("/etc/fstab")
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(payload), "\n") {
		if strings.TrimSpace(line) == entry {
			return nil
		}
	}
	if len(payload) > 0 && payload[len(payload)-1] != '\n' {
		payload = append(payload, '\n')
	}
	payload = append(payload, []byte(entry+"\n")...)
	return atomicWriteFile("/etc/fstab", payload, 0o644)
}

func installCNIPlugins(release string) error {
	matches, err := filepath.Glob(filepath.Join(release, "packages/cni-plugins-linux-amd64-v*.tgz"))
	if err != nil || len(matches) != 1 {
		return errors.New("runtime release must contain exactly one CNI plugin archive")
	}
	handle, err := os.Open(matches[0])
	if err != nil {
		return err
	}
	defer handle.Close()
	gzipReader, err := gzip.NewReader(handle)
	if err != nil {
		return err
	}
	defer gzipReader.Close()
	approved := map[string]bool{
		"bandwidth": true, "bridge": true, "dhcp": true, "dummy": true,
		"firewall": true, "host-device": true, "host-local": true, "ipvlan": true,
		"loopback": true, "macvlan": true, "portmap": true, "ptp": true,
		"sbr": true, "static": true, "tap": true, "tuning": true,
		"vlan": true, "vrf": true, "LICENSE": false, "README.md": false,
	}
	contents := make(map[string][]byte, len(approved))
	archive := tar.NewReader(gzipReader)
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		name := strings.TrimPrefix(header.Name, "./")
		if name == "." && header.Typeflag == tar.TypeDir {
			continue
		}
		executable, exists := approved[name]
		if !exists || header.Typeflag != tar.TypeReg || strings.Contains(name, "/") ||
			header.Size <= 0 || header.Size > 64<<20 {
			return fmt.Errorf("CNI archive contains an unexpected member %q", header.Name)
		}
		payload, err := io.ReadAll(io.LimitReader(archive, header.Size+1))
		if err != nil || int64(len(payload)) != header.Size {
			return errors.New("CNI archive member is truncated")
		}
		if executable {
			contents[name] = payload
		} else {
			contents[name] = nil
		}
	}
	if len(contents) != len(approved) {
		return errors.New("CNI archive inventory differs from the approved release")
	}
	if err := ensureCanonicalDirectory("/opt/cni/bin", 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir("/opt/cni/bin")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return errors.New("existing CNI binary directory contains a non-regular entry")
		}
		if err := os.Remove(filepath.Join("/opt/cni/bin", entry.Name())); err != nil {
			return err
		}
	}
	names := make([]string, 0, len(contents))
	for name, payload := range contents {
		if payload != nil {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if err := atomicWriteFile(filepath.Join("/opt/cni/bin", name), contents[name], 0o755); err != nil {
			return err
		}
	}
	return nil
}
