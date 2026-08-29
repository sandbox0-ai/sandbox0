package nodeenrollment

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"text/template"
	"time"
)

const maxRuntimeConfigArchiveBytes = 8 << 20

type RuntimeConfigIdentity struct {
	NodeName                string
	NodeID                  string
	NodeUID                 string
	AgentUID                string
	PrivateIP               string
	AllocationCIDR          string
	RegionID                string
	ClusterID               string
	ManagerAuthorityURL     string
	ManagerAuthorityPeerURI string
}

type runtimeConfigFile struct {
	name     string
	mode     int64
	payload  []byte
	template bool
}

// RuntimeConfigTemplate is an immutable, control-plane-owned node config
// bundle. It replaces the retired approach of copying configuration and
// credentials from another live worker.
type RuntimeConfigTemplate struct {
	files                   []runtimeConfigFile
	managerAuthorityURL     string
	managerAuthorityPeerURI string
}

func NewRuntimeConfigTemplateFromFile(
	archiveFile, managerAuthorityURL, managerAuthorityPeerURI string,
) (*RuntimeConfigTemplate, error) {
	payload, err := os.ReadFile(archiveFile)
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 || len(payload) > maxRuntimeConfigArchiveBytes {
		return nil, errors.New("node runtime config template archive size is invalid")
	}
	return newRuntimeConfigTemplate(payload, managerAuthorityURL, managerAuthorityPeerURI)
}

func newRuntimeConfigTemplate(
	payload []byte,
	managerAuthorityURL, managerAuthorityPeerURI string,
) (*RuntimeConfigTemplate, error) {
	authorityURL, err := url.Parse(strings.TrimSpace(managerAuthorityURL))
	if err != nil || authorityURL.Scheme != "https" || authorityURL.Host == "" ||
		authorityURL.RawQuery != "" || authorityURL.Fragment != "" {
		return nil, errors.New("manager authority URL is invalid")
	}
	peerURI, err := url.Parse(strings.TrimSpace(managerAuthorityPeerURI))
	if err != nil || peerURI.Scheme != "spiffe" || peerURI.Host != "sandbox0.internal" {
		return nil, errors.New("manager authority peer URI is invalid")
	}
	gzipReader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()
	templateFiles := make([]runtimeConfigFile, 0, 16)
	seen := make(map[string]struct{})
	tarReader := tar.NewReader(io.LimitReader(gzipReader, maxRuntimeConfigArchiveBytes+1))
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		name := strings.TrimPrefix(header.Name, "./")
		if header.Typeflag == tar.TypeDir && name == "node-runtime-template/" {
			continue
		}
		if header.Typeflag != tar.TypeReg || !strings.HasPrefix(name, "node-runtime-template/") ||
			path.Clean(name) != name || strings.Contains(name, "..") || header.Size <= 0 ||
			header.Size > 2<<20 || header.Mode&0o022 != 0 || header.Uid != 0 || header.Gid != 0 {
			return nil, fmt.Errorf("unsafe node runtime config archive member %q", header.Name)
		}
		relative := strings.TrimPrefix(name, "node-runtime-template/")
		if relative == "" || strings.HasPrefix(relative, "/") {
			return nil, errors.New("node runtime config archive path is invalid")
		}
		isTemplate := strings.HasSuffix(relative, ".tmpl")
		outputName := strings.TrimSuffix(relative, ".tmpl")
		if !allowedRuntimeConfigOutput(outputName, header.Mode&0o777) {
			return nil, fmt.Errorf("node runtime config path or mode is outside the installation contract: %s", outputName)
		}
		if _, exists := seen[outputName]; exists {
			return nil, fmt.Errorf("duplicate rendered node runtime path %q", outputName)
		}
		seen[outputName] = struct{}{}
		contents, err := io.ReadAll(io.LimitReader(tarReader, header.Size+1))
		if err != nil || int64(len(contents)) != header.Size {
			return nil, errors.New("node runtime config archive member is truncated")
		}
		if isTemplate {
			if _, err := template.New(outputName).Option("missingkey=error").Parse(string(contents)); err != nil {
				return nil, fmt.Errorf("parse node runtime template %s: %w", outputName, err)
			}
		}
		templateFiles = append(templateFiles, runtimeConfigFile{
			name: outputName, mode: header.Mode & 0o777, payload: contents, template: isTemplate,
		})
	}
	required := []string{
		"etc/sandbox0/ctld.yaml",
		"etc/sandbox0/ctld-networking.yaml",
		"etc/sandbox0/ctld.env",
		"etc/sandbox0/ctld-a.env",
		"etc/sandbox0/ctld-b.env",
		"etc/sandbox0/internal-auth/data-public.pem",
		"etc/sandbox0/pki/manager-ca.pem",
		"etc/sandbox0/tokens/nomad.token",
		"etc/nomad.d/30-sandbox0-gvisor.hcl",
		"opt/cni/config/10-sandbox0.conflist",
	}
	for _, name := range required {
		if _, ok := seen[name]; !ok {
			return nil, fmt.Errorf("node runtime config template is missing %s", name)
		}
	}
	return &RuntimeConfigTemplate{files: templateFiles,
		managerAuthorityURL: authorityURL.String(), managerAuthorityPeerURI: peerURI.String()}, nil
}

func allowedRuntimeConfigOutput(name string, mode int64) bool {
	if mode != 0o600 && mode != 0o644 {
		return false
	}
	if strings.HasPrefix(name, "etc/sandbox0/") && len(name) > len("etc/sandbox0/") {
		return true
	}
	return name == "etc/nomad.d/30-sandbox0-gvisor.hcl" ||
		name == "opt/cni/config/10-sandbox0.conflist"
}

func (r *RuntimeConfigTemplate) Render(identity RuntimeConfigIdentity) ([]byte, error) {
	if r == nil || len(r.files) == 0 {
		return nil, errors.New("node runtime config template is unavailable")
	}
	identity.ManagerAuthorityURL = r.managerAuthorityURL
	identity.ManagerAuthorityPeerURI = r.managerAuthorityPeerURI
	if identity.NodeName == "" || identity.NodeID == "" || identity.NodeUID == "" ||
		identity.AgentUID == "" || identity.PrivateIP == "" || identity.AllocationCIDR == "" ||
		identity.RegionID == "" || identity.ClusterID == "" {
		return nil, errors.New("node runtime config identity is incomplete")
	}
	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	gzipWriter.ModTime = time.Time{}
	tarWriter := tar.NewWriter(gzipWriter)
	for _, file := range r.files {
		contents := file.payload
		if file.template {
			parsed, err := template.New(file.name).Option("missingkey=error").Parse(string(file.payload))
			if err != nil {
				return nil, err
			}
			var rendered bytes.Buffer
			if err := parsed.Execute(&rendered, identity); err != nil {
				return nil, fmt.Errorf("render node runtime config %s: %w", file.name, err)
			}
			contents = rendered.Bytes()
			if bytes.Contains(contents, []byte("{{")) || bytes.Contains(contents, []byte("}}")) {
				return nil, fmt.Errorf("rendered node runtime config %s retains a template marker", file.name)
			}
		}
		header := &tar.Header{
			Name: "node-runtime/" + file.name, Mode: file.mode, Size: int64(len(contents)),
			Typeflag: tar.TypeReg, Uid: 0, Gid: 0,
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return nil, err
		}
		if _, err := tarWriter.Write(contents); err != nil {
			return nil, err
		}
	}
	if err := tarWriter.Close(); err != nil {
		return nil, err
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}
	if output.Len() > maxRuntimeConfigArchiveBytes {
		return nil, errors.New("rendered node runtime archive is too large")
	}
	return output.Bytes(), nil
}
