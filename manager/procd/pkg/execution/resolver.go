// Package execution attributes sandbox processes and sockets to logical
// execution scopes declared by trusted supervisor roots.
package execution

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

const maxExecutionScopeIDBytes = 256

type SocketQuery struct {
	Transport  string
	LocalIP    net.IP
	LocalPort  int
	RemoteIP   net.IP
	RemotePort int
}

type RootProvider func() []session.ExecutionScopeRoot

type Resolver struct {
	procRoot string
	roots    RootProvider
	readFile func(string) ([]byte, error)
}

func NewResolver(procRoot string, roots RootProvider) *Resolver {
	if strings.TrimSpace(procRoot) == "" {
		procRoot = "/proc"
	}
	return &Resolver{procRoot: procRoot, roots: roots, readFile: os.ReadFile}
}

// ResolveSocket finds the process that owns a sandbox socket and attributes it
// to the closest trusted supervisor root. A missing scope is not an error:
// shared harness traffic and unrelated background processes are intentionally
// left unattributed.
func (r *Resolver) ResolveSocket(query SocketQuery) (sandboxobservability.ExecutionScope, bool, error) {
	if r == nil || r.roots == nil {
		return sandboxobservability.ExecutionScope{}, false, nil
	}
	if query.Transport != "tcp" && query.Transport != "udp" {
		return sandboxobservability.ExecutionScope{}, false, fmt.Errorf("unsupported transport %q", query.Transport)
	}
	if query.LocalPort <= 0 || query.LocalPort > 65535 {
		return sandboxobservability.ExecutionScope{}, false, fmt.Errorf("local port is outside 1-65535")
	}
	if query.RemotePort <= 0 || query.RemotePort > 65535 {
		return sandboxobservability.ExecutionScope{}, false, fmt.Errorf("remote port is outside 1-65535")
	}
	if query.LocalIP == nil || query.LocalIP.IsUnspecified() {
		return sandboxobservability.ExecutionScope{}, false, fmt.Errorf("local IP is required")
	}
	if query.RemoteIP == nil || query.RemoteIP.IsUnspecified() {
		return sandboxobservability.ExecutionScope{}, false, fmt.Errorf("remote IP is required")
	}

	inodes, err := r.socketInodes(query)
	if err != nil {
		return sandboxobservability.ExecutionScope{}, false, err
	}
	if len(inodes) == 0 {
		return sandboxobservability.ExecutionScope{}, false, nil
	}
	owners, err := r.socketOwners(inodes)
	if err != nil {
		return sandboxobservability.ExecutionScope{}, false, err
	}
	roots := r.roots()
	var resolved sandboxobservability.ExecutionScope
	attributed := false
	for _, owner := range owners {
		scope, ok := r.resolveProcess(owner, roots)
		if !ok {
			// One inode can be shared across processes after fork or explicit
			// FD passing. If any owner cannot be tied to the same scope, the
			// process that initiated this flow is ambiguous.
			return sandboxobservability.ExecutionScope{}, false, nil
		}
		if attributed && (scope.Namespace != resolved.Namespace || scope.Kind != resolved.Kind || scope.ID != resolved.ID) {
			return sandboxobservability.ExecutionScope{}, false, nil
		}
		resolved = scope
		attributed = true
	}
	return resolved, attributed, nil
}

func (r *Resolver) socketInodes(query SocketQuery) (map[string]struct{}, error) {
	result := map[string]struct{}{}
	for _, name := range []string{query.Transport, query.Transport + "6"} {
		data, err := r.read(filepath.Join(r.procRoot, "net", name))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("read proc net %s: %w", name, err)
		}
		ipv6 := strings.HasSuffix(name, "6")
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 10 || fields[0] == "sl" {
				continue
			}
			local, ok := parseProcEndpoint(fields[1], ipv6)
			if !ok || local.Port != query.LocalPort || !matchesLocalEndpoint(query.Transport, local.IP, query.LocalIP) {
				continue
			}
			remote, ok := parseProcEndpoint(fields[2], ipv6)
			if !ok || !matchesRemoteEndpoint(query.Transport, remote, query.RemoteIP, query.RemotePort) {
				continue
			}
			if fields[9] != "" && fields[9] != "0" {
				result[fields[9]] = struct{}{}
			}
		}
	}
	return result, nil
}

type procEndpoint struct {
	IP   net.IP
	Port int
}

func parseProcEndpoint(endpoint string, ipv6 bool) (procEndpoint, bool) {
	address, portText, ok := strings.Cut(endpoint, ":")
	if !ok || address == "" || portText == "" {
		return procEndpoint{}, false
	}
	port, err := strconv.ParseUint(portText, 16, 16)
	if err != nil {
		return procEndpoint{}, false
	}
	raw, err := hex.DecodeString(address)
	if err != nil {
		return procEndpoint{}, false
	}
	if ipv6 {
		if len(raw) != net.IPv6len {
			return procEndpoint{}, false
		}
		// Linux renders IPv6 /proc addresses as four native-endian 32-bit
		// words. procd runs on little-endian sandbox architectures today.
		for offset := 0; offset < len(raw); offset += 4 {
			reverseBytes(raw[offset : offset+4])
		}
	} else {
		if len(raw) != net.IPv4len {
			return procEndpoint{}, false
		}
		reverseBytes(raw)
	}
	return procEndpoint{IP: net.IP(raw), Port: int(port)}, true
}

func reverseBytes(value []byte) {
	for left, right := 0, len(value)-1; left < right; left, right = left+1, right-1 {
		value[left], value[right] = value[right], value[left]
	}
}

func matchesLocalEndpoint(transport string, actual, expected net.IP) bool {
	if actual.Equal(expected) {
		return true
	}
	// An unconnected UDP socket is commonly bound to the wildcard address even
	// though TPROXY observes the concrete source address on each datagram.
	return transport == "udp" && actual.IsUnspecified()
}

func matchesRemoteEndpoint(transport string, actual procEndpoint, expectedIP net.IP, expectedPort int) bool {
	if transport == "udp" && actual.Port == 0 && actual.IP.IsUnspecified() {
		// sendto-style UDP sockets do not retain a peer in /proc. The concrete
		// destination still comes from the intercepted datagram, while ownership
		// is resolved from the exact local address/port and socket inode.
		return true
	}
	return actual.Port == expectedPort && actual.IP.Equal(expectedIP)
}

func (r *Resolver) socketOwners(inodes map[string]struct{}) ([]processIdentity, error) {
	entries, err := os.ReadDir(r.procRoot)
	if err != nil {
		return nil, fmt.Errorf("read proc root: %w", err)
	}
	owners := make([]processIdentity, 0, len(inodes))
	seen := map[processIdentity]struct{}{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		before, err := r.readProcessIdentity(pid)
		if err != nil {
			continue
		}
		fdEntries, err := os.ReadDir(filepath.Join(r.procRoot, entry.Name(), "fd"))
		if err != nil {
			continue
		}
		for _, fd := range fdEntries {
			target, err := os.Readlink(filepath.Join(r.procRoot, entry.Name(), "fd", fd.Name()))
			if err != nil || !strings.HasPrefix(target, "socket:[") || !strings.HasSuffix(target, "]") {
				continue
			}
			inode := strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")
			if _, ok := inodes[inode]; !ok {
				continue
			}
			after, err := r.readProcessIdentity(pid)
			if err != nil || after != before {
				break
			}
			if _, ok := seen[before]; !ok {
				seen[before] = struct{}{}
				owners = append(owners, before)
			}
			break
		}
	}
	return owners, nil
}

type processIdentity struct {
	pid            int
	ppid           int
	startTimeTicks uint64
}

type processSnapshot struct {
	processIdentity
	environ map[string]string
}

func (r *Resolver) resolveProcess(owner processIdentity, roots []session.ExecutionScopeRoot) (sandboxobservability.ExecutionScope, bool) {
	chain := make([]processSnapshot, 0, 8)
	seen := map[int]struct{}{}
	pid := owner.pid
	for pid > 0 {
		if _, duplicate := seen[pid]; duplicate {
			break
		}
		seen[pid] = struct{}{}
		snapshot, err := r.process(pid)
		if err != nil {
			break
		}
		if len(chain) == 0 && snapshot.processIdentity != owner {
			return sandboxobservability.ExecutionScope{}, false
		}
		chain = append(chain, snapshot)
		if snapshot.ppid <= 0 || snapshot.ppid == pid {
			break
		}
		pid = snapshot.ppid
	}
	for _, snapshot := range chain {
		current, err := r.readProcessIdentity(snapshot.pid)
		if err != nil || current != snapshot.processIdentity {
			return sandboxobservability.ExecutionScope{}, false
		}
	}

	rootIndex := -1
	var root session.ExecutionScopeRoot
	for index, process := range chain {
		for _, candidate := range roots {
			if process.pid == candidate.PID &&
				process.startTimeTicks == candidate.ProcessStartTimeTicks &&
				(rootIndex < 0 || index < rootIndex) {
				rootIndex = index
				root = candidate
			}
		}
	}
	if rootIndex <= 0 {
		return sandboxobservability.ExecutionScope{}, false
	}

	// Prefer the first scoped descendant below the trusted root. Descendants
	// cannot reassign their lineage merely by replacing the marker later.
	for index := rootIndex - 1; index >= 0; index-- {
		id := chain[index].environ[root.Spec.IDEnvironmentVariable]
		if id == "" {
			continue
		}
		if id != strings.TrimSpace(id) || len(id) > maxExecutionScopeIDBytes {
			return sandboxobservability.ExecutionScope{}, false
		}
		return sandboxobservability.ExecutionScope{
			Namespace:   root.Spec.Namespace,
			Kind:        root.Spec.Kind,
			ID:          id,
			Attribution: sandboxobservability.ExecutionScopeAttributionProcessEnvironment,
		}, true
	}
	return sandboxobservability.ExecutionScope{}, false
}

func (r *Resolver) process(pid int) (processSnapshot, error) {
	before, err := r.readProcessIdentity(pid)
	if err != nil {
		return processSnapshot{}, err
	}
	environ, err := r.read(filepath.Join(r.procRoot, strconv.Itoa(pid), "environ"))
	if err != nil {
		return processSnapshot{}, err
	}
	after, err := r.readProcessIdentity(pid)
	if err != nil {
		return processSnapshot{}, err
	}
	if after != before {
		return processSnapshot{}, fmt.Errorf("process identity changed while reading environment")
	}
	return processSnapshot{processIdentity: before, environ: parseEnvironment(environ)}, nil
}

func (r *Resolver) readProcessIdentity(pid int) (processIdentity, error) {
	stat, err := r.read(filepath.Join(r.procRoot, strconv.Itoa(pid), "stat"))
	if err != nil {
		return processIdentity{}, err
	}
	closeParen := strings.LastIndexByte(string(stat), ')')
	if closeParen < 0 || closeParen+2 >= len(stat) {
		return processIdentity{}, fmt.Errorf("malformed process stat")
	}
	fields := strings.Fields(string(stat[closeParen+2:]))
	const startTimeIndex = 19 // Field 22 after removing pid and comm.
	if len(fields) <= startTimeIndex {
		return processIdentity{}, fmt.Errorf("malformed process stat")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return processIdentity{}, fmt.Errorf("parse process parent: %w", err)
	}
	startTimeTicks, err := strconv.ParseUint(fields[startTimeIndex], 10, 64)
	if err != nil || startTimeTicks == 0 {
		return processIdentity{}, fmt.Errorf("parse process starttime")
	}
	return processIdentity{pid: pid, ppid: ppid, startTimeTicks: startTimeTicks}, nil
}

func (r *Resolver) read(path string) ([]byte, error) {
	if r.readFile == nil {
		return os.ReadFile(path)
	}
	return r.readFile(path)
}

func parseEnvironment(data []byte) map[string]string {
	result := map[string]string{}
	for _, entry := range strings.Split(string(data), "\x00") {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key != "" {
			result[key] = value
		}
	}
	return result
}
