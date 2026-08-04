package framework

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// PortForwardService starts a kubectl port-forward to a service.
func PortForwardService(ctx context.Context, kubeconfig, namespace, service string, remotePort int) (string, func(), error) {
	if service == "" {
		return "", nil, fmt.Errorf("service name is required")
	}
	if remotePort <= 0 {
		return "", nil, fmt.Errorf("remote port is required")
	}

	localPort, err := freePort()
	if err != nil {
		return "", nil, fmt.Errorf("allocate local port: %w", err)
	}

	pfCtx, cancel := context.WithCancel(ctx)
	args := []string{
		"port-forward",
		"svc/" + service,
		fmt.Sprintf("%d:%d", localPort, remotePort),
		"--address",
		"127.0.0.1",
	}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}
	if kubeconfig != "" {
		args = append(args, "--kubeconfig", kubeconfig)
	}

	cmd := exec.CommandContext(pfCtx, "kubectl", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return "", nil, fmt.Errorf("port-forward stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return "", nil, fmt.Errorf("port-forward stderr: %w", err)
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return "", nil, fmt.Errorf("start port-forward: %w", err)
	}

	readyCh := make(chan struct{})
	var readyOnce sync.Once
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "Forwarding from 127.0.0.1:") || strings.Contains(line, "Forwarding from [::1]:") {
				readyOnce.Do(func() { close(readyCh) })
				return
			}
		}
	}()
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "Forwarding from 127.0.0.1:") || strings.Contains(line, "Forwarding from [::1]:") {
				readyOnce.Do(func() { close(readyCh) })
				return
			}
		}
	}()

	select {
	case <-readyCh:
	case err := <-waitCh:
		cancel()
		if err == nil {
			err = fmt.Errorf("port-forward exited unexpectedly")
		}
		return "", nil, fmt.Errorf("port-forward exited: %w", err)
	case <-time.After(20 * time.Second):
		cancel()
		return "", nil, fmt.Errorf("timed out waiting for port-forward to be ready")
	}

	cleanup := func() {
		cancel()
		select {
		case <-waitCh:
		case <-time.After(5 * time.Second):
		}
	}

	return fmt.Sprintf("http://127.0.0.1:%d", localPort), cleanup, nil
}

func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected listener address type")
	}
	return addr.Port, nil
}
