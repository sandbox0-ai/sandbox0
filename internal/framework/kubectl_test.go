package framework

import (
	"reflect"
	"testing"
)

func TestKubectlArgsPlacesKubeconfigBeforeExecSeparator(t *testing.T) {
	args := []string{"exec", "sandbox", "--", "sh", "-c", "test -f /tmp/ready"}

	got := kubectlArgs("/tmp/kubeconfig", args)
	want := []string{
		"--kubeconfig",
		"/tmp/kubeconfig",
		"exec",
		"sandbox",
		"--",
		"sh",
		"-c",
		"test -f /tmp/ready",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("kubectlArgs() = %#v, want %#v", got, want)
	}
}

func TestKubectlArgsWithoutKubeconfigLeavesArgumentsUnchanged(t *testing.T) {
	args := []string{"get", "pods"}

	got := kubectlArgs("", args)

	if !reflect.DeepEqual(got, args) {
		t.Fatalf("kubectlArgs() = %#v, want %#v", got, args)
	}
}
