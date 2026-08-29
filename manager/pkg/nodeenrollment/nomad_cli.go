package nodeenrollment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

type NomadCLIConfig struct {
	Binary         string
	Address        string
	Region         string
	CACertFile     string
	ClientCertFile string
	ClientKeyFile  string
	TokenFile      string
	NodePool       string
	IntroTTL       time.Duration
}

type NomadCLI struct {
	cli NomadCLIConfig
}

func NewNomadCLI(config NomadCLIConfig) (*NomadCLI, error) {
	config.Binary = strings.TrimSpace(config.Binary)
	config.Address = strings.TrimSpace(config.Address)
	config.Region = strings.TrimSpace(config.Region)
	config.NodePool = strings.TrimSpace(config.NodePool)
	if config.IntroTTL == 0 {
		config.IntroTTL = 30 * time.Minute
	}
	if config.Binary == "" || config.Address == "" || config.Region == "" ||
		config.CACertFile == "" || config.ClientCertFile == "" || config.ClientKeyFile == "" ||
		config.TokenFile == "" || config.NodePool == "" || config.IntroTTL < time.Minute ||
		config.IntroTTL > 30*time.Minute {
		return nil, errors.New("nomad enrollment CLI config is incomplete")
	}
	return &NomadCLI{cli: config}, nil
}

func (n *NomadCLI) IssueClientIntroductionToken(ctx context.Context, nodeName string) (string, error) {
	payload, err := n.run(ctx, "node", "intro", "create", "-node-name="+nodeName,
		"-node-pool="+n.cli.NodePool, "-ttl="+n.cli.IntroTTL.String(), "-json")
	if err != nil {
		return "", err
	}
	var response struct {
		JWT string `json:"JWT"`
	}
	if err := json.Unmarshal(payload, &response); err != nil || strings.Count(response.JWT, ".") != 2 {
		return "", errors.New("nomad did not return a valid client introduction JWT")
	}
	return response.JWT, nil
}

func (n *NomadCLI) ValidateRegisteredNode(
	ctx context.Context,
	nodeID, nodeName, privateIP string,
	alreadyAdmitted bool,
) error {
	payload, err := n.run(ctx, "node", "status", "-json", nodeID)
	if err != nil {
		return err
	}
	var node struct {
		ID                    string            `json:"ID"`
		Name                  string            `json:"Name"`
		Address               string            `json:"Address"`
		NodePool              string            `json:"NodePool"`
		Status                string            `json:"Status"`
		SchedulingEligibility string            `json:"SchedulingEligibility"`
		Meta                  map[string]string `json:"Meta"`
	}
	if err := json.Unmarshal(payload, &node); err != nil {
		return err
	}
	admitted := node.Meta["sandbox0_admitted"]
	if node.ID != nodeID || node.Name != nodeName || node.Address != privateIP ||
		node.NodePool != n.cli.NodePool || node.Status != "ready" ||
		(!alreadyAdmitted && admitted != "false") ||
		(alreadyAdmitted && admitted != "false" && admitted != "true") {
		return errors.New("nomad node is not the exact enrollment target")
	}
	return nil
}

func (n *NomadCLI) FenceRegisteredNode(ctx context.Context, nodeID string) error {
	if _, err := n.run(ctx, "node", "eligibility", "-disable", nodeID); err != nil {
		return err
	}
	return n.validateNodeSchedulingState(ctx, nodeID, "false", "ineligible")
}

func (n *NomadCLI) AdmitRegisteredNode(
	ctx context.Context,
	nodeID, nodeName, privateIP string,
) error {
	payload, err := n.run(ctx, "node", "status", "-json", nodeID)
	if err != nil {
		return err
	}
	var node struct {
		ID                    string            `json:"ID"`
		Name                  string            `json:"Name"`
		Address               string            `json:"Address"`
		NodePool              string            `json:"NodePool"`
		Status                string            `json:"Status"`
		SchedulingEligibility string            `json:"SchedulingEligibility"`
		Meta                  map[string]string `json:"Meta"`
	}
	if err := json.Unmarshal(payload, &node); err != nil {
		return err
	}
	if node.ID != nodeID || node.Name != nodeName || node.Address != privateIP ||
		node.NodePool != n.cli.NodePool || node.Status != "ready" ||
		node.SchedulingEligibility != "ineligible" || node.Meta["sandbox0_admitted"] != "true" {
		return errors.New("nomad node does not present its exact admitted metadata while fenced")
	}
	if _, err := n.run(ctx, "node", "eligibility", "-enable", nodeID); err != nil {
		return err
	}
	return n.validateNodeSchedulingState(ctx, nodeID, "true", "eligible")
}

func (n *NomadCLI) validateNodeSchedulingState(
	ctx context.Context,
	nodeID, admitted, eligibility string,
) error {
	payload, err := n.run(ctx, "node", "status", "-json", nodeID)
	if err != nil {
		return err
	}
	var node struct {
		ID                    string            `json:"ID"`
		Status                string            `json:"Status"`
		SchedulingEligibility string            `json:"SchedulingEligibility"`
		Meta                  map[string]string `json:"Meta"`
	}
	if err := json.Unmarshal(payload, &node); err != nil {
		return err
	}
	if node.ID != nodeID || node.Status != "ready" ||
		node.SchedulingEligibility != eligibility || node.Meta["sandbox0_admitted"] != admitted {
		return errors.New("nomad node scheduling state did not converge")
	}
	return nil
}

func (n *NomadCLI) run(ctx context.Context, args ...string) ([]byte, error) {
	tokenBytes, err := os.ReadFile(n.cli.TokenFile)
	token := strings.TrimSpace(string(tokenBytes))
	if err != nil || len(tokenBytes) == 0 || len(tokenBytes) > 64<<10 || token == "" || len(strings.Fields(token)) != 1 {
		return nil, errors.New("nomad enrollment token file is invalid")
	}
	command := exec.CommandContext(ctx, n.cli.Binary, args...)
	command.Env = []string{
		"NOMAD_ADDR=" + n.cli.Address,
		"NOMAD_REGION=" + n.cli.Region,
		"NOMAD_CACERT=" + n.cli.CACertFile,
		"NOMAD_CLIENT_CERT=" + n.cli.ClientCertFile,
		"NOMAD_CLIENT_KEY=" + n.cli.ClientKeyFile,
		"NOMAD_TOKEN=" + token,
	}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("run Nomad enrollment command: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	if stdout.Len() > 2<<20 {
		return nil, errors.New("nomad enrollment response is too large")
	}
	return stdout.Bytes(), nil
}
