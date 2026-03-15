package proxy

import (
	"net"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

type decisionAction string

const (
	decisionActionDeny        decisionAction = "deny"
	decisionActionPassThrough decisionAction = "pass-through"
	decisionActionUseAdapter  decisionAction = "use-adapter"
)

type trafficClassification struct {
	Transport     string
	Protocol      string
	DestIP        net.IP
	DestPort      int
	Host          string
	UnknownReason string
}

type trafficDecision struct {
	Action       decisionAction
	Transport    string
	Protocol     string
	Reason       string
	NeedsAdapter bool
}

func classifyKnownTraffic(transport, protocol string, destIP net.IP, destPort int, host string) trafficClassification {
	return trafficClassification{
		Transport: transport,
		Protocol:  protocol,
		DestIP:    destIP,
		DestPort:  destPort,
		Host:      host,
	}
}

func classifyUnknownTraffic(transport, protocol string, destIP net.IP, destPort int, reason string) trafficClassification {
	return trafficClassification{
		Transport:     transport,
		Protocol:      protocol,
		DestIP:        destIP,
		DestPort:      destPort,
		UnknownReason: reason,
	}
}

func decideTraffic(compiled *policy.CompiledPolicy, classification trafficClassification) trafficDecision {
	decision := trafficDecision{
		Transport: classification.Transport,
		Protocol:  classification.Protocol,
	}
	protoForPolicy := classification.Transport
	if protoForPolicy == "" {
		protoForPolicy = "tcp"
	}

	if classification.DestIP != nil && classification.DestPort > 0 {
		if !policy.AllowEgressL4(compiled, classification.DestIP, classification.DestPort, protoForPolicy) {
			decision.Action = decisionActionDeny
			decision.Reason = "l4_denied"
			return decision
		}
	}

	if classification.UnknownReason != "" {
		decision.Reason = classification.UnknownReason
		if policy.UnknownFallbackAction(compiled) == policy.UnknownTrafficPassThrough {
			decision.Action = decisionActionPassThrough
			return decision
		}
		decision.Action = decisionActionDeny
		return decision
	}

	if classification.Host != "" && !policy.AllowEgressDomain(compiled, classification.Host) {
		decision.Action = decisionActionDeny
		decision.Reason = "l7_denied"
		return decision
	}

	decision.Action = decisionActionUseAdapter
	decision.Reason = "allowed"
	decision.NeedsAdapter = true
	return decision
}
