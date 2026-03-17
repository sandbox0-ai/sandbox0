package proxy

import (
	"net"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
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
	Verification  string
}

type trafficDecision struct {
	Action           decisionAction
	Transport        string
	Protocol         string
	Reason           string
	ClassifierResult string
	NeedsAdapter     bool
	NeedsEgressAuth  bool
	MatchedAuthRule  *policy.CompiledEgressAuthRule
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
	if classification.UnknownReason != "" {
		decision.ClassifierResult = "unknown"
	} else {
		decision.ClassifierResult = "known"
	}
	protoForPolicy := classification.Transport
	if protoForPolicy == "" {
		protoForPolicy = "tcp"
	}

	if classification.DestIP != nil && classification.DestPort > 0 {
		allowL4 := policy.AllowEgressL4(compiled, classification.DestIP, classification.DestPort, protoForPolicy)
		if classification.UnknownReason == "" {
			allowL4 = policy.AllowEgressDestination(compiled, classification.DestIP, classification.DestPort, protoForPolicy, classification.Host)
		}
		if !allowL4 {
			decision.Action = decisionActionDeny
			decision.Reason = "l4_denied"
			return decision
		}
	}

	if classification.UnknownReason != "" {
		decision.Reason = classification.UnknownReason
		if policy.AllowUnknownEgressFallback(compiled, classification.DestIP, classification.Host) {
			decision.Action = decisionActionPassThrough
			return decision
		}
		decision.Action = decisionActionDeny
		return decision
	}

	if classification.Host != "" && policy.HasDomainRules(compiled) && !policy.AllowEgressDomain(compiled, classification.Host) {
		decision.Action = decisionActionDeny
		decision.Reason = "l7_denied"
		return decision
	}

	if classification.Verification != "" {
		decision.Reason = classification.Verification
		if compiled != nil && compiled.Mode == v1alpha1.NetworkModeBlockAll && policy.HasDomainRules(compiled) {
			decision.Action = decisionActionDeny
			return decision
		}
		decision.Action = decisionActionPassThrough
		return decision
	}

	decision.Action = decisionActionUseAdapter
	decision.Reason = "allowed"
	decision.NeedsAdapter = true
	decision.MatchedAuthRule = policy.MatchEgressAuthRule(compiled, classification.Transport, classification.Protocol, classification.DestPort, classification.Host)
	decision.NeedsEgressAuth = decision.MatchedAuthRule != nil
	return decision
}
