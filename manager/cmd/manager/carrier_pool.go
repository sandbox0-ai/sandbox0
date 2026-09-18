package main

import (
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/carrierpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodepoollifecycle"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadclaim"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
)

func configureCarrierPool(cfg *config.ManagerConfig, store carrierpool.Store) (*carrierpool.Worker, error) {
	if cfg == nil || !cfg.CarrierPool.Enabled {
		return nil, nil
	}
	if !cfg.NodePoolAutoscaler.Enrollment.Enabled {
		return nil, fmt.Errorf("adaptive carriers require authenticated Nomad enrollment configuration")
	}
	enrollment := cfg.NodePoolAutoscaler.Enrollment
	nomad, err := nodepoollifecycle.NewNomadClient(nodepoollifecycle.NomadConfig{
		Address: enrollment.NomadAddress, Region: strings.ReplaceAll(cfg.RegionID, "-", "_"),
		CACertFile: enrollment.NomadCACertFile, ClientCertFile: enrollment.NomadClientCertFile,
		ClientKeyFile: enrollment.NomadClientKeyFile, TokenFile: enrollment.NomadTokenFile, WarmJobID: "sandbox0-warm-slots",
	})
	if err != nil {
		return nil, err
	}
	c := cfg.CarrierPool
	classes, err := nomadclaim.LoadRuntimeClassCatalog(cfg.NodeAuthority.Claim.ClassCatalogFile)
	if err != nil {
		return nil, err
	}
	standard, err := classes.Resolve(cfg.DefaultClusterId, "standard")
	if err != nil {
		return nil, err
	}
	privileged, err := classes.Resolve(cfg.DefaultClusterId, "privileged")
	if err != nil {
		return nil, err
	}
	return carrierpool.New(store, nomad, carrierpool.Config{ClusterID: cfg.DefaultClusterId, Maximum: c.Maximum,
		StandardDigest: standard.CompatibilityDigest, PrivilegedDigest: privileged.CompatibilityDigest,
		LowWatermark: c.LowWatermark, Spare: c.Spare, ShrinkAfter: c.ShrinkAfter.Duration, Interval: c.Interval.Duration})
}
