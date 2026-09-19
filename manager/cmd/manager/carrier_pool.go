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
	windows := make([]carrierpool.PrewarmWindow, 0, len(c.PrewarmWindows))
	for _, v := range c.PrewarmWindows {
		class := v.SecurityClass
		if class == "" {
			class = "standard"
		}
		windows = append(windows, carrierpool.PrewarmWindow{Name: v.Name, Start: v.Start, End: v.End, Slots: v.Slots, CPUMillicores: v.CPUMillicores, MemoryBytes: v.MemoryBytes, SecurityClass: class})
	}
	if len(windows) > 0 && !cfg.NodePoolAutoscaler.Enabled {
		return nil, fmt.Errorf("planned prewarm requires an enabled node pool autoscaler")
	}
	return carrierpool.New(store, nomad, carrierpool.Config{ClusterID: cfg.DefaultClusterId, PoolID: cfg.NodePoolAutoscaler.PoolID, PrewarmWindows: windows, Maximum: c.Maximum,
		StandardDigest: standard.CompatibilityDigest, PrivilegedDigest: privileged.CompatibilityDigest,
		LowWatermark: c.LowWatermark, Spare: c.Spare, ShrinkAfter: c.ShrinkAfter.Duration, Interval: c.Interval.Duration})
}
