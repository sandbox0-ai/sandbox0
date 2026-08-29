package main

import (
	"fmt"
	"net"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeenrollment"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
)

func configureNodeEnrollment(
	cfg *config.ManagerConfig,
	store nodeenrollment.Store,
) (*nodeenrollment.Server, error) {
	if cfg == nil || !cfg.NodePoolAutoscaler.Enrollment.Enabled {
		return nil, nil
	}
	if !cfg.NodePoolAutoscaler.Enabled {
		return nil, fmt.Errorf("node enrollment requires the node pool autoscaler")
	}
	nodePool := cfg.NodePoolAutoscaler
	enrollment := nodePool.Enrollment
	if strings.TrimSpace(nodePool.Provider) != "aliyun" {
		return nil, fmt.Errorf("node enrollment provider must be aliyun")
	}
	identity, err := nodeenrollment.NewAliyunIdentityVerifier(nodeenrollment.AliyunIdentityPolicy{
		RegionID:       nodePool.Region,
		OwnerAccountID: enrollment.OwnerAccountID,
		ImageID:        enrollment.ImageID,
		InstanceTypes:  enrollment.InstanceTypes,
		SignerCertPEM:  []byte(nodeenrollment.OfficialAliyunInstanceIdentitySignerPEM),
	})
	if err != nil {
		return nil, err
	}
	cloud, err := nodeenrollment.NewAliyunCloud(
		nodePool.Region,
		nodePool.ScalingGroupID,
		enrollment.RouteTableIDs,
	)
	if err != nil {
		return nil, err
	}
	issuer, err := nodeenrollment.NewX509Issuer(nodeenrollment.X509IssuerConfig{
		RegionID:            cfg.RegionID,
		NomadCACertFile:     enrollment.NomadCACertFile,
		NomadCAKeyFile:      enrollment.NomadCAKeyFile,
		AuthorityCACertFile: enrollment.AuthorityCACertFile,
		AuthorityCAKeyFile:  enrollment.AuthorityCAKeyFile,
		ExactTTL:            enrollment.ExactCertificateTTL.Duration,
	})
	if err != nil {
		return nil, err
	}
	nomad, err := nodeenrollment.NewNomadCLI(nodeenrollment.NomadCLIConfig{
		Binary:         enrollment.NomadBinary,
		Address:        enrollment.NomadAddress,
		Region:         strings.ReplaceAll(cfg.RegionID, "-", "_"),
		CACertFile:     enrollment.NomadCACertFile,
		ClientCertFile: enrollment.NomadClientCertFile,
		ClientKeyFile:  enrollment.NomadClientKeyFile,
		TokenFile:      enrollment.NomadTokenFile,
		NodePool:       enrollment.NomadNodePool,
		IntroTTL:       enrollment.NomadIntroductionTTL.Duration,
	})
	if err != nil {
		return nil, err
	}
	runtimeConfig, err := nodeenrollment.NewRuntimeConfigTemplateFromFile(
		enrollment.RuntimeConfigArchiveFile,
		enrollment.ManagerAuthorityURL,
		enrollment.ManagerAuthorityPeerURI,
	)
	if err != nil {
		return nil, err
	}
	runtimeArtifact := nodeenrollment.RuntimeArtifact{
		SourceCommit: enrollment.RuntimeSourceCommit,
		ObjectKey:    enrollment.RuntimeBundleKey,
		SHA256:       enrollment.RuntimeBundleSHA256,
		OSSEndpoint:  enrollment.RuntimeOSSEndpoint,
		OSSBucket:    enrollment.RuntimeOSSBucket,
	}
	if strings.TrimSpace(enrollment.RuntimeManifestFile) != "" {
		if runtimeArtifact.SourceCommit != "" || runtimeArtifact.ObjectKey != "" ||
			runtimeArtifact.SHA256 != "" || runtimeArtifact.OSSEndpoint != "" ||
			runtimeArtifact.OSSBucket != "" {
			return nil, fmt.Errorf("node enrollment runtime manifest cannot be combined with inline artifact fields")
		}
		runtimeArtifact, err = nodeenrollment.LoadRuntimeArtifact(enrollment.RuntimeManifestFile)
		if err != nil {
			return nil, err
		}
	}
	service, err := nodeenrollment.NewService(store, identity, cloud, cloud, nomad, issuer, runtimeConfig, nodeenrollment.Config{
		PoolID:             nodePool.PoolID,
		ClusterID:          cfg.DefaultClusterId,
		RegionID:           cfg.RegionID,
		CloudRegion:        nodePool.Region,
		AllocationSupernet: enrollment.AllocationSupernet,
		AllocationPrefix:   enrollment.AllocationPrefix,
		ChallengeTTL:       enrollment.ChallengeTTL.Duration,
		RuntimeArtifact:    runtimeArtifact,
	})
	if err != nil {
		return nil, err
	}
	handler, err := nodeenrollment.NewHTTPHandler(service)
	if err != nil {
		return nil, err
	}
	listenHost := strings.TrimSpace(enrollment.ListenHost)
	if listenHost == "" {
		listenHost = "0.0.0.0"
	}
	if enrollment.Port <= 0 || enrollment.Port > 65535 {
		return nil, fmt.Errorf("node enrollment port is invalid")
	}
	return nodeenrollment.NewServer(
		net.JoinHostPort(listenHost, fmt.Sprintf("%d", enrollment.Port)),
		enrollment.TLSCertFile,
		enrollment.TLSKeyFile,
		handler,
	)
}
