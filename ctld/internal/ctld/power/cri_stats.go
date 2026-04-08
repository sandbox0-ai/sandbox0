package power

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const defaultCRIEndpoint = "/host-run/containerd/containerd.sock"
const defaultCRITimeout = 2 * time.Second

type criRuntimeService interface {
	ListPodSandbox(ctx context.Context, in *runtimeapi.ListPodSandboxRequest, opts ...grpc.CallOption) (*runtimeapi.ListPodSandboxResponse, error)
	PodSandboxStats(ctx context.Context, in *runtimeapi.PodSandboxStatsRequest, opts ...grpc.CallOption) (*runtimeapi.PodSandboxStatsResponse, error)
}

type CRIStatsProvider struct {
	Endpoint    string
	Timeout     time.Duration
	Client      criRuntimeService
	DialContext func(ctx context.Context, endpoint string) (*grpc.ClientConn, error)
}

func NewCRIStatsProvider(endpoint string) *CRIStatsProvider {
	return &CRIStatsProvider{Endpoint: endpoint, Timeout: defaultCRITimeout}
}

func (p *CRIStatsProvider) SandboxResourceUsage(ctx context.Context, target Target) (*ctldapi.SandboxResourceUsage, error) {
	if strings.TrimSpace(target.PodNamespace) == "" || strings.TrimSpace(target.PodName) == "" || strings.TrimSpace(target.PodUID) == "" {
		return nil, fmt.Errorf("sandbox target is missing pod identity")
	}

	client, conn, err := p.runtimeClient(ctx)
	if err != nil {
		return nil, err
	}
	if conn != nil {
		defer conn.Close()
	}

	podSandbox, err := p.findPodSandbox(ctx, client, target)
	if err != nil {
		return nil, err
	}
	statsResp, err := client.PodSandboxStats(ctx, &runtimeapi.PodSandboxStatsRequest{PodSandboxId: podSandbox.Id})
	if err != nil {
		return nil, fmt.Errorf("get pod sandbox stats for %s/%s: %w", target.PodNamespace, target.PodName, err)
	}
	usage := sandboxUsageFromPodSandboxStats(statsResp.GetStats())
	if usage == nil {
		return nil, fmt.Errorf("pod sandbox stats for %s/%s returned no usage", target.PodNamespace, target.PodName)
	}
	return usage, nil
}

func (p *CRIStatsProvider) runtimeClient(ctx context.Context) (criRuntimeService, *grpc.ClientConn, error) {
	if p != nil && p.Client != nil {
		return p.Client, nil, nil
	}
	endpoint := defaultCRIEndpoint
	if p != nil && strings.TrimSpace(p.Endpoint) != "" {
		endpoint = strings.TrimSpace(p.Endpoint)
	}
	endpoint = normalizeCRIEndpoint(endpoint)
	timeout := defaultCRITimeout
	if p != nil && p.Timeout > 0 {
		timeout = p.Timeout
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dialer := dialCRIEndpoint
	if p != nil && p.DialContext != nil {
		dialer = p.DialContext
	}
	conn, err := dialer(dialCtx, endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("dial cri endpoint %s: %w", endpoint, err)
	}
	return runtimeapi.NewRuntimeServiceClient(conn), conn, nil
}

func (p *CRIStatsProvider) findPodSandbox(ctx context.Context, client criRuntimeService, target Target) (*runtimeapi.PodSandbox, error) {
	listResp, err := client.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{})
	if err != nil {
		return nil, fmt.Errorf("list pod sandboxes: %w", err)
	}
	for _, podSandbox := range listResp.GetItems() {
		metadata := podSandbox.GetMetadata()
		if metadata == nil {
			continue
		}
		if metadata.GetNamespace() != target.PodNamespace || metadata.GetName() != target.PodName || metadata.GetUid() != target.PodUID {
			continue
		}
		return podSandbox, nil
	}
	return nil, ErrSandboxNotFound
}

func sandboxUsageFromPodSandboxStats(stats *runtimeapi.PodSandboxStats) *ctldapi.SandboxResourceUsage {
	if stats == nil {
		return nil
	}
	linux := stats.GetLinux()
	if linux == nil {
		return nil
	}
	usage := &ctldapi.SandboxResourceUsage{}
	memory := linux.GetMemory()
	if memory != nil {
		usage.ContainerMemoryUsage = int64(memory.GetUsageBytes().GetValue())
		usage.ContainerMemoryWorkingSet = int64(memory.GetWorkingSetBytes().GetValue())
		usage.TotalMemoryRSS = int64(memory.GetRssBytes().GetValue())
		limitBase := usage.ContainerMemoryUsage
		if usage.ContainerMemoryWorkingSet > limitBase {
			limitBase = usage.ContainerMemoryWorkingSet
		}
		usage.ContainerMemoryLimit = limitBase + int64(memory.GetAvailableBytes().GetValue())
	}
	process := linux.GetProcess()
	if process != nil {
		usage.TotalThreadCount = int(process.GetProcessCount().GetValue())
	}
	if isZeroSandboxUsage(usage) {
		return nil
	}
	return usage
}

func dialCRIEndpoint(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}

func normalizeCRIEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "unix://" + defaultCRIEndpoint
	}
	if strings.Contains(endpoint, "://") {
		return endpoint
	}
	if strings.HasPrefix(endpoint, "/") {
		return "unix://" + endpoint
	}
	return endpoint
}

func isZeroSandboxUsage(usage *ctldapi.SandboxResourceUsage) bool {
	if usage == nil {
		return true
	}
	return usage.ContainerMemoryUsage == 0 && usage.ContainerMemoryLimit == 0 && usage.ContainerMemoryWorkingSet == 0 && usage.TotalMemoryRSS == 0 && usage.TotalMemoryVMS == 0 && usage.TotalOpenFiles == 0 && usage.TotalThreadCount == 0 && usage.TotalIOReadBytes == 0 && usage.TotalIOWriteBytes == 0 && usage.ContextCount == 0 && usage.RunningContextCount == 0 && usage.PausedContextCount == 0
}
