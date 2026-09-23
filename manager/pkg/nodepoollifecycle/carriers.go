package nodepoollifecycle

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/carrierpool"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadinventory"
)

const carrierNodeAttribute = "${node.unique.id}"
const carrierEmptyNodes = "00000000-0000-0000-0000-000000000000"

type carrierConstraint struct{ LTarget, RTarget, Operand string }
type carrierGroup struct {
	Name        string
	Constraints []carrierConstraint
}
type carrierJob struct {
	ID, Type, NodePool, Namespace string
	Stop                          bool
	JobModifyIndex                uint64
	Meta                          map[string]string
	TaskGroups                    []json.RawMessage
}

// carrierJobs reads exact known IDs only. Raw messages preserve every runtime,
// networking and resource setting when only placement membership is updated.
func (n *NomadClient) carrierJobs(ctx context.Context) ([]map[string]json.RawMessage, error) {
	var catalog []struct {
		ID   string
		Stop bool
	}
	// Filter locally after a bounded read; the shared request reader rejects
	// truncation. Unexpected family members fail closed instead of being ignored.
	if err := n.request(ctx, http.MethodGet, "/v1/jobs", nil, &catalog); err != nil {
		return nil, err
	}
	var result []map[string]json.RawMessage
	seen := map[string]bool{}
	for _, row := range catalog {
		if !strings.HasPrefix(row.ID, n.warmJobID) {
			continue
		}
		if !nomadinventory.IsWarmJob(n.warmJobID, row.ID) || seen[row.ID] || row.Stop {
			return nil, fmt.Errorf("invalid adaptive carrier catalog")
		}
		seen[row.ID] = true
		var raw map[string]json.RawMessage
		if err := n.request(ctx, http.MethodGet, "/v1/job/"+row.ID, nil, &raw); err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(raw)
		if err != nil {
			return nil, err
		}
		var job carrierJob
		if err := json.Unmarshal(encoded, &job); err != nil {
			return nil, err
		}
		if job.ID != row.ID || job.Type != "system" || job.NodePool != "sandbox0" || job.Namespace != "default" || job.Stop ||
			job.JobModifyIndex == 0 || job.Meta["sandbox0_adaptive_carriers"] != "v1" || len(job.TaskGroups) > 32 || len(job.TaskGroups) == 0 {
			return nil, fmt.Errorf("carrier job has no validated adaptive placement contract")
		}
		result = append(result, raw)
	}
	if !seen[n.warmJobID] || len(result) > nomadinventory.WarmJobShardCount {
		return nil, fmt.Errorf("incomplete adaptive carrier family")
	}
	sort.Slice(result, func(i, j int) bool { return string(result[i]["ID"]) < string(result[j]["ID"]) })
	return result, nil
}

func (n *NomadClient) CarrierCatalog(ctx context.Context, node string) ([]string, error) {
	catalog, _, err := n.carrierCatalog(ctx, node)
	return catalog, err
}

// carrierCatalog returns the validated jobs with their catalog so a resize uses
// the same snapshot for validation and CAS updates. Refetching the full family
// here doubles serialization and network work on every pending resize retry.
func (n *NomadClient) carrierCatalog(ctx context.Context, node string) ([]string, []map[string]json.RawMessage, error) {
	standard, privileged := 512, 256
	if node != "" {
		id, err := uuid.Parse(node)
		if err != nil || id.String() != node {
			return nil, nil, fmt.Errorf("invalid carrier node identity")
		}
		if _, reserved, identityErr := carrierEpochMember(id); identityErr != nil || reserved {
			return nil, nil, fmt.Errorf("carrier node identity is reserved")
		}
		var host struct {
			ID, NodePool string
			Meta         map[string]string
		}
		if err := n.request(ctx, http.MethodGet, "/v1/node/"+node, nil, &host); err != nil {
			return nil, nil, err
		}
		if host.ID != node || host.NodePool != "sandbox0" || host.Meta["sandbox0_dedicated"] != "true" {
			return nil, nil, fmt.Errorf("carrier node is outside dedicated pool")
		}
		standard, privileged = 6, 2
		for key, destination := range map[string]*int{"sandbox0_standard_carriers": &standard, "sandbox0_privileged_carriers": &privileged} {
			if value := host.Meta[key]; value != "" {
				parsed, err := strconv.Atoi(value)
				if err != nil || parsed < 0 || parsed > 512 {
					return nil, nil, fmt.Errorf("invalid node carrier ceiling")
				}
				*destination = parsed
			}
		}
	}
	jobs, err := n.carrierJobs(ctx)
	if err != nil {
		return nil, nil, err
	}
	result := []string{}
	seen := map[string]bool{}
	for _, raw := range jobs {
		var groups []carrierGroup
		if err := json.Unmarshal(raw["TaskGroups"], &groups); err != nil {
			return nil, nil, err
		}
		for _, g := range groups {
			class, index, err := carrierpool.GroupIndex(g.Name)
			if err != nil {
				return nil, nil, err
			}
			if seen[g.Name] {
				return nil, nil, fmt.Errorf("duplicate adaptive carrier group")
			}
			seen[g.Name] = true
			extra := index >= 6
			if class == "privileged" {
				extra = index >= 2
			}
			if extra {
				if len(g.Constraints) != 2 || g.Constraints[0].LTarget != "${meta.sandbox0_"+class+"_carriers}" ||
					g.Constraints[0].Operand != ">=" || g.Constraints[0].RTarget != strconv.Itoa(index+1) ||
					g.Constraints[1].LTarget != carrierNodeAttribute || g.Constraints[1].Operand != "set_contains_any" {
					return nil, nil, fmt.Errorf("adaptive carrier group lost its capacity or membership constraint")
				}
				if _, err := carrierMembers(g.Constraints[1].RTarget); err != nil {
					return nil, nil, err
				}
			} else if len(g.Constraints) != 0 {
				return nil, nil, fmt.Errorf("enrollment anchor changed")
			}
			if !extra || (class == "standard" && index < standard) || (class == "privileged" && index < privileged) {
				result = append(result, g.Name)
			}
		}
	}
	return result, jobs, nil
}

func carrierMembers(value string) (map[string]bool, error) {
	result := map[string]bool{}
	for _, member := range strings.Split(value, ",") {
		id, err := uuid.Parse(member)
		if err != nil || id.String() != member || result[member] {
			return nil, fmt.Errorf("invalid carrier node membership")
		}
		result[member] = true
		if epoch, reserved, memberErr := carrierEpochMember(id); memberErr != nil {
			return nil, memberErr
		} else if reserved && epoch > 0 {
			delete(result, member)
			continue
		}
	}
	delete(result, carrierEmptyNodes)
	if len(result) > 300 {
		return nil, fmt.Errorf("carrier node membership exceeds bound")
	}
	return result, nil
}

func carrierEpochMarker(revision int64) (string, error) {
	if revision <= 0 {
		return "", fmt.Errorf("invalid carrier constraint epoch")
	}
	value := uint64(revision)
	return fmt.Sprintf("%08x-%04x-%04x-0000-000000000000",
		value>>32, (value>>16)&0xffff, value&0xffff), nil
}

func carrierEpochMember(id uuid.UUID) (epoch int64, reserved bool, err error) {
	if binary.BigEndian.Uint64(id[8:]) != 0 {
		return 0, false, nil
	}
	value := binary.BigEndian.Uint64(id[:8])
	if value == 0 {
		return 0, true, nil
	}
	if value > uint64(^uint64(0)>>1) {
		return 0, true, fmt.Errorf("carrier constraint epoch exceeds bound")
	}
	return int64(value), true, nil
}

func carrierGroupEpoch(group carrierGroup) (int64, error) {
	result := int64(0)
	if len(group.Constraints) != 2 {
		return result, nil
	}
	markers := 0
	for _, member := range strings.Split(group.Constraints[1].RTarget, ",") {
		id, err := uuid.Parse(member)
		if err != nil || id.String() != member {
			return 0, fmt.Errorf("invalid carrier constraint epoch membership")
		}
		epoch, reserved, err := carrierEpochMember(id)
		if err != nil {
			return 0, err
		}
		if !reserved || epoch == 0 {
			continue
		}
		if markers != 0 {
			return 0, fmt.Errorf("carrier constraint has multiple epochs")
		}
		markers++
		result = epoch
	}
	return result, nil
}

func (n *NomadClient) CarrierAllocations(ctx context.Context, node string) ([]nomadinventory.Allocation, error) {
	rows, err := n.allocations(ctx, node)
	if err != nil {
		return nil, err
	}
	result := make([]nomadinventory.Allocation, 0, len(rows))
	for _, a := range rows {
		if !nomadinventory.IsWarmJob(n.warmJobID, a.JobID) || a.Namespace != "default" {
			return nil, fmt.Errorf("node has foreign allocations")
		}
		if _, _, err := carrierpool.GroupIndex(a.TaskGroup); err != nil {
			return nil, err
		}
		result = append(result, nomadinventory.Allocation(a))
	}
	return result, nil
}

// ApplyCarrierPlan rejects stale writers twice: changed shards retain a global
// monotonic epoch, and Nomad EnforceIndex rejects requests delayed past an
// intervening registration. Unchanged shards need no additional evaluation.
func (n *NomadClient) ApplyCarrierPlan(ctx context.Context, node string, revision int64, groups []string) error {
	id, err := uuid.Parse(node)
	if err != nil || id.String() != node || revision <= 0 {
		return fmt.Errorf("invalid carrier resize identity")
	}
	catalogNode := node
	anchorsOnly := len(groups) == 2 || len(groups) == 8
	for _, g := range groups {
		class, index, err := carrierpool.GroupIndex(g)
		if err != nil {
			return err
		}
		anchorsOnly = anchorsOnly && ((class == "standard" && index < 6) || (class == "privileged" && index < 2))
	}
	if anchorsOnly {
		catalogNode = ""
	}
	catalog, jobs, err := n.carrierCatalog(ctx, catalogNode)
	if err != nil {
		return err
	}
	known := map[string]bool{}
	for _, g := range catalog {
		known[g] = true
	}
	for _, g := range groups {
		if !known[g] {
			return fmt.Errorf("carrier plan exceeds node placement ceiling")
		}
	}
	allowed := map[string]bool{}
	for _, g := range groups {
		allowed[g] = true
	}
	if len(allowed) != len(groups) || len(groups) > 576 {
		return fmt.Errorf("invalid carrier plan inventory")
	}
	firstAnchor := 0
	if !known["warm-0"] {
		firstAnchor = 6
	}
	for i := firstAnchor; i < 8; i++ {
		if !allowed[fmt.Sprintf("warm-%d", i)] {
			return fmt.Errorf("carrier plan lost enrollment anchors")
		}
	}
	key := "sandbox0_carrier_epoch"
	for _, raw := range jobs {
		var meta map[string]string
		if err := json.Unmarshal(raw["Meta"], &meta); err != nil {
			return err
		}
		previous := int64(0)
		if value := meta[key]; value != "" {
			previous, err = strconv.ParseInt(value, 10, 64)
			if err != nil || previous <= 0 {
				return fmt.Errorf("invalid carrier epoch")
			}
		}
		previousOwner := meta["sandbox0_carrier_node"]
		var taskGroups []map[string]json.RawMessage
		if err := json.Unmarshal(raw["TaskGroups"], &taskGroups); err != nil {
			return err
		}
		// Job-level metadata is shared by every task group in a shard. Keep it
		// immutable for compatibility, and fence newer writes with a reserved
		// non-node member in the placement set. An existing real node remains a
		// matching set member, so preserving its allocation needs no task update.
		for _, group := range taskGroups {
			var identity carrierGroup
			if err := json.Unmarshal(group["Name"], &identity.Name); err != nil {
				return err
			}
			if value := group["Constraints"]; value != nil {
				if err := json.Unmarshal(value, &identity.Constraints); err != nil {
					return err
				}
			}
			groupEpoch, groupEpochErr := carrierGroupEpoch(identity)
			if groupEpochErr != nil {
				return groupEpochErr
			}
			if groupEpoch > previous {
				previousOwner = ""
			}
			previous = max(previous, groupEpoch)
		}
		if previous > revision {
			return fmt.Errorf("stale carrier resize revision")
		}
		if previous == revision && previousOwner != "" && previousOwner != node {
			return fmt.Errorf("carrier epoch belongs to another node")
		}
		changed := false
		for _, group := range taskGroups {
			var name string
			var constraints []carrierConstraint
			if err := json.Unmarshal(group["Name"], &name); err != nil {
				return err
			}
			if value := group["Constraints"]; value != nil {
				if err := json.Unmarshal(value, &constraints); err != nil {
					return err
				}
			}
			if len(constraints) == 0 {
				continue
			}
			if len(constraints) != 2 {
				return fmt.Errorf("adaptive carrier constraints changed")
			}
			members, err := carrierMembers(constraints[1].RTarget)
			if err != nil {
				return err
			}
			if members[node] == allowed[name] {
				continue
			}
			if previous == revision {
				return fmt.Errorf("same carrier epoch has conflicting intent")
			}
			changed = true
			if allowed[name] {
				members[node] = true
			} else {
				delete(members, node)
			}
			names := make([]string, 0, len(members))
			for member := range members {
				names = append(names, member)
			}
			sort.Strings(names)
			marker, markerErr := carrierEpochMarker(revision)
			if markerErr != nil {
				return markerErr
			}
			names = append(names, marker)
			sort.Strings(names)
			constraints[1].RTarget = strings.Join(names, ",")
			group["Constraints"], err = json.Marshal(constraints)
			if err != nil {
				return err
			}
		}
		// Unchanged shards need no registration: any delayed earlier mutation
		// carries the pre-acknowledgement JobModifyIndex and therefore cannot
		// overwrite the acknowledged state. Avoid generating empty evaluations
		// for every high-density shard on each small ready-buffer refill.
		if !changed {
			continue
		}
		raw["TaskGroups"], err = json.Marshal(taskGroups)
		if err != nil {
			return err
		}
		var index uint64
		if err := json.Unmarshal(raw["JobModifyIndex"], &index); err != nil {
			return err
		}
		request := struct {
			Job            map[string]json.RawMessage
			EnforceIndex   bool
			JobModifyIndex uint64
		}{raw, true, index}
		if err := n.request(ctx, http.MethodPost, "/v1/jobs", request, nil); err != nil {
			return err
		}
	}
	return nil
}
