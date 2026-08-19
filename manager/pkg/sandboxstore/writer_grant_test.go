package sandboxstore

import (
	"crypto/sha256"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateIssueRootFSWriterGrantHashesRawToken(t *testing.T) {
	binding := sha256.Sum256([]byte("binding"))
	req := &IssueRootFSWriterGrantRequest{
		GrantID:             "grant-1",
		SandboxID:           "sandbox-1",
		ClaimID:             "claim-1",
		SlotID:              "slot-1",
		OperationID:         "operation-1",
		RawToken:            "0123456789abcdef0123456789abcdef",
		BindingVersion:      RootFSWriterBindingVersion,
		BindingDigest:       binding[:],
		NodeUID:             "node-1",
		NodeBootID:          "boot-1",
		PodNamespace:        "ns-1",
		PodName:             "pod-1",
		PodUID:              "pod-uid-1",
		NodeName:            "node-1",
		GateParent:          "gate-parent-1",
		RuntimeGeneration:   "runtime-1",
		ConsumeExpiresAt:    time.Now().Add(time.Minute),
		ExpectedWriterEpoch: 0,
	}

	normalized, tokenDigest, err := validateIssueRootFSWriterGrantRequest(req)

	require.NoError(t, err)
	assert.Equal(t, sha256.Sum256([]byte(req.RawToken)), tokenDigest)
	assert.Equal(t, req.RawToken, normalized.RawToken)
	assert.NotSame(t, &req.BindingDigest[0], &normalized.BindingDigest[0])
}

func TestPGSandboxStoreHasNoBareWriterRetireCompletionEntryPoint(t *testing.T) {
	storeType := reflect.TypeOf((*PGSandboxStore)(nil))
	_, hasLegacyComplete := storeType.MethodByName("CompleteRootFSWriterRetire")
	_, hasBarePublish := storeType.MethodByName("CompleteRootFSWriterRetireAndPublish")
	_, hasBareCrashAbandon := storeType.MethodByName("CompleteRootFSWriterCrashAbandon")
	_, hasCrashAbandonBegin := storeType.MethodByName("BeginRootFSWriterCrashAbandon")
	assert.False(t, hasLegacyComplete)
	assert.False(t, hasBarePublish)
	assert.False(t, hasBareCrashAbandon)
	assert.True(t, hasCrashAbandonBegin)

	txType := reflect.TypeOf(sandboxStoreTx{})
	_, hasTransactionalPublish := txType.MethodByName("CompleteRootFSWriterRetireAndPublish")
	_, hasTransactionalCrashAbandon := txType.MethodByName("CompleteRootFSWriterCrashAbandon")
	_, hasTransactionalCrashAbandonBegin := txType.MethodByName("BeginRootFSWriterCrashAbandon")
	assert.True(t, hasTransactionalPublish)
	assert.True(t, hasTransactionalCrashAbandon)
	assert.False(t, hasTransactionalCrashAbandonBegin)
}

func TestValidateIssueRootFSWriterGrantRejectsShortSecretsAndDigests(t *testing.T) {
	req := &IssueRootFSWriterGrantRequest{
		GrantID:           "grant-1",
		SandboxID:         "sandbox-1",
		ClaimID:           "claim-1",
		SlotID:            "slot-1",
		OperationID:       "operation-1",
		RawToken:          "too-short",
		BindingVersion:    RootFSWriterBindingVersion,
		BindingDigest:     []byte("too-short"),
		NodeUID:           "node-1",
		NodeBootID:        "boot-1",
		PodNamespace:      "ns-1",
		PodName:           "pod-1",
		PodUID:            "pod-uid-1",
		NodeName:          "node-1",
		GateParent:        "gate-parent-1",
		RuntimeGeneration: "runtime-1",
		ConsumeExpiresAt:  time.Now().Add(time.Minute),
	}

	_, _, err := validateIssueRootFSWriterGrantRequest(req)

	require.Error(t, err)
	assert.ErrorContains(t, err, "raw token")
}

func TestRootFSWriterGrantExactIssueMatchIncludesTokenAndBinding(t *testing.T) {
	binding := sha256.Sum256([]byte("binding"))
	token := "0123456789abcdef0123456789abcdef"
	tokenDigest := sha256.Sum256([]byte(token))
	record := &rootFSWriterGrantRecord{
		RootFSWriterGrant: RootFSWriterGrant{
			ID:                 "grant-1",
			FilesystemID:       "sandbox-1",
			SandboxID:          "sandbox-1",
			ClaimID:            "claim-1",
			SlotID:             "slot-1",
			IssueOperationID:   "operation-1",
			WriterEpoch:        1,
			InitialHeadLayerID: "",
			BindingVersion:     RootFSWriterBindingVersion,
			BindingDigest:      binding[:],
			NodeUID:            "node-1",
			NodeBootID:         "boot-1",
			PodNamespace:       "ns-1",
			PodName:            "pod-1",
			PodUID:             "pod-uid-1",
			NodeName:           "node-1",
			GateParent:         "gate-parent-1",
			RuntimeGeneration:  "runtime-1",
		},
		tokenDigest: tokenDigest[:],
	}
	req := &IssueRootFSWriterGrantRequest{
		GrantID:             "grant-1",
		SandboxID:           "sandbox-1",
		ClaimID:             "claim-1",
		SlotID:              "slot-1",
		OperationID:         "operation-1",
		RawToken:            token,
		BindingVersion:      RootFSWriterBindingVersion,
		BindingDigest:       binding[:],
		NodeUID:             "node-1",
		NodeBootID:          "boot-1",
		PodNamespace:        "ns-1",
		PodName:             "pod-1",
		PodUID:              "pod-uid-1",
		NodeName:            "node-1",
		GateParent:          "gate-parent-1",
		RuntimeGeneration:   "runtime-1",
		ExpectedWriterEpoch: 0,
	}

	assert.True(t, rootFSWriterGrantMatchesIssue(record, req, tokenDigest[:]))
	req.RawToken = "fedcba9876543210fedcba9876543210"
	differentToken := sha256.Sum256([]byte(req.RawToken))
	assert.False(t, rootFSWriterGrantMatchesIssue(record, req, differentToken[:]))
	req.RawToken = token
	differentBinding := sha256.Sum256([]byte("different"))
	req.BindingDigest = differentBinding[:]
	assert.False(t, rootFSWriterGrantMatchesIssue(record, req, tokenDigest[:]))
}

func TestRootFSWriterBindingVersionIsRequiredAcrossGrantTransitions(t *testing.T) {
	binding := sha256.Sum256([]byte("binding"))
	tests := []struct {
		name     string
		validate func() error
	}{
		{
			name: "issue",
			validate: func() error {
				_, _, err := validateIssueRootFSWriterGrantRequest(&IssueRootFSWriterGrantRequest{
					GrantID: "grant-1", SandboxID: "sandbox-1", ClaimID: "claim-1", SlotID: "slot-1",
					OperationID: "issue-1", RawToken: "0123456789abcdef0123456789abcdef",
					BindingDigest: binding[:], NodeUID: "node-1", NodeBootID: "boot-1",
					PodNamespace: "ns-1", PodName: "pod-1", PodUID: "pod-uid-1",
					NodeName: "node-1", GateParent: "gate-parent-1", RuntimeGeneration: "runtime-1",
					ConsumeExpiresAt: time.Now().Add(time.Minute),
				})
				return err
			},
		},
		{
			name: "consume",
			validate: func() error {
				_, _, err := validateConsumeRootFSWriterGrantRequest(&ConsumeRootFSWriterGrantRequest{
					GrantID: "grant-1", WriterEpoch: 1, RawToken: "0123456789abcdef0123456789abcdef",
					BindingDigest: binding[:], ConsumerNodeUID: "node-1",
					ConsumerCtldPodUID: "ctld-1", LeaseTTL: time.Minute,
				})
				return err
			},
		},
		{
			name: "begin retire",
			validate: func() error {
				_, err := validateBeginRootFSWriterRetireRequest(&BeginRootFSWriterRetireRequest{
					GrantID: "grant-1", WriterEpoch: 1, OperationID: "retire-1", BindingDigest: binding[:],
				})
				return err
			},
		},
		{
			name: "begin crash abandon",
			validate: func() error {
				_, err := validateBeginRootFSWriterCrashAbandonRequest(&BeginRootFSWriterCrashAbandonRequest{
					GrantID: "grant-1", WriterEpoch: 1, OperationID: "crash-1",
					BindingDigest: binding[:], NodeUID: "node-1", NodeBootID: "boot-1",
					ExpectedOldGenerationID: "generation-1",
				})
				return err
			},
		},
		{
			name: "complete retire and publish",
			validate: func() error {
				_, err := validateCompleteRootFSWriterRetireAndPublishRequest(&CompleteRootFSWriterRetireAndPublishRequest{
					LifecycleTxnID: "txn-1", GrantID: "grant-1", WriterEpoch: 1,
					OperationID: "retire-1", BindingDigest: binding[:],
				})
				return err
			},
		},
		{
			name: "complete crash abandon",
			validate: func() error {
				_, err := validateCompleteRootFSWriterCrashAbandonRequest(&CompleteRootFSWriterCrashAbandonRequest{
					LifecycleTxnID: "txn-1", GrantID: "grant-1", WriterEpoch: 1,
					OperationID: "txn-1", BindingDigest: binding[:],
					ProofVersion: RootFSWriterCrashAbandonProofVersion, ProofDigest: binding[:],
					NodeUID: "node-1", NodeBootID: "boot-1", ExpectedOldGenerationID: "generation-1",
				})
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.validate()
			require.Error(t, err)
			assert.ErrorContains(t, err, "unsupported binding_version 0")
		})
	}
}

func TestValidateCompleteRootFSWriterCrashAbandonRequiresExactProofAndAllowsRecoveryLifecycle(t *testing.T) {
	binding := sha256.Sum256([]byte("binding"))
	proof := sha256.Sum256([]byte("node-terminal-proof"))
	request := &CompleteRootFSWriterCrashAbandonRequest{
		LifecycleTxnID: "crash-txn", GrantID: "grant-1", WriterEpoch: 1,
		OperationID: "crash-txn", BindingVersion: RootFSWriterBindingVersion,
		BindingDigest: binding[:], ProofVersion: RootFSWriterCrashAbandonProofVersion,
		ProofDigest: proof[:], NodeUID: "node-1", NodeBootID: "boot-1",
		ExpectedOldGenerationID: "generation-old",
	}

	normalized, err := validateCompleteRootFSWriterCrashAbandonRequest(request)
	require.NoError(t, err)
	assert.Equal(t, request.OperationID, normalized.LifecycleTxnID)
	assert.NotSame(t, &request.ProofDigest[0], &normalized.ProofDigest[0])

	wrongOperation := *request
	wrongOperation.OperationID = "another-operation"
	normalized, err = validateCompleteRootFSWriterCrashAbandonRequest(&wrongOperation)
	require.NoError(t, err)
	assert.Equal(t, request.LifecycleTxnID, normalized.LifecycleTxnID)
	assert.Equal(t, wrongOperation.OperationID, normalized.OperationID)

	wrongVersion := *request
	wrongVersion.ProofVersion++
	_, err = validateCompleteRootFSWriterCrashAbandonRequest(&wrongVersion)
	require.ErrorContains(t, err, "unsupported proof_version")

	shortProof := *request
	shortProof.ProofDigest = []byte("short")
	_, err = validateCompleteRootFSWriterCrashAbandonRequest(&shortProof)
	require.ErrorContains(t, err, "proof_digest")
}
