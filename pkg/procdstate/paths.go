// Package procdstate defines the durable paths shared by procd and node recovery.
package procdstate

const (
	RootDir                 = "/var/lib/sandbox0/procd"
	ContextStateDir         = RootDir + "/contexts"
	RecoveryRequestFilename = ".context-recovery-requested"
	RecoveryRequestPath     = RootDir + "/" + RecoveryRequestFilename
)
