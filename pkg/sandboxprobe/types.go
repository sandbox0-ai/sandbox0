package sandboxprobe

type Kind string

const (
	KindStartup   Kind = "startup"
	KindReadiness Kind = "readiness"
	KindLiveness  Kind = "liveness"
)

type Status string

const (
	StatusPassed    Status = "passed"
	StatusFailed    Status = "failed"
	StatusSuspended Status = "suspended"
)

type Response struct {
	Kind    Kind          `json:"kind"`
	Status  Status        `json:"status"`
	Reason  string        `json:"reason,omitempty"`
	Message string        `json:"message,omitempty"`
	Checks  []CheckResult `json:"checks,omitempty"`
}

type CheckResult struct {
	Name    string `json:"name"`
	Target  string `json:"target"`
	Status  Status `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

func Passed(kind Kind, reason, message string, checks []CheckResult) Response {
	return Response{Kind: kind, Status: StatusPassed, Reason: reason, Message: message, Checks: checks}
}

func Failed(kind Kind, reason, message string, checks []CheckResult) Response {
	return Response{Kind: kind, Status: StatusFailed, Reason: reason, Message: message, Checks: checks}
}

func Suspended(kind Kind, reason, message string, checks []CheckResult) Response {
	return Response{Kind: kind, Status: StatusSuspended, Reason: reason, Message: message, Checks: checks}
}

func ValidKind(kind Kind) bool {
	switch kind {
	case KindStartup, KindReadiness, KindLiveness:
		return true
	default:
		return false
	}
}
