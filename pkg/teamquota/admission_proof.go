package teamquota

import "time"

// MaxAdmissionProofLifetime bounds a forwarding proof and its distributed
// replay key. The carrying internal token may impose an earlier expiry.
const MaxAdmissionProofLifetime = 2 * time.Minute
