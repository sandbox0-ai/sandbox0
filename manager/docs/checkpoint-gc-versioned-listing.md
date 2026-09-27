# Checkpoint GC on versioned object storage

An empty `ListObjectsV2` response with `IsTruncated=true` is not proof that a
capture contains current objects, or proof that the scope is empty. Versioned
OSS can return this response while scanning delete markers. The next opaque
continuation token must be followed before either conclusion is drawn.

Checkpoint image and capture collectors advance one bounded page per pass.
They remember continuation hints only for empty pages, separately for each
exact image/capture prefix, with at most 1,024 hints. A nonempty or final page
clears the hint before any deletion. Eviction or worker restart safely repeats
the scan. Missing or nonadvancing continuation tokens are errors, not permission
to declare completion.

Regional terminal custody remains required before collection. A missing
publication marker with a live capture object still requires independent
capture cleanup authority. Pagination never substitutes for that authority;
valid forks, paused owners and active restores retain their existing protections.

For an already deleted, versioned capture, the old collector could keep reading
the absent publication marker, mistake a truncated empty page for an unbound
live capture, and repeat from the beginning indefinitely. The completion row
then remained unset even though all current objects had been deleted. Existing
rows converge through ordinary workers after the collector update, without
changing historical billing, deleting noncurrent versions manually, or relaxing
database retention predicates.

When investigating this warning, compare exact current-object listings with
version listings and follow `ListObjectsV2` continuation pages. Do not infer
storage bytes from the warning or treat it as an object-store access-denied
response. Noncurrent object expiry remains an independent bucket policy.
