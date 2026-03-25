package pathnorm

func CanonicalPathKey(raw string) string {
	return CompatibilityPathKey(raw, FilesystemCapabilities{
		CaseSensitive:                   false,
		UnicodeNormalizationInsensitive: true,
	})
}
