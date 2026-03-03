package template

const (
	DefaultTemplateImage       = "sandbox0ai/otemplates:default-v0.1.0"
	DefaultTemplateCPU         = "500m"
	DefaultTemplateMemory      = "512Mi"
	DefaultTemplateDisplayName = "Default"
	DefaultTemplateMinIdle     = int32(1)
	DefaultTemplateMaxIdle     = int32(5)
)

// ApplyDefaultPool applies default pool values when not explicitly set.
func ApplyDefaultPool(minIdle, maxIdle int32) (int32, int32) {
	if minIdle == 0 && maxIdle == 0 {
		return DefaultTemplateMinIdle, DefaultTemplateMaxIdle
	}
	if minIdle == 0 {
		minIdle = DefaultTemplateMinIdle
	}
	if maxIdle == 0 {
		maxIdle = DefaultTemplateMaxIdle
	}
	return minIdle, maxIdle
}
