package setup

// TemplateFixture represents a reusable template for integration tests.
type TemplateFixture struct {
	ID       string
	Image    string
	Command  string
	Metadata map[string]string
}

// DefaultTemplateFixture provides a baseline template definition.
func DefaultTemplateFixture() TemplateFixture {
	return TemplateFixture{
		ID:      "tpl-basic",
		Image:   "sandbox0ai/infra:latest",
		Command: "sleep 300",
		Metadata: map[string]string{
			"owner": "tests",
		},
	}
}
