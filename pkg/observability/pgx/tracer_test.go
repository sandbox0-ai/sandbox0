package pgx

import "testing"

func TestInferOperationTrimsLeadingWhitespace(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{sql: "\n\tselect * from sandboxes", want: "SELECT"},
		{sql: " insert into sandboxes(id) values($1)", want: "INSERT"},
		{sql: "", want: "UNKNOWN"},
	}

	for _, tt := range tests {
		if got := inferOperation(tt.sql); got != tt.want {
			t.Fatalf("inferOperation(%q) = %q, want %q", tt.sql, got, tt.want)
		}
	}
}
