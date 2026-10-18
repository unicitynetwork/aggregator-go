package metrics

import "testing"

func TestNormalizeMethodKnowsBatchProofs(t *testing.T) {
	for _, m := range []string{"get_inclusion_proofs.v1", "get_inclusion_proof.v2"} {
		if got := NormalizeMethod(m); got != m {
			t.Errorf("NormalizeMethod(%q) = %q", m, got)
		}
	}
	if got := NormalizeMethod("nope"); got != "unknown" {
		t.Errorf("unregistered method normalized to %q", got)
	}
}
