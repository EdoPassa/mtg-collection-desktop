package analysis

import "testing"

func TestListFormatTargets(t *testing.T) {
	targets := ListFormatTargets()
	if len(targets) != 2 {
		t.Fatalf("len = %d, want 2", len(targets))
	}
	if targets[0].ID != FormatStandard || targets[0].Size != 60 {
		t.Fatalf("standard = %#v", targets[0])
	}
	if targets[1].ID != FormatCommander || targets[1].Size != 100 {
		t.Fatalf("commander = %#v", targets[1])
	}
}
