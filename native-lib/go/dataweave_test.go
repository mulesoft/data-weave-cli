package dataweave

import (
	"testing"
)

func TestRun_SimpleArithmetic(t *testing.T) {
	result, err := Run("2 + 2", nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if !result.Success {
		t.Fatalf("Script execution failed: %s", result.Error)
	}
	str, err := result.GetString()
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if str != "4" {
		t.Errorf("Expected '4', got '%s'", str)
	}
}
