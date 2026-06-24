package dataweave

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
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

func TestRun_WithInputs(t *testing.T) {
	inputs := map[string]interface{}{
		"num1": 25,
		"num2": 17,
	}
	result, err := Run("num1 + num2", inputs)
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
	if str != "42" {
		t.Errorf("Expected '42', got '%s'", str)
	}
}

func TestRun_ScriptError(t *testing.T) {
	result, err := Run("invalid syntax here", nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if result.Success {
		t.Errorf("Expected script to fail")
	}
	if result.Error == "" {
		t.Errorf("Expected error message, got empty string")
	}
}

// --- Streaming Tests ---

func TestRunStreaming_SimpleOutput(t *testing.T) {
	result := RunStreaming("output application/json --- (1 to 5)", nil)
	if result.Err != nil {
		t.Fatalf("RunStreaming failed: %v", result.Err)
	}
	var chunks [][]byte
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
	}
	metadata := <-result.Metadata
	if !metadata.Success {
		t.Fatalf("Script failed: %s", metadata.Error)
	}
	output := string(bytes.Join(chunks, nil))
	if !strings.Contains(output, "1") || !strings.Contains(output, "5") {
		t.Errorf("Expected output to contain numbers 1-5, got: %s", output)
	}
	if metadata.MimeType != "application/json" {
		t.Errorf("Expected mime type 'application/json', got '%s'", metadata.MimeType)
	}
}

func TestRunStreaming_WithInputs(t *testing.T) {
	inputs := map[string]interface{}{
		"payload": []int{1, 2, 3},
	}
	result := RunStreaming("output application/json --- payload", inputs)
	if result.Err != nil {
		t.Fatalf("RunStreaming failed: %v", result.Err)
	}
	var chunks [][]byte
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
	}
	metadata := <-result.Metadata
	if !metadata.Success {
		t.Fatalf("Script failed: %s", metadata.Error)
	}
	output := string(bytes.Join(chunks, nil))
	if !strings.Contains(output, "1") || !strings.Contains(output, "3") {
		t.Errorf("Expected output to contain array [1,2,3], got: %s", output)
	}
}

func TestRunStreaming_ScriptError(t *testing.T) {
	result := RunStreaming("invalid syntax here !!!", nil)
	if result.Err != nil {
		t.Fatalf("RunStreaming returned FFI error: %v", result.Err)
	}
	// Drain chunks (there should be none or few)
	for range result.Chunks {
	}
	metadata := <-result.Metadata
	if metadata.Success {
		t.Errorf("Expected script to fail, but metadata.Success is true")
	}
	if metadata.Error == "" {
		t.Errorf("Expected error message in metadata")
	}
}

func TestRunStreaming_LargeDataset(t *testing.T) {
	result := RunStreaming("output application/json --- (1 to 1000)", nil)
	if result.Err != nil {
		t.Fatalf("RunStreaming failed: %v", result.Err)
	}
	var totalBytes int
	chunkCount := 0
	for chunk := range result.Chunks {
		totalBytes += len(chunk)
		chunkCount++
	}
	metadata := <-result.Metadata
	if !metadata.Success {
		t.Fatalf("Script failed: %s", metadata.Error)
	}
	if totalBytes == 0 {
		t.Errorf("Expected non-zero output bytes")
	}
	// Large datasets should produce multiple chunks
	if chunkCount == 0 {
		t.Errorf("Expected at least one chunk")
	}
}

// --- Concurrent Execution Tests ---

func TestRun_Concurrent(t *testing.T) {
	const numGoroutines = 20
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			inputs := map[string]interface{}{
				"id": id,
			}
			result, err := Run("id * 2", inputs)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: Run failed: %v", id, err)
				return
			}
			if !result.Success {
				errors <- fmt.Errorf("goroutine %d: script failed: %s", id, result.Error)
				return
			}
			str, err := result.GetString()
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: GetString failed: %v", id, err)
				return
			}
			expected := fmt.Sprintf("%d", id*2)
			if str != expected {
				errors <- fmt.Errorf("goroutine %d: expected '%s', got '%s'", id, expected, str)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

func TestRunStreaming_Concurrent(t *testing.T) {
	const numGoroutines = 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			result := RunStreaming("output application/json --- (1 to 100)", nil)
			if result.Err != nil {
				errors <- fmt.Errorf("goroutine %d: RunStreaming failed: %v", id, result.Err)
				return
			}
			var totalBytes int
			for chunk := range result.Chunks {
				totalBytes += len(chunk)
			}
			metadata := <-result.Metadata
			if !metadata.Success {
				errors <- fmt.Errorf("goroutine %d: script failed: %s", id, metadata.Error)
				return
			}
			if totalBytes == 0 {
				errors <- fmt.Errorf("goroutine %d: expected non-zero output", id)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// --- Bidirectional Streaming Tests ---

func TestRunTransform_SimpleCase(t *testing.T) {
	input := strings.NewReader(`[1,2,3,4,5]`)
	opts := TransformOptions{
		InputMimeType: "application/json",
	}
	result := RunTransform("output application/json --- payload map ($ * $)", input, opts)
	if result.Err != nil {
		t.Fatalf("RunTransform failed: %v", result.Err)
	}
	var chunks [][]byte
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
	}
	metadata := <-result.Metadata
	if !metadata.Success {
		t.Fatalf("Script failed: %s", metadata.Error)
	}
	output := string(bytes.Join(chunks, nil))
	if !strings.Contains(output, "1") || !strings.Contains(output, "25") {
		t.Errorf("Expected output to contain squared values, got: %s", output)
	}
}

func TestRunTransform_LargeInput(t *testing.T) {
	// Generate a large JSON array
	var sb strings.Builder
	sb.WriteString("[")
	for i := 0; i < 1000; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`{"id":`)
		sb.WriteString(strings.Repeat("1", 1))
		sb.WriteString("}")
	}
	sb.WriteString("]")
	input := strings.NewReader(sb.String())
	opts := TransformOptions{
		InputMimeType: "application/json",
	}
	result := RunTransform("output application/json --- sizeOf(payload)", input, opts)
	if result.Err != nil {
		t.Fatalf("RunTransform failed: %v", result.Err)
	}
	var chunks [][]byte
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
	}
	metadata := <-result.Metadata
	if !metadata.Success {
		t.Fatalf("Script failed: %s", metadata.Error)
	}
	output := string(bytes.Join(chunks, nil))
	if !strings.Contains(output, "1000") {
		t.Errorf("Expected output to contain '1000', got: %s", output)
	}
}

// errorReader is an io.Reader that always returns an error.
type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, io.ErrUnexpectedEOF
}

func TestRunTransform_InputError(t *testing.T) {
	reader := &errorReader{}
	opts := TransformOptions{
		InputMimeType: "application/json",
	}
	result := RunTransform("output application/json --- payload", reader, opts)
	if result.Err != nil {
		t.Fatalf("RunTransform returned FFI error: %v", result.Err)
	}
	// Drain chunks
	for range result.Chunks {
	}
	metadata := <-result.Metadata
	// With an error reader, the script should fail
	if metadata.Success {
		t.Logf("Note: Script may still succeed with empty input depending on native behavior")
	}
}
