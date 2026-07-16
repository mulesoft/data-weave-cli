package main

import (
	"bytes"
	"fmt"
	"log"
	"strings"

	dataweave "github.com/mulesoft/data-weave-cli/native-lib/go"
)

func main() {
	fmt.Println("=== DataWeave Go Streaming Demo ===\n")

	// Example 1: Simple output streaming
	fmt.Println("--- Example 1: Output Streaming ---")
	simpleStreaming()

	// Example 2: Streaming with inputs
	fmt.Println("\n--- Example 2: Streaming with Inputs ---")
	streamingWithInputs()

	// Example 3: Bidirectional streaming
	fmt.Println("\n--- Example 3: Bidirectional Streaming ---")
	bidirectionalStreaming()

	// Example 4: Error handling
	fmt.Println("\n--- Example 4: Error Handling ---")
	errorHandling()
}

func simpleStreaming() {
	result := dataweave.RunStreaming("output application/json --- (1 to 10)", nil)
	if result.Err != nil {
		log.Fatalf("RunStreaming failed: %v", result.Err)
	}

	var chunks [][]byte
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
		fmt.Printf("  Received chunk: %d bytes\n", len(chunk))
	}

	metadata := <-result.Metadata
	if !metadata.Success {
		log.Fatalf("Script failed: %s", metadata.Error)
	}

	output := string(bytes.Join(chunks, nil))
	fmt.Printf("  Output: %s\n", output)
	fmt.Printf("  MimeType: %s, Charset: %s\n", metadata.MimeType, metadata.Charset)
}

func streamingWithInputs() {
	inputs := map[string]interface{}{
		"payload": map[string]interface{}{
			"users": []map[string]interface{}{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
				{"id": 3, "name": "Charlie"},
			},
		},
	}

	script := `output application/json --- payload.users map { name: $.name }`
	result := dataweave.RunStreaming(script, inputs)
	if result.Err != nil {
		log.Fatalf("RunStreaming failed: %v", result.Err)
	}

	var chunks [][]byte
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
	}

	metadata := <-result.Metadata
	if !metadata.Success {
		log.Fatalf("Script failed: %s", metadata.Error)
	}

	fmt.Printf("  Output: %s\n", string(bytes.Join(chunks, nil)))
}

func bidirectionalStreaming() {
	input := strings.NewReader(`[1,2,3,4,5]`)
	opts := dataweave.TransformOptions{
		InputMimeType: "application/json",
	}

	result := dataweave.RunTransform(
		"output application/json --- payload map ($ * $)",
		input,
		opts,
	)
	if result.Err != nil {
		log.Fatalf("RunTransform failed: %v", result.Err)
	}

	var chunks [][]byte
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
		fmt.Printf("  Received chunk: %d bytes\n", len(chunk))
	}

	metadata := <-result.Metadata
	if !metadata.Success {
		log.Fatalf("Script failed: %s", metadata.Error)
	}

	fmt.Printf("  Output (squares): %s\n", string(bytes.Join(chunks, nil)))
}

func errorHandling() {
	result := dataweave.RunStreaming("this is invalid !!!", nil)
	if result.Err != nil {
		fmt.Printf("  FFI error: %v\n", result.Err)
		return
	}

	// Drain any chunks
	for range result.Chunks {
	}

	metadata := <-result.Metadata
	if !metadata.Success {
		fmt.Printf("  Script error (expected): %s\n", metadata.Error)
	} else {
		fmt.Println("  Script unexpectedly succeeded")
	}
}
