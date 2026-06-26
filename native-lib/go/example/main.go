package main

import (
	"fmt"
	"log"
	"os"

	dataweave "github.com/mulesoft-labs/data-weave-native/go"
)

func main() {
	fmt.Println("=== DataWeave Go Bindings - Examples ===")

	// Example 1: Basic execution
	fmt.Println("1. Basic execution:")
	result, err := dataweave.Run("2 + 2", nil)
	if err != nil {
		log.Fatal(err)
	}
	output, _ := result.GetString()
	fmt.Printf("   2 + 2 = %s\n\n", output)

	// Example 2: With inputs
	fmt.Println("2. Execution with inputs:")
	inputs := map[string]interface{}{
		"num1": 25,
		"num2": 17,
	}
	result, err = dataweave.Run("num1 + num2", inputs)
	if err != nil {
		log.Fatal(err)
	}
	output, _ = result.GetString()
	fmt.Printf("   num1 + num2 = %s\n\n", output)

	// Example 3: Array manipulation
	fmt.Println("3. Array manipulation:")
	inputs = map[string]interface{}{
		"numbers": []int{10, 20, 30, 40, 50},
	}
	result, err = dataweave.Run("numbers map ($ * 2)", inputs)
	if err != nil {
		log.Fatal(err)
	}
	output, _ = result.GetString()
	fmt.Printf("   numbers map ($ * 2) = %s\n\n", output)

	// Example 4: JSON transformation
	fmt.Println("4. JSON transformation:")
	inputs = map[string]interface{}{
		"user": map[string]interface{}{
			"firstName": "John",
			"lastName":  "Doe",
			"age":       30,
		},
	}
	script := `output application/json
---
{
  fullName: user.firstName ++ " " ++ user.lastName,
  isAdult: user.age >= 18
}`
	result, err = dataweave.Run(script, inputs)
	if err != nil {
		log.Fatal(err)
	}
	output, _ = result.GetString()
	fmt.Printf("   Result: %s\n\n", output)

	// Example 5: Streaming output
	fmt.Println("5. Streaming output (large dataset):")
	stream, err := dataweave.RunStreaming(
		`output application/json --- (1 to 100) map {id: $, name: "item_" ++ $}`,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print("   Streaming chunks: ")
	chunkCount := 0
	for range stream.Chunks {
		chunkCount++
	}
	metadata := stream.Wait()
	fmt.Printf("%d chunks received\n", chunkCount)
	fmt.Printf("   MIME Type: %s\n", metadata.MimeType)
	fmt.Printf("   Success: %v\n\n", metadata.Success)

	// Example 6: Context manager pattern
	fmt.Println("6. Context manager (explicit lifecycle):")
	dw, err := dataweave.New()
	if err != nil {
		log.Fatal(err)
	}
	defer dw.Cleanup()

	result, _ = dw.Run("sqrt(144)", nil)
	output, _ = result.GetString()
	fmt.Printf("   sqrt(144) = %s\n", output)

	result, _ = dw.Run("sqrt(10000)", nil)
	output, _ = result.GetString()
	fmt.Printf("   sqrt(10000) = %s\n\n", output)

	// Example 7: Error handling
	fmt.Println("7. Error handling:")
	result, err = dataweave.Run("invalid_variable", nil)
	if err != nil {
		fmt.Printf("   Error occurred: %v\n", err)
	} else if !result.Success {
		fmt.Printf("   Script failed: %s\n", result.Error)
	}

	fmt.Println("\n=== All examples completed successfully ===")

	// Cleanup global instance
	dataweave.Cleanup()
	os.Exit(0)
}
