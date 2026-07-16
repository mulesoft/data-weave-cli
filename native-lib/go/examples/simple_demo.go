package main

import (
	"fmt"
	"log"

	dataweave "github.com/mulesoft/data-weave-cli/native-lib/go"
)

func main() {
	fmt.Println("=== DataWeave Go Demo ===\n")

	// Example 1: Simple arithmetic
	fmt.Println("1. Simple arithmetic:")
	result, err := dataweave.Run("2 + 2", nil)
	if err != nil {
		log.Fatalf("Failed to run script: %v", err)
	}
	if !result.Success {
		log.Fatalf("Script failed: %s", result.Error)
	}
	output, _ := result.GetString()
	fmt.Printf("   2 + 2 = %s\n\n", output)

	// Example 2: With inputs
	fmt.Println("2. Script with inputs:")
	inputs := map[string]interface{}{
		"name": "World",
	}
	result, err = dataweave.Run(`"Hello, " ++ name ++ "!"`, inputs)
	if err != nil {
		log.Fatalf("Failed to run script: %v", err)
	}
	if !result.Success {
		log.Fatalf("Script failed: %s", result.Error)
	}
	output, _ = result.GetString()
	fmt.Printf("   %s\n\n", output)

	// Example 3: JSON transformation
	fmt.Println("3. JSON transformation:")
	inputs = map[string]interface{}{
		"payload": map[string]interface{}{
			"users": []map[string]interface{}{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			},
		},
	}
	script := `output application/json --- payload.users map { name: $.name }`
	result, err = dataweave.Run(script, inputs)
	if err != nil {
		log.Fatalf("Failed to run script: %v", err)
	}
	if !result.Success {
		log.Fatalf("Script failed: %s", result.Error)
	}
	output, _ = result.GetString()
	fmt.Printf("   %s\n\n", output)

	fmt.Println("Demo complete!")
}
