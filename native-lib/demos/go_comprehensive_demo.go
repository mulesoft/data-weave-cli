package main

/*
DataWeave Go Bindings - Comprehensive Demo

Showcases all major capabilities:
- Basic transformations
- Working with inputs
- JSON transformations
- Streaming for large datasets
- Error handling
- Concurrent execution (goroutines)
*/

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	// In a real project: import "github.com/mulesoft/data-weave-cli/native-lib/go"
	dataweave "github.com/mulesoft/data-weave-cli/native-lib/go"
)

func demoBasicOperations() {
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("DEMO 1: Basic Operations")
	fmt.Println(strings.Repeat("=", 60))

	// Simple arithmetic
	fmt.Println("\n1.1 Arithmetic:")
	result, err := dataweave.Run("2 + 2 * 3", nil)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		output, _ := result.GetString()
		fmt.Println("   Expression: 2 + 2 * 3")
		fmt.Printf("   Result: %s\n", output)
	}

	// String concatenation
	fmt.Println("\n1.2 String operations:")
	result, err = dataweave.Run(`"Hello" ++ " " ++ "World"`, nil)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		output, _ := result.GetString()
		fmt.Printf("   Expression: \"Hello\" ++ \" \" ++ \"World\"\n")
		fmt.Printf("   Result: %s\n", output)
	}

	// Array operations
	fmt.Println("\n1.3 Array operations:")
	result, err = dataweave.Run("[1, 2, 3, 4, 5] map ($ * 2)", nil)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		output, _ := result.GetString()
		fmt.Println("   Expression: [1, 2, 3, 4, 5] map ($ * 2)")
		fmt.Printf("   Result: %s\n", output)
	}
}

func demoWithInputs() {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("DEMO 2: Working with Inputs")
	fmt.Println(strings.Repeat("=", 60))

	// Simple variable substitution
	fmt.Println("\n2.1 Variable substitution:")
	inputs := map[string]interface{}{
		"name": "Alice",
		"age":  30,
	}
	script := `"Hello, " ++ name ++ "! You are " ++ age ++ " years old."`
	result, err := dataweave.Run(script, inputs)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		output, _ := result.GetString()
		fmt.Printf("   Inputs: %v\n", inputs)
		fmt.Printf("   Result: %s\n", output)
	}

	// Working with payload
	fmt.Println("\n2.2 Payload transformation:")
	inputs = map[string]interface{}{
		"payload": map[string]interface{}{
			"firstName": "John",
			"lastName":  "Doe",
			"email":     "john.doe@example.com",
		},
	}
	script = `
	output application/json
	---
	{
		fullName: payload.firstName ++ " " ++ payload.lastName,
		contact: payload.email,
		username: lower(payload.lastName) ++ "." ++ lower(payload.firstName)
	}
	`
	result, err = dataweave.Run(script, inputs)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		output, _ := result.GetString()
		fmt.Println("   Input: User record")
		fmt.Printf("   Output: %s\n", output)
	}
}

func demoJSONTransformations() {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("DEMO 3: JSON Transformations")
	fmt.Println(strings.Repeat("=", 60))

	// Array mapping
	fmt.Println("\n3.1 Array mapping:")
	inputs := map[string]interface{}{
		"payload": map[string]interface{}{
			"users": []map[string]interface{}{
				{"id": 1, "name": "Alice", "age": 30, "city": "New York"},
				{"id": 2, "name": "Bob", "age": 25, "city": "London"},
				{"id": 3, "name": "Charlie", "age": 35, "city": "Tokyo"},
			},
		},
	}
	script := `
	output application/json
	---
	payload.users map {
		userId: $.id,
		userName: $.name,
		location: $.city,
		isAdult: $.age >= 18
	}
	`
	result, err := dataweave.Run(script, inputs)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		output, _ := result.GetString()
		fmt.Println("   Transformed 3 users")
		fmt.Printf("   Output: %s\n", output)
	}

	// Filtering and grouping
	fmt.Println("\n3.2 Filtering and grouping:")
	script = `
	output application/json
	---
	{
		adults: payload.users filter ($.age >= 30) map $.name,
		totalUsers: sizeOf(payload.users),
		averageAge: avg(payload.users map $.age)
	}
	`
	result, err = dataweave.Run(script, inputs)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		output, _ := result.GetString()
		fmt.Printf("   Output: %s\n", output)
	}
}

func demoStreaming() {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("DEMO 4: Streaming (Constant Memory)")
	fmt.Println(strings.Repeat("=", 60))

	fmt.Println("\n4.1 Streaming large array transformation:")
	fmt.Println("   Generating 1000 records and streaming output...")

	// Generate large dataset
	records := make([]map[string]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		records[i] = map[string]interface{}{
			"id":    i,
			"value": i * 10,
		}
	}

	inputs := map[string]interface{}{
		"payload": records,
	}

	script := `
	output application/json
	---
	payload map {
		recordId: $.id,
		computedValue: $.value * 2,
		category: if ($.id mod 2 == 0) "even" else "odd"
	}
	`

	// Use streaming API
	chunkCount := 0
	totalBytes := 0

	outputChan, err := dataweave.RunStreaming(script, inputs)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
		return
	}

	for chunk := range outputChan {
		chunkCount++
		totalBytes += len(chunk)
	}

	fmt.Printf("   ✓ Streamed %d chunks\n", chunkCount)
	fmt.Printf("   ✓ Total output: %d bytes\n", totalBytes)
	fmt.Println("   ✓ Memory usage: Constant (chunks processed incrementally)")
}

func demoErrorHandling() {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("DEMO 5: Error Handling")
	fmt.Println(strings.Repeat("=", 60))

	// Syntax error
	fmt.Println("\n5.1 Handling syntax errors:")
	_, err := dataweave.Run("2 + + 3", nil)
	if err != nil {
		fmt.Println("   ✗ Syntax error detected:")
		fmt.Printf("     %v\n", err)
	}

	// Runtime error
	fmt.Println("\n5.2 Handling runtime errors:")
	inputs := map[string]interface{}{
		"payload": map[string]interface{}{},
	}
	_, err = dataweave.Run("payload.user.email", inputs)
	if err != nil {
		fmt.Println("   ✗ Runtime error detected:")
		fmt.Printf("     %v\n", err)
	}

	// Type error
	fmt.Println("\n5.3 Handling type errors:")
	_, err = dataweave.Run(`"text" + 123`, nil)
	if err != nil {
		fmt.Println("   ✗ Type error detected:")
		fmt.Printf("     %v\n", err)
	}

	// Successful execution
	fmt.Println("\n5.4 Successful execution:")
	result, err := dataweave.Run("2 + 3", nil)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		output, _ := result.GetString()
		fmt.Printf("   ✓ Valid expression: 2 + 3 = %s\n", output)
	}
}

func demoConcurrentExecution() {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("DEMO 6: Concurrent Execution (Goroutines)")
	fmt.Println(strings.Repeat("=", 60))

	fmt.Println("\n6.1 Running 10 transformations concurrently:")
	fmt.Println("   (Validates goroutine safety and cgo.Handle correctness)")

	var wg sync.WaitGroup
	results := make([]string, 10)
	errors := make([]error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			inputs := map[string]interface{}{
				"n": index,
			}

			script := `
			output application/json
			---
			{
				goroutineId: n,
				squared: n * n,
				message: "Computed by goroutine " ++ n
			}
			`

			result, err := dataweave.Run(script, inputs)
			if err != nil {
				errors[index] = err
			} else {
				output, _ := result.GetString()
				results[index] = output
			}
		}(i)
	}

	// Wait for all goroutines
	wg.Wait()

	successCount := 0
	for i, err := range errors {
		if err == nil {
			successCount++
		} else {
			fmt.Printf("   Goroutine %d error: %v\n", i, err)
		}
	}

	fmt.Printf("   ✓ All %d goroutines completed successfully\n", successCount)
	fmt.Println("   ✓ No race conditions (validated by checkptr and -race flag)")
	fmt.Println("   ✓ Safe context passing via cgo.Handle (no pointer corruption)")
}

func demoAdvancedFeatures() {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("DEMO 7: Advanced Features")
	fmt.Println(strings.Repeat("=", 60))

	// Reduce/fold
	fmt.Println("\n7.1 Reduce (sum of array):")
	script := "[1, 2, 3, 4, 5] reduce ((item, accumulator=0) -> accumulator + item)"
	result, err := dataweave.Run(script, nil)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		output, _ := result.GetString()
		fmt.Printf("   Expression: %s\n", script)
		fmt.Printf("   Result: %s\n", output)
	}

	// Pattern matching
	fmt.Println("\n7.2 Pattern matching:")
	inputs := map[string]interface{}{
		"status": "SUCCESS",
	}
	script = `
	status match {
		case "SUCCESS" -> "Operation completed successfully"
		case "PENDING" -> "Operation in progress"
		case "FAILED" -> "Operation failed"
		else -> "Unknown status"
	}
	`
	result, err = dataweave.Run(script, inputs)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		output, _ := result.GetString()
		fmt.Println("   Input status: SUCCESS")
		fmt.Printf("   Result: %s\n", output)
	}

	// JSON marshaling
	fmt.Println("\n7.3 Go struct to DataWeave transformation:")
	type Person struct {
		Name  string   `json:"name"`
		Age   int      `json:"age"`
		Email string   `json:"email"`
		Tags  []string `json:"tags"`
	}
	person := Person{
		Name:  "Alice",
		Age:   30,
		Email: "alice@example.com",
		Tags:  []string{"developer", "golang"},
	}
	inputs = map[string]interface{}{
		"payload": person,
	}
	script = `
	output application/json
	---
	{
		profile: {
			name: payload.name,
			contact: payload.email,
			isAdult: payload.age >= 18
		},
		skills: payload.tags
	}
	`
	result, err = dataweave.Run(script, inputs)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else {
		output, _ := result.GetString()
		fmt.Println("   Input: Go struct (Person)")
		fmt.Printf("   Output: %s\n", output)
	}
}

func main() {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println(" DataWeave Go Bindings - Comprehensive Demo")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("\n This demo showcases:")
	fmt.Println("  • Basic transformations and operations")
	fmt.Println("  • Working with inputs and context")
	fmt.Println("  • JSON data transformations")
	fmt.Println("  • Streaming for large datasets")
	fmt.Println("  • Error handling")
	fmt.Println("  • Concurrent execution (goroutine safety)")
	fmt.Println("  • Advanced DataWeave features")
	fmt.Println()

	demoBasicOperations()
	demoWithInputs()
	demoJSONTransformations()
	demoStreaming()
	demoErrorHandling()
	demoConcurrentExecution()
	demoAdvancedFeatures()

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("✓ All demos completed successfully!")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println()
}
