package dataweave

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBasic tests basic script execution
func TestBasic(t *testing.T) {
	result, err := Run("2 + 2", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Success)

	str, err := result.GetString()
	require.NoError(t, err)
	assert.Equal(t, "4", str)
}

// TestWithInputs tests script execution with inputs
func TestWithInputs(t *testing.T) {
	inputs := map[string]interface{}{
		"num1": 25,
		"num2": 17,
	}

	result, err := Run("num1 + num2", inputs)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Success)

	str, err := result.GetString()
	require.NoError(t, err)
	assert.Equal(t, "42", str)
}

// TestContextManager tests explicit lifecycle control with defer
func TestContextManager(t *testing.T) {
	dw, err := New()
	require.NoError(t, err)
	defer dw.Cleanup()

	// First execution
	result, err := dw.Run("sqrt(144)", nil)
	require.NoError(t, err)
	assert.True(t, result.Success)

	str, err := result.GetString()
	require.NoError(t, err)
	assert.Equal(t, "12", str)

	// Second execution on same instance
	result, err = dw.Run("sqrt(10000)", nil)
	require.NoError(t, err)
	assert.True(t, result.Success)

	str, err = result.GetString()
	require.NoError(t, err)
	assert.Equal(t, "100", str)
}

// TestEncoding tests reading UTF-16 XML input and producing CSV output
func TestEncoding(t *testing.T) {
	xmlBytes, err := os.ReadFile("testdata/person.xml")
	require.NoError(t, err)

	script := `output application/csv header=true
---
[payload.person]
`

	inputs := map[string]interface{}{
		"payload": &InputValue{
			Content:  xmlBytes,
			MimeType: "application/xml",
			Charset:  "UTF-16",
		},
	}

	result, err := Run(script, inputs)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Success, "Expected success, got error: %s", result.Error)

	str, err := result.GetString()
	require.NoError(t, err)

	assert.Contains(t, str, "name")
	assert.Contains(t, str, "age")
	assert.Contains(t, str, "Billy")
	assert.Contains(t, str, "31")
}

// TestAutoConversion tests auto-conversion of different types
func TestAutoConversion(t *testing.T) {
	// Test array
	inputs := map[string]interface{}{
		"numbers": []int{1, 2, 3},
	}

	result, err := Run("numbers[0]", inputs)
	require.NoError(t, err)
	assert.True(t, result.Success)

	str, err := result.GetString()
	require.NoError(t, err)
	assert.Equal(t, "1", str)
}

// TestCallbackOutputBasic tests callback-based output streaming
func TestCallbackOutputBasic(t *testing.T) {
	var chunks [][]byte

	writeCallback := func(data []byte) int {
		chunks = append(chunks, data)
		return 0
	}

	result, err := RunCallback("2 + 2", writeCallback, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Success, "Expected success, got error: %s", result.Error)

	full := bytes.Join(chunks, nil)
	charset := result.Charset
	if charset == "" {
		charset = "utf-8"
	}
	text := string(full)
	assert.Equal(t, "4", text)
}

// TestCallbackOutputWithInputs tests callback-based output streaming with inputs
func TestCallbackOutputWithInputs(t *testing.T) {
	var chunks [][]byte

	writeCallback := func(data []byte) int {
		chunks = append(chunks, data)
		return 0
	}

	inputs := map[string]interface{}{
		"num1": 25,
		"num2": 17,
	}

	result, err := RunCallback("num1 + num2", writeCallback, inputs)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Success, "Expected success, got error: %s", result.Error)

	full := bytes.Join(chunks, nil)
	text := string(full)
	assert.Equal(t, "42", text)
}

// TestCallbackInputOutput tests callback-based input and output streaming
func TestCallbackInputOutput(t *testing.T) {
	dw, err := New()
	require.NoError(t, err)
	defer dw.Cleanup()

	source := bytes.NewReader([]byte(`[10, 20, 30, 40, 50]`))

	script := "output application/json\n---\npayload map ($ * 2)"

	// Create input stream channel
	inputStream := make(chan []byte, 1)
	go func() {
		defer close(inputStream)
		for {
			buf := make([]byte, 1024)
			n, err := source.Read(buf)
			if n > 0 {
				inputStream <- buf[:n]
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("Read error: %v", err)
				break
			}
		}
	}()

	result, err := dw.RunTransform(script, inputStream, "payload", "application/json", "", nil)
	require.NoError(t, err)

	// Collect output
	var output []byte
	for chunk := range result.Chunks {
		output = append(output, chunk...)
	}

	metadata := result.Wait()
	require.NotNil(t, metadata)
	assert.True(t, metadata.Success, "Expected success, got error: %s", metadata.Error)

	text := string(output)
	assert.Contains(t, text, "20") // 10 * 2
	assert.Contains(t, text, "100") // 50 * 2
}

// TestRunStreamingBasic tests run_streaming yields chunks and returns metadata
func TestRunStreamingBasic(t *testing.T) {
	stream, err := RunStreaming("output application/json --- {a: 1, b: 2}", nil)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var chunks [][]byte
	for chunk := range stream.Chunks {
		chunks = append(chunks, chunk)
	}

	metadata := stream.Wait()
	require.NotNil(t, metadata)
	assert.True(t, metadata.Success, "Expected success, got error: %s", metadata.Error)
	assert.Equal(t, "application/json", metadata.MimeType)

	full := bytes.Join(chunks, nil)
	text := string(full)
	assert.True(t, strings.Contains(text, `"a": 1`) || strings.Contains(text, `"a":1`))
}

// TestRunStreamingLarge tests run_streaming with large output to verify true streaming
func TestRunStreamingLarge(t *testing.T) {
	script := `output application/json --- (1 to 5000) map {id: $, name: "item_" ++ $}`
	stream, err := RunStreaming(script, nil)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var chunks [][]byte
	for chunk := range stream.Chunks {
		chunks = append(chunks, chunk)
	}

	metadata := stream.Wait()
	require.NotNil(t, metadata)
	assert.True(t, metadata.Success, "Expected success, got error: %s", metadata.Error)
	assert.Greater(t, len(chunks), 1, "Expected multiple chunks for large output")

	full := bytes.Join(chunks, nil)
	text := string(full)
	assert.True(t, strings.Contains(text, `"id": 5000`) || strings.Contains(text, `"id":5000`))
}

// TestRunStreamingError tests run_streaming with an invalid script returns error metadata
func TestRunStreamingError(t *testing.T) {
	stream, err := RunStreaming("output application/json --- invalid_var", nil)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var chunks [][]byte
	for chunk := range stream.Chunks {
		chunks = append(chunks, chunk)
	}

	metadata := stream.Wait()
	require.NotNil(t, metadata)
	assert.False(t, metadata.Success)
	assert.NotEmpty(t, metadata.Error)
	assert.Equal(t, 0, len(chunks), "Expected no chunks on error")
}

// TestRunStreamingWithInputs tests run_streaming with input bindings
func TestRunStreamingWithInputs(t *testing.T) {
	inputs := map[string]interface{}{
		"num1": 25,
		"num2": 17,
	}

	stream, err := RunStreaming("num1 + num2", inputs)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var chunks [][]byte
	for chunk := range stream.Chunks {
		chunks = append(chunks, chunk)
	}

	metadata := stream.Wait()
	require.NotNil(t, metadata)
	assert.True(t, metadata.Success, "Expected success, got error: %s", metadata.Error)

	full := bytes.Join(chunks, nil)
	text := strings.TrimSpace(string(full))
	assert.Equal(t, "42", text)
}

// TestCallbackInputOutputLarge tests callback-based input+output streaming with large data
func TestCallbackInputOutputLarge(t *testing.T) {
	dw, err := New()
	require.NoError(t, err)
	defer dw.Cleanup()

	// Build a large JSON array
	var buf bytes.Buffer
	buf.WriteString("[")
	for i := 1; i <= 1000; i++ {
		if i > 1 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf(`{"id":%d}`, i))
	}
	buf.WriteString("]")

	inputData := buf.Bytes()

	// Create input stream channel
	inputStream := make(chan []byte, 1)
	go func() {
		defer close(inputStream)
		inputStream <- inputData
	}()

	script := "output application/json\n---\nsizeOf(payload)"
	result, err := dw.RunTransform(script, inputStream, "payload", "application/json", "", nil)
	require.NoError(t, err)

	// Collect output
	var output []byte
	for chunk := range result.Chunks {
		output = append(output, chunk...)
	}

	metadata := result.Wait()
	require.NotNil(t, metadata)
	assert.True(t, metadata.Success, "Expected success, got error: %s", metadata.Error)

	text := string(output)
	assert.Equal(t, "1000", text)
}

// TestRunTransformBasic tests run_transform with an input stream and streaming output
func TestRunTransformBasic(t *testing.T) {
	inputData := [][]byte{[]byte(`[10, 20, 30, 40, 50]`)}

	// Create input stream channel
	inputStream := make(chan []byte, len(inputData))
	for _, data := range inputData {
		inputStream <- data
	}
	close(inputStream)

	script := "output application/json\n---\npayload map ($ * 2)"
	stream, err := RunTransform(script, inputStream, "payload", "application/json", "", nil)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var chunks [][]byte
	for chunk := range stream.Chunks {
		chunks = append(chunks, chunk)
	}

	metadata := stream.Wait()
	require.NotNil(t, metadata)
	assert.True(t, metadata.Success, "Expected success, got error: %s", metadata.Error)

	full := bytes.Join(chunks, nil)
	text := string(full)
	assert.Contains(t, text, "20")  // 10 * 2
	assert.Contains(t, text, "100") // 50 * 2
}

// TestRunTransformLarge tests run_transform with large chunked input
func TestRunTransformLarge(t *testing.T) {
	// Build a large JSON array
	var buf bytes.Buffer
	buf.WriteString("[")
	for i := 1; i <= 1000; i++ {
		if i > 1 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf(`{"id":%d}`, i))
	}
	buf.WriteString("]")

	fullInput := buf.Bytes()

	// Feed in 4KB chunks
	inputStream := make(chan []byte, 10)
	go func() {
		defer close(inputStream)
		chunkSize := 4096
		for i := 0; i < len(fullInput); i += chunkSize {
			end := i + chunkSize
			if end > len(fullInput) {
				end = len(fullInput)
			}
			inputStream <- fullInput[i:end]
		}
	}()

	script := "output application/json\n---\nsizeOf(payload)"
	stream, err := RunTransform(script, inputStream, "payload", "application/json", "", nil)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var chunks [][]byte
	for chunk := range stream.Chunks {
		chunks = append(chunks, chunk)
	}

	metadata := stream.Wait()
	require.NotNil(t, metadata)
	assert.True(t, metadata.Success, "Expected success, got error: %s", metadata.Error)

	full := bytes.Join(chunks, nil)
	text := string(full)
	assert.Equal(t, "1000", text)
}

// TestRunTransformWithFile tests run_transform reading from a file
func TestRunTransformWithFile(t *testing.T) {
	file, err := os.Open("testdata/person.xml")
	require.NoError(t, err)
	defer file.Close()

	// Create input stream channel from file
	inputStream := make(chan []byte, 10)
	go func() {
		defer close(inputStream)
		buf := make([]byte, 4096)
		for {
			n, err := file.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				inputStream <- chunk
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("Read error: %v", err)
				break
			}
		}
	}()

	script := "output application/csv header=true\n---\n[payload.person]"
	stream, err := RunTransform(script, inputStream, "payload", "application/xml", "UTF-16", nil)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var chunks [][]byte
	for chunk := range stream.Chunks {
		chunks = append(chunks, chunk)
	}

	metadata := stream.Wait()
	require.NotNil(t, metadata)
	assert.True(t, metadata.Success, "Expected success, got error: %s", metadata.Error)

	full := bytes.Join(chunks, nil)
	text := string(full)
	assert.Contains(t, text, "Billy")
	assert.Contains(t, text, "31")
}

// TestCleanup tests explicit cleanup
func TestCleanup(t *testing.T) {
	// This test just ensures cleanup doesn't panic
	dw, err := New()
	require.NoError(t, err)

	result, err := dw.Run("2 + 2", nil)
	require.NoError(t, err)
	assert.True(t, result.Success)

	dw.Cleanup()

	// Attempting to use after cleanup should error
	_, err = dw.Run("2 + 2", nil)
	assert.Error(t, err)
	assert.Equal(t, ErrNotInitialized, err)
}
