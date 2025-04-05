package sink

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sujanks/data-gen-app/pkg/types"
)

func TestCSVSink(t *testing.T) {
	// Create a temporary directory for test output
	tempDir := t.TempDir()

	// Create a test schema
	schema := &types.Schema{
		Tables: []types.Table{
			{
				Name: "users",
				Columns: []types.Column{
					{Name: "id", Type: "string"},
					{Name: "name", Type: "string"},
					{Name: "age", Type: "int"},
					{Name: "metadata", Type: "json"},
				},
			},
		},
	}

	// Create a new CSV sink
	sink, err := NewCSVSink(tempDir, schema)
	assert.NoError(t, err)
	defer sink.Close()

	// Test inserting records
	records := []map[string]interface{}{
		{
			"id":   "USER001",
			"name": "John Doe",
			"age":  30,
			"metadata": map[string]interface{}{
				"email": "john@example.com",
				"type":  "admin",
			},
		},
		{
			"id":   "USER002",
			"name": "Jane Doe",
			"age":  25,
			"metadata": map[string]interface{}{
				"email": "jane@example.com",
				"type":  "user",
			},
		},
	}

	for _, record := range records {
		err := sink.InsertRecord("users", record)
		assert.NoError(t, err)
	}

	// Flush the writer
	sink.Close()

	// Read and verify the CSV file
	content, err := os.ReadFile(filepath.Join(tempDir, "users.csv"))
	assert.NoError(t, err)

	expected := "id,name,age,metadata\n" +
		"USER001,John Doe,30,\"{email:john@example.com,type:admin}\"\n" +
		"USER002,Jane Doe,25,\"{email:jane@example.com,type:user}\"\n"

	assert.Equal(t, expected, string(content))
}

func TestCSVSinkMultipleTables(t *testing.T) {
	// Create a temporary directory for test output
	tempDir := t.TempDir()

	// Create a test schema
	schema := &types.Schema{
		Tables: []types.Table{
			{
				Name: "users",
				Columns: []types.Column{
					{Name: "id", Type: "string"},
					{Name: "name", Type: "string"},
				},
			},
			{
				Name: "orders",
				Columns: []types.Column{
					{Name: "id", Type: "string"},
					{Name: "user_id", Type: "string"},
				},
			},
		},
	}

	// Create a new CSV sink
	sink, err := NewCSVSink(tempDir, schema)
	assert.NoError(t, err)
	defer sink.Close()

	// Test inserting records to multiple tables
	err = sink.InsertRecord("users", map[string]interface{}{
		"id":   "USER001",
		"name": "John Doe",
	})
	assert.NoError(t, err)

	err = sink.InsertRecord("orders", map[string]interface{}{
		"id":      "ORDER001",
		"user_id": "USER001",
	})
	assert.NoError(t, err)

	// Flush the writers
	sink.Close()

	// Read and verify the users CSV file
	usersContent, err := os.ReadFile(filepath.Join(tempDir, "users.csv"))
	assert.NoError(t, err)
	expectedUsers := "id,name\nUSER001,John Doe\n"
	assert.Equal(t, expectedUsers, string(usersContent))

	// Read and verify the orders CSV file
	ordersContent, err := os.ReadFile(filepath.Join(tempDir, "orders.csv"))
	assert.NoError(t, err)
	expectedOrders := "id,user_id\nORDER001,USER001\n"
	assert.Equal(t, expectedOrders, string(ordersContent))
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{
			name:     "String value",
			input:    "test",
			expected: "test",
		},
		{
			name:     "Integer value",
			input:    42,
			expected: "42",
		},
		{
			name:     "Float value",
			input:    42.123,
			expected: "42.12",
		},
		{
			name:     "Boolean value",
			input:    true,
			expected: "true",
		},
		{
			name:     "Nil value",
			input:    nil,
			expected: "",
		},
		{
			name: "JSON value",
			input: map[string]interface{}{
				"key2": 42,
				"key1": "value1",
			},
			expected: "{key1:value1,key2:42}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
