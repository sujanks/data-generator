package sink

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCSVSink(t *testing.T) {
	// Create temporary directory for test files
	tempDir, err := os.MkdirTemp("", "csv_sink_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create CSV sink
	sink, err := NewCSVSink(tempDir)
	assert.NoError(t, err)
	defer sink.Close()

	// Test data
	testCases := []struct {
		tableName string
		data      map[string]interface{}
	}{
		{
			tableName: "users",
			data: map[string]interface{}{
				"id":   "USER001",
				"name": "John Doe",
				"age":  30,
				"metadata": map[string]interface{}{
					"email": "john@example.com",
					"type":  "admin",
				},
			},
		},
		{
			tableName: "users",
			data: map[string]interface{}{
				"id":   "USER002",
				"name": "Jane Doe",
				"age":  25,
				"metadata": map[string]interface{}{
					"email": "jane@example.com",
					"type":  "user",
				},
			},
		},
	}

	// Insert test records
	for _, tc := range testCases {
		err := sink.InsertRecord(tc.tableName, tc.data)
		assert.NoError(t, err)
	}

	// Close sink to ensure all data is written
	err = sink.Close()
	assert.NoError(t, err)

	// Verify CSV file contents
	csvFile := filepath.Join(tempDir, "users.csv")
	file, err := os.Open(csvFile)
	assert.NoError(t, err)
	defer file.Close()

	reader := csv.NewReader(file)

	// Read and verify headers
	headers, err := reader.Read()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"id", "name", "age", "metadata"}, headers)

	// Read and verify records
	records, err := reader.ReadAll()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(records))

	// Verify first record
	assert.Equal(t, "USER001", records[0][0])
	assert.Equal(t, "John Doe", records[0][1])
	assert.Equal(t, "30", records[0][2])
	assert.Equal(t, "{email:john@example.com,type:admin}", records[0][3])

	// Verify second record
	assert.Equal(t, "USER002", records[1][0])
	assert.Equal(t, "Jane Doe", records[1][1])
	assert.Equal(t, "25", records[1][2])
	assert.Equal(t, "{email:jane@example.com,type:user}", records[1][3])
}

func TestCSVSinkMultipleTables(t *testing.T) {
	// Create temporary directory for test files
	tempDir, err := os.MkdirTemp("", "csv_sink_test_multi")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create CSV sink
	sink, err := NewCSVSink(tempDir)
	assert.NoError(t, err)
	defer sink.Close()

	// Insert records for multiple tables
	tables := map[string][]map[string]interface{}{
		"users": {
			{"id": "U1", "name": "John"},
			{"id": "U2", "name": "Jane"},
		},
		"orders": {
			{"order_id": "O1", "user_id": "U1", "amount": 100.50},
			{"order_id": "O2", "user_id": "U2", "amount": 200.75},
		},
	}

	for tableName, records := range tables {
		for _, record := range records {
			err := sink.InsertRecord(tableName, record)
			assert.NoError(t, err)
		}
	}

	// Close sink
	err = sink.Close()
	assert.NoError(t, err)

	// Verify each table's CSV file
	for tableName := range tables {
		csvFile := filepath.Join(tempDir, tableName+".csv")
		_, err := os.Stat(csvFile)
		assert.NoError(t, err, "CSV file should exist for table: %s", tableName)
	}
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
