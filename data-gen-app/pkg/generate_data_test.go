package pkg

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// MockDataSink is a mock implementation of the DataSink interface
type MockDataSink struct {
	Records []map[string]interface{}
}

func (m *MockDataSink) InsertRecord(tableName string, data map[string]interface{}) error {
	m.Records = append(m.Records, data)
	return nil
}

func TestGenerateData(t *testing.T) {
	// Create a temporary manifest file for testing
	manifestContent := `
tables:
- name: test_table
  priority: 1
  columns:
  - name: id
    pattern: "TEST####"
    parent: true
    validation:
      unique: true
  - name: timestamp
    type: timestamp
    format: "2006-01-02 15:04:05"
  - name: name
    type: string
    value: ["Test User 1", "Test User 2"]
  - name: age
    type: int
    range:
      min: 18
      max: 65
`
	tmpfile, err := os.CreateTemp("", "test_manifest*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(manifestContent)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	// Test cases
	tests := []struct {
		name      string
		count     int
		wantError bool
	}{
		{
			name:      "Generate 10 records",
			count:     10,
			wantError: false,
		},
		{
			name:      "Generate 0 records",
			count:     0,
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSink := &MockDataSink{
				Records: make([]map[string]interface{}, 0),
			}

			GenerateData(mockSink, tt.count, tmpfile.Name())

			// Verify the number of records generated
			assert.Equal(t, tt.count, len(mockSink.Records))

			// Verify the data format for each record
			for _, record := range mockSink.Records {
				// Check ID pattern
				id, ok := record["id"].(string)
				assert.True(t, ok)
				assert.Regexp(t, "^TEST[0-9]{4}$", id)

				// Check timestamp format
				timestamp, ok := record["timestamp"].(time.Time)
				assert.True(t, ok)
				assert.NotNil(t, timestamp)

				// Check name is from predefined values
				name, ok := record["name"].(string)
				assert.True(t, ok)
				assert.Contains(t, []string{"Test User 1", "Test User 2"}, name)

				// Check age range
				age, ok := record["age"].(int)
				assert.True(t, ok)
				assert.GreaterOrEqual(t, age, 18)
				assert.LessOrEqual(t, age, 65)
			}
		})
	}
}

func TestGenerateColumnValue(t *testing.T) {
	tests := []struct {
		name     string
		column   Column
		wantType interface{}
		validate func(t *testing.T, value interface{})
	}{
		{
			name: "Generate UUID",
			column: Column{
				Name: "id",
				Type: "uuid",
			},
			wantType: "",
			validate: func(t *testing.T, value interface{}) {
				assert.Regexp(t, "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", value)
			},
		},
		{
			name: "Generate Int with Range",
			column: Column{
				Name: "age",
				Type: "int",
				Range: Range{
					Min: 18,
					Max: 65,
				},
			},
			wantType: 0,
			validate: func(t *testing.T, value interface{}) {
				v := value.(int)
				assert.GreaterOrEqual(t, v, 18)
				assert.LessOrEqual(t, v, 65)
			},
		},
		{
			name: "Generate Pattern",
			column: Column{
				Name:    "code",
				Pattern: "TEST####",
			},
			wantType: "",
			validate: func(t *testing.T, value interface{}) {
				assert.Regexp(t, "^TEST[0-9]{4}$", value)
			},
		},
		{
			name: "Generate From Values",
			column: Column{
				Name:  "status",
				Value: []string{"active", "inactive"},
			},
			wantType: "",
			validate: func(t *testing.T, value interface{}) {
				assert.Contains(t, []string{"active", "inactive"}, value)
			},
		},
		{
			name: "Generate Timestamp with Format",
			column: Column{
				Name:   "created_at",
				Type:   "timestamp",
				Format: "2006-01-02 15:04:05",
				Range: Range{
					Min: "2023-01-01 00:00:00",
					Max: "2023-12-31 23:59:59",
				},
			},
			wantType: time.Time{},
			validate: func(t *testing.T, value interface{}) {
				v := value.(time.Time)
				minTime, _ := time.Parse("2006-01-02 15:04:05", "2023-01-01 00:00:00")
				maxTime, _ := time.Parse("2006-01-02 15:04:05", "2023-12-31 23:59:59")
				assert.True(t, v.After(minTime) || v.Equal(minTime))
				assert.True(t, v.Before(maxTime) || v.Equal(maxTime))
			},
		},
		{
			name: "Generate JSON",
			column: Column{
				Name: "metadata",
				Type: "json",
				JSONConfig: JSONConfig{
					{
						Name: "name",
						Type: "string",
					},
					{
						Name: "age",
						Type: "int",
						Range: Range{
							Min: 0,
							Max: 100,
						},
					},
				},
			},
			wantType: map[string]interface{}{},
			validate: func(t *testing.T, value interface{}) {
				jsonObj, ok := value.(map[string]interface{})
				assert.True(t, ok)
				assert.Contains(t, jsonObj, "name")
				assert.Contains(t, jsonObj, "age")

				name, ok := jsonObj["name"].(string)
				assert.True(t, ok)
				assert.NotEmpty(t, name)

				age, ok := jsonObj["age"].(int)
				assert.True(t, ok)
				assert.GreaterOrEqual(t, age, 0)
				assert.LessOrEqual(t, age, 100)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existingValues := make(map[interface{}]bool)

			value := generateColumnValue(tt.column)
			if tt.wantType != nil {
				assert.IsType(t, tt.wantType, value, existingValues)
			}
			tt.validate(t, value)
		})
	}
}

func TestSortTablesByDependency(t *testing.T) {
	tables := []Table{
		{
			Name:      "table3",
			Priority:  1,
			DependsOn: "table1",
		},
		{
			Name:     "table1",
			Priority: 3,
		},
		{
			Name:      "table2",
			Priority:  2,
			DependsOn: "table1",
		},
	}

	sorted := sortTablesByDependency(tables)

	// Verify the order
	assert.Equal(t, "table1", sorted[0].Name)
	assert.True(t, sorted[1].Priority >= sorted[2].Priority)
}

func TestReplaceWithNumbers(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		expected string
	}{
		{
			name:     "Empty pattern",
			pattern:  "",
			expected: "",
		},
		{
			name:     "No hashtags",
			pattern:  "ABC",
			expected: "ABC",
		},
		{
			name:     "Single hashtag",
			pattern:  "ABC#",
			expected: "ABC[0-9]",
		},
		{
			name:     "Multiple hashtags",
			pattern:  "ABC###",
			expected: "ABC[0-9]{3}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceWithNumbers(tt.pattern)
			if tt.expected == "" {
				assert.Equal(t, "", result)
			} else {
				assert.Regexp(t, "^"+tt.expected+"$", result)
			}
		})
	}
}

func TestGenerateDataWithRelations(t *testing.T) {
	manifestContent := `
tables:
- name: table_a
  priority: 1
  columns:
  - name: id
    pattern: "TA####"
    parent: true
    validation:
      unique: true
  - name: name
    type: string
    value: ["John Doe", "Jane Doe"]
- name: table_b
  priority: 2
  depends_on: table_a
  columns:
  - name: id
    foreign: "table_a.id"
  - name: age
    type: int
    range:
      min: 20
      max: 50
`
	tmpfile, err := os.CreateTemp("", "test_manifest*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(manifestContent)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	mockSink := &MockDataSink{
		Records: make([]map[string]interface{}, 0),
	}

	recordCount := 5
	GenerateData(mockSink, recordCount, tmpfile.Name())

	// Verify table_a records
	tableARecords := make([]map[string]interface{}, 0)
	tableBRecords := make([]map[string]interface{}, 0)

	for _, record := range mockSink.Records {
		if record["name"] != nil {
			tableARecords = append(tableARecords, record)
		} else {
			tableBRecords = append(tableBRecords, record)
		}
	}

	// Check table_a records
	assert.Equal(t, recordCount, len(tableARecords))
	for _, record := range tableARecords {
		// Check ID pattern
		id, ok := record["id"].(string)
		assert.True(t, ok)
		assert.Regexp(t, "^TA[0-9]{4}$", id)

		// Check name is from predefined values
		name, ok := record["name"].(string)
		assert.True(t, ok)
		assert.Contains(t, []string{"John Doe", "Jane Doe"}, name)
	}

	// Check table_b records
	assert.Equal(t, recordCount, len(tableBRecords))
	for _, record := range tableBRecords {
		// Check foreign key exists in table_a
		id, ok := record["id"].(string)
		assert.True(t, ok)
		found := false
		for _, tableARecord := range tableARecords {
			if tableARecord["id"] == id {
				found = true
				break
			}
		}
		assert.True(t, found, "Foreign key not found in table_a")

		// Check age range
		age, ok := record["age"].(int)
		assert.True(t, ok)
		assert.GreaterOrEqual(t, age, 20)
		assert.LessOrEqual(t, age, 50)
	}
}

func TestGenerateJSON(t *testing.T) {
	tests := []struct {
		name   string
		config JSONConfig
		verify func(t *testing.T, result interface{})
	}{
		{
			name:   "Default JSON Generation",
			config: JSONConfig{},
			verify: func(t *testing.T, result interface{}) {
				jsonObj, ok := result.(map[string]interface{})
				assert.True(t, ok)
				assert.GreaterOrEqual(t, len(jsonObj), 1)
				assert.LessOrEqual(t, len(jsonObj), 5)
			},
		},
		{
			name: "Predefined Fields",
			config: JSONConfig{
				{
					Name: "name",
					Type: "string",
				},
				{
					Name: "age",
					Type: "int",
					Range: Range{
						Min: 18,
						Max: 65,
					},
				},
				{
					Name: "email",
					Type: "email",
				},
			},
			verify: func(t *testing.T, result interface{}) {
				jsonObj, ok := result.(map[string]interface{})
				assert.True(t, ok)

				// Check name field
				name, ok := jsonObj["name"].(string)
				assert.True(t, ok)
				assert.NotEmpty(t, name)

				// Check age field
				age, ok := jsonObj["age"].(int)
				assert.True(t, ok)
				assert.GreaterOrEqual(t, age, 18)
				assert.LessOrEqual(t, age, 65)

				// Check email field
				email, ok := jsonObj["email"].(string)
				assert.True(t, ok)
				assert.Contains(t, email, "@")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateJSON(tt.config)
			tt.verify(t, result)
		})
	}
}
