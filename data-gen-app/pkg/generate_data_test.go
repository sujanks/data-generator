package pkg

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/sujanks/data-gen-app/pkg/types"
)

// MockDataSink is a mock implementation of the DataSink interface
type MockDataSink struct {
	Records []map[string]interface{}
}

func (m *MockDataSink) InsertRecord(tableName string, data map[string]interface{}) error {
	m.Records = append(m.Records, data)
	return nil
}

// Initialize pattern handling for tests
func init() {
	// Since the pattern handling is in the pkg package and not in types package,
	// we need to tell the test to handle the patterns correctly
	types.RegisterStringPatternHandler(func(pattern string) string {
		return replaceWithNumbers(pattern)
	})
}

func TestCSVSink(t *testing.T) {
	os.Setenv("SINK", "csv")
	os.Setenv("PROFILE", "test")
	os.Setenv("RECORDS", "10")
	manifestPath := "../manifest/test.yaml"
	GenerateData(nil, 1, manifestPath)
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
		column   types.Column
		wantType interface{}
		validate func(t *testing.T, value interface{})
	}{
		{
			name: "Generate UUID",
			column: types.Column{
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
			column: types.Column{
				Name: "age",
				Type: "int",
				Range: types.Range{
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
			column: types.Column{
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
			column: types.Column{
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
			column: types.Column{
				Name:   "created_at",
				Type:   "timestamp",
				Format: "2006-01-02 15:04:05",
				Range: types.Range{
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
			column: types.Column{
				Name: "metadata",
				Type: "json",
				JSONConfig: types.JSONConfig{
					{
						Name: "name",
						Type: "string",
					},
				},
			},
			wantType: map[string]interface{}{},
			validate: func(t *testing.T, value interface{}) {
				v, ok := value.(map[string]interface{})
				assert.True(t, ok)
				assert.NotEmpty(t, v)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := generateColumnValue(tt.column)
			assert.IsType(t, tt.wantType, value)
			tt.validate(t, value)
		})
	}
}

func TestSortTablesByDependency(t *testing.T) {
	tables := []types.Table{
		{
			Name:      "table3",
			Priority:  1,
			DependsOn: "table1",
		},
		{
			Name:     "table1",
			Priority: 1,
		},
		{
			Name:      "table2",
			Priority:  2,
			DependsOn: "table1",
		},
	}

	sortedTables := sortTablesByDependency(tables)

	// table1 should come first since it's a dependency for others
	assert.Equal(t, "table1", sortedTables[0].Name)

	// table2 should come before table3 due to higher priority
	assert.Equal(t, "table2", sortedTables[1].Name)
	assert.Equal(t, "table3", sortedTables[2].Name)
}

func TestReplaceWithNumbers(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		validate func(t *testing.T, result string)
	}{
		{
			name:    "Empty pattern",
			pattern: "",
			validate: func(t *testing.T, result string) {
				assert.Equal(t, "", result)
			},
		},
		{
			name:    "No hashtags",
			pattern: "ABCDEF",
			validate: func(t *testing.T, result string) {
				assert.Equal(t, "ABCDEF", result)
			},
		},
		{
			name:    "Single hashtag",
			pattern: "ABC#",
			validate: func(t *testing.T, result string) {
				assert.Regexp(t, "^ABC[0-9]$", result)
			},
		},
		{
			name:    "Multiple hashtags",
			pattern: "TEST####",
			validate: func(t *testing.T, result string) {
				assert.Regexp(t, "^TEST[0-9]{4}$", result)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceWithNumbers(tt.pattern)
			tt.validate(t, result)
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
		config types.JSONConfig
		verify func(t *testing.T, result interface{})
	}{
		{
			name:   "Default JSON Generation",
			config: types.JSONConfig{},
			verify: func(t *testing.T, result interface{}) {
				jsonObj, ok := result.(map[string]interface{})
				assert.True(t, ok)
				assert.NotEmpty(t, jsonObj)
			},
		},
		{
			name: "Predefined Fields",
			config: types.JSONConfig{
				{
					Name: "name",
					Type: "string",
				},
				{
					Name: "age",
					Type: "int",
					Range: types.Range{
						Min: 18,
						Max: 65,
					},
				},
			},
			verify: func(t *testing.T, result interface{}) {
				jsonObj, ok := result.(map[string]interface{})
				assert.True(t, ok)
				assert.Contains(t, jsonObj, "name")
				assert.Contains(t, jsonObj, "age")

				// Verify types
				_, ok = jsonObj["name"].(string)
				assert.True(t, ok)

				age, ok := jsonObj["age"].(int)
				assert.True(t, ok)
				assert.GreaterOrEqual(t, age, 18)
				assert.LessOrEqual(t, age, 65)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator := &types.JSONGenerator{Config: tt.config}
			result := generator.Generate()
			tt.verify(t, result)
		})
	}
}

func TestParseValue(t *testing.T) {
	baseTime := time.Date(2025, 3, 7, 12, 0, 0, 0, time.UTC)
	fields := map[string]interface{}{
		"created_on": baseTime,
	}

	tests := []struct {
		name     string
		value    string
		fields   map[string]interface{}
		expected interface{}
	}{
		{
			name:     "Parse integer",
			value:    "123",
			fields:   fields,
			expected: 123,
		},
		{
			name:     "Parse float",
			value:    "123.45",
			fields:   fields,
			expected: 123.45,
		},
		{
			name:     "Parse boolean",
			value:    "true",
			fields:   fields,
			expected: true,
		},
		{
			name:     "Parse timestamp",
			value:    "2025-03-07 12:00:00",
			fields:   fields,
			expected: time.Date(2025, 3, 7, 12, 0, 0, 0, time.UTC),
		},
		{
			name:     "Time arithmetic - add 1 hour",
			value:    "created_on + 1h",
			fields:   fields,
			expected: baseTime.Add(time.Hour),
		},
		{
			name:     "Time arithmetic - add 30 minutes",
			value:    "created_on + 30m",
			fields:   fields,
			expected: baseTime.Add(30 * time.Minute),
		},
		{
			name:     "Time arithmetic - add 2 hours",
			value:    "created_on + 2h",
			fields:   fields,
			expected: baseTime.Add(2 * time.Hour),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseValue(tt.value, tt.fields)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTimeArithmeticRules(t *testing.T) {
	baseTime := time.Date(2025, 3, 7, 12, 0, 0, 0, time.UTC)
	fields := map[string]interface{}{
		"created_on": baseTime,
		"status":     "PENDING",
	}

	tests := []struct {
		name           string
		rules          []types.Rule
		initialFields  map[string]interface{}
		expectedFields map[string]interface{}
	}{
		{
			name: "Simple time addition",
			rules: []types.Rule{
				{
					When: "true",
					Then: map[string]string{
						"modified_on": "created_on + 1h",
					},
				},
			},
			initialFields: fields,
			expectedFields: map[string]interface{}{
				"created_on":  baseTime,
				"modified_on": baseTime.Add(time.Hour),
				"status":      "PENDING",
			},
		},
		{
			name: "Conditional time addition based on status",
			rules: []types.Rule{
				{
					When: "status == PENDING",
					Then: map[string]string{
						"modified_on": "created_on + 30m",
					},
					Otherwise: map[string]string{
						"modified_on": "created_on + 2h",
					},
				},
			},
			initialFields: fields,
			expectedFields: map[string]interface{}{
				"created_on":  baseTime,
				"modified_on": baseTime.Add(30 * time.Minute),
				"status":      "PENDING",
			},
		},
		{
			name: "Multiple rules with time arithmetic",
			rules: []types.Rule{
				{
					When: "status == PENDING",
					Then: map[string]string{
						"modified_on": "created_on + 30m",
						"status":      "IN_PROGRESS",
					},
				},
				{
					When: "status == IN_PROGRESS",
					Then: map[string]string{
						"completed_on": "modified_on + 1h",
					},
				},
			},
			initialFields: fields,
			expectedFields: map[string]interface{}{
				"created_on":   baseTime,
				"modified_on":  baseTime.Add(30 * time.Minute),
				"completed_on": baseTime.Add(90 * time.Minute),
				"status":       "IN_PROGRESS",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy of initial fields to avoid modifying the original
			testFields := make(map[string]interface{})
			for k, v := range tt.initialFields {
				testFields[k] = v
			}

			// Apply rules
			applyRules(tt.rules, testFields)

			// Check results
			for key, expectedValue := range tt.expectedFields {
				assert.Equal(t, expectedValue, testFields[key], "Field %s has unexpected value", key)
			}
		})
	}
}

func TestGenerateDataWithTimeRules(t *testing.T) {
	// Create a temporary manifest file for testing
	manifestContent := `
tables:
- name: test_table
  priority: 1
  columns:
  - name: created_on
    type: timestamp
    format: "2006-01-02 15:04:05"
    range:
      min: "2025-03-07 12:00:00"
      max: "2025-03-07 12:00:00"
  - name: modified_on
    type: timestamp
    format: "2006-01-02 15:04:05"
    rules:
    - when: "true"
      then:
        modified_on: "created_on + 1h"
  - name: status
    type: string
    value: ["PENDING", "IN_PROGRESS"]
  - name: completed_on
    type: timestamp
    format: "2006-01-02 15:04:05"
    rules:
    - when: "status == IN_PROGRESS"
      then:
        completed_on: "modified_on + 2h"
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

	GenerateData(mockSink, 5, tmpfile.Name())

	// Verify the generated records
	assert.Equal(t, 5, len(mockSink.Records))

	baseTime := time.Date(2025, 3, 7, 12, 0, 0, 0, time.UTC)
	for _, record := range mockSink.Records {
		// Verify created_on
		createdOn, ok := record["created_on"].(time.Time)
		assert.True(t, ok)
		assert.Equal(t, baseTime, createdOn)

		// Verify modified_on is created_on + 1h
		modifiedOn, ok := record["modified_on"].(time.Time)
		assert.True(t, ok)
		assert.Equal(t, baseTime.Add(time.Hour), modifiedOn)

		// If status is IN_PROGRESS, verify completed_on is modified_on + 2h
		status := record["status"].(string)
		if status == "IN_PROGRESS" {
			completedOn, ok := record["completed_on"].(time.Time)
			assert.True(t, ok)
			assert.Equal(t, modifiedOn.Add(2*time.Hour), completedOn)
		}
	}
}

func TestExprEvaluation(t *testing.T) {
	fields := map[string]interface{}{
		"status":      "PENDING",
		"age":         30,
		"salary":      75000.0,
		"created_on":  time.Date(2025, 3, 7, 12, 0, 0, 0, time.UTC),
		"modified_on": time.Date(2025, 3, 7, 13, 0, 0, 0, time.UTC),
		"is_active":   true,
		"name":        "John Doe",
	}

	tests := []struct {
		name       string
		expression string
		fields     map[string]interface{}
		want       bool
	}{
		{
			name:       "Simple field comparison",
			expression: `fields.status == "PENDING"`,
			fields:     fields,
			want:       true,
		},
		{
			name:       "Numeric comparison",
			expression: "fields.age > 25 && fields.salary < 100000",
			fields:     fields,
			want:       true,
		},
		{
			name:       "Time comparison",
			expression: "fields.modified_on > fields.created_on",
			fields:     fields,
			want:       true,
		},
		{
			name:       "Complex condition with multiple fields",
			expression: `fields.age > 25 && fields.status == "PENDING" && fields.is_active`,
			fields:     fields,
			want:       true,
		},
		{
			name:       "Salary-based priority check",
			expression: "fields.salary > 50000 || (fields.salary > 25000 && fields.age > 25)",
			fields:     fields,
			want:       true,
		},
		{
			name:       "Time arithmetic comparison",
			expression: "fields.modified_on == addDuration(fields.created_on, '1h')",
			fields:     fields,
			want:       true,
		},
		{
			name:       "String prefix and suffix check",
			expression: `hasPrefix(fields.name, "John") && !hasSuffix(fields.name, "Smith")`,
			fields:     fields,
			want:       true,
		},
		{
			name:       "String trim and case conversion",
			expression: `trim(lower(fields.name)) == "john doe"`,
			fields:     fields,
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluateExpression(tt.expression, tt.fields)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, result, "Expression evaluation failed for: %s", tt.name)
		})
	}
}

func TestStringManipulationRules(t *testing.T) {
	fields := map[string]interface{}{
		"name": "John Doe",
	}

	tests := []struct {
		name       string
		expression string
		fields     map[string]interface{}
		want       bool
	}{
		{
			name:       "Simple string check",
			expression: `fields.name == "John Doe"`,
			fields:     fields,
			want:       true,
		},
		{
			name:       "Case-insensitive comparison",
			expression: `lower(fields.name) == "john doe"`,
			fields:     fields,
			want:       true,
		},
		{
			name:       "Negative string comparison",
			expression: `fields.name != "Smith"`,
			fields:     fields,
			want:       true,
		},
		{
			name:       "Multiple string operations",
			expression: `lower(fields.name) == "john doe" && fields.name != "Smith"`,
			fields:     fields,
			want:       true,
		},
		{
			name:       "String length check",
			expression: `len(trim(fields.name)) == 8`,
			fields:     fields,
			want:       true,
		},
		{
			name:       "Case conversion check",
			expression: `upper(fields.name) == "JOHN DOE"`,
			fields:     fields,
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluateExpression(tt.expression, tt.fields)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, result, "Expression evaluation failed for: %s", tt.name)
		})
	}
}

func TestMapGenerator(t *testing.T) {
	tests := []struct {
		name     string
		config   types.MapConfig
		validate func(t *testing.T, value interface{})
	}{
		{
			name: "Map with predefined keys and values",
			config: types.MapConfig{
				MinEntries: 2,
				MaxEntries: 3,
				Keys:       []string{"key1", "key2", "key3"},
				Values:     []string{"value1", "value2", "value3"},
				KeyType:    "string",
				ValueType:  "string",
			},
			validate: func(t *testing.T, value interface{}) {
				m, ok := value.(map[string]interface{})
				assert.True(t, ok)
				assert.GreaterOrEqual(t, len(m), 2)
				assert.LessOrEqual(t, len(m), 3)

				// Check that keys are from the predefined list
				for k := range m {
					assert.Contains(t, []string{"key1", "key2", "key3"}, k)
				}

				// Check that values are from the predefined list
				for _, v := range m {
					assert.Contains(t, []string{"value1", "value2", "value3"}, v)
				}
			},
		},
		{
			name: "Map with generated keys and values",
			config: types.MapConfig{
				MinEntries: 1,
				MaxEntries: 2,
				KeyType:    "string",
				ValueType:  "int",
			},
			validate: func(t *testing.T, value interface{}) {
				m, ok := value.(map[string]interface{})
				assert.True(t, ok)
				assert.GreaterOrEqual(t, len(m), 1)
				assert.LessOrEqual(t, len(m), 2)

				// Check key and value types
				for k, v := range m {
					assert.IsType(t, "", k)
					assert.IsType(t, 0, v)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator := &types.MapGenerator{Config: tt.config}
			value := generator.Generate()
			tt.validate(t, value)
		})
	}
}

func TestSetGenerator(t *testing.T) {
	tests := []struct {
		name     string
		config   types.SetConfig
		validate func(t *testing.T, value interface{})
	}{
		{
			name: "Set with predefined values",
			config: types.SetConfig{
				MinElements: 1,
				MaxElements: 2,
				Values:      []string{"value1", "value2", "value3"},
				ElementType: "string",
			},
			validate: func(t *testing.T, value interface{}) {
				set, ok := value.([]interface{})
				assert.True(t, ok)
				assert.GreaterOrEqual(t, len(set), 1)
				assert.LessOrEqual(t, len(set), 2)

				// Check that values are from the predefined list
				for _, v := range set {
					assert.Contains(t, []string{"value1", "value2", "value3"}, v)
				}

				// Check for uniqueness
				seen := make(map[interface{}]bool)
				for _, v := range set {
					assert.False(t, seen[v], "Duplicate value found in set: %v", v)
					seen[v] = true
				}
			},
		},
		{
			name: "Set with generated values",
			config: types.SetConfig{
				MinElements: 2,
				MaxElements: 3,
				ElementType: "string",
			},
			validate: func(t *testing.T, value interface{}) {
				set, ok := value.([]interface{})
				assert.True(t, ok)
				assert.GreaterOrEqual(t, len(set), 2)
				assert.LessOrEqual(t, len(set), 3)

				// Check for uniqueness
				seen := make(map[interface{}]bool)
				for _, v := range set {
					assert.False(t, seen[v], "Duplicate value found in set: %v", v)
					seen[v] = true
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator := &types.SetGenerator{Config: tt.config}
			value := generator.Generate()
			tt.validate(t, value)
		})
	}
}

func TestListGenerator(t *testing.T) {
	tests := []struct {
		name     string
		config   types.ListConfig
		validate func(t *testing.T, value interface{})
	}{
		{
			name: "List with predefined values",
			config: types.ListConfig{
				MinElements: 2,
				MaxElements: 3,
				Values:      []string{"value1", "value2", "value3"},
				ElementType: "string",
			},
			validate: func(t *testing.T, value interface{}) {
				list, ok := value.([]interface{})
				assert.True(t, ok)
				assert.GreaterOrEqual(t, len(list), 2)
				assert.LessOrEqual(t, len(list), 3)

				// Check that values are from the predefined list
				for _, v := range list {
					assert.Contains(t, []string{"value1", "value2", "value3"}, v)
				}
			},
		},
		{
			name: "List with pattern",
			config: types.ListConfig{
				MinElements: 1,
				MaxElements: 2,
				Pattern:     "TEST##",
				ElementType: "string",
			},
			validate: func(t *testing.T, value interface{}) {
				list, ok := value.([]interface{})
				assert.True(t, ok)
				assert.GreaterOrEqual(t, len(list), 1)
				assert.LessOrEqual(t, len(list), 2)

				// Check pattern
				for _, v := range list {
					str, ok := v.(string)
					assert.True(t, ok)
					assert.Regexp(t, "^TEST[0-9]{2}$", str)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator := &types.ListGenerator{Config: tt.config}
			value := generator.Generate()
			tt.validate(t, value)
		})
	}
}

func TestUDTGenerator(t *testing.T) {
	tests := []struct {
		name     string
		config   types.UDTConfig
		validate func(t *testing.T, value interface{})
	}{
		{
			name: "UDT with multiple fields",
			config: types.UDTConfig{
				Name: "address",
				Fields: []types.Column{
					{
						Name: "street",
						Type: "string",
					},
					{
						Name: "city",
						Type: "string",
					},
					{
						Name:    "zip",
						Pattern: "#####",
					},
				},
			},
			validate: func(t *testing.T, value interface{}) {
				udt, ok := value.(map[string]interface{})
				assert.True(t, ok)
				assert.Contains(t, udt, "street")
				assert.Contains(t, udt, "city")
				assert.Contains(t, udt, "zip")

				// Check types
				_, ok = udt["street"].(string)
				assert.True(t, ok)
				_, ok = udt["city"].(string)
				assert.True(t, ok)

				// Check pattern for zip
				zip, ok := udt["zip"].(string)
				assert.True(t, ok)
				assert.Regexp(t, "^[0-9]{5}$", zip)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Register UDT generator
			types.RegisterGenerateUDT(func(g *types.UDTGenerator) interface{} {
				result := make(map[string]interface{})
				for _, field := range g.Config.Fields {
					result[field.Name] = generateColumnValue(field)
				}
				return result
			})

			generator := &types.UDTGenerator{Config: tt.config}
			value := generator.Generate()
			tt.validate(t, value)
		})
	}
}

func TestTupleGenerator(t *testing.T) {
	tests := []struct {
		name     string
		config   types.TupleConfig
		validate func(t *testing.T, value interface{})
	}{
		{
			name: "Tuple with mixed types",
			config: types.TupleConfig{
				Elements: []types.Column{
					{
						Name: "lat",
						Type: "decimal",
						Range: types.Range{
							Min: -90.0,
							Max: 90.0,
						},
					},
					{
						Name: "lon",
						Type: "decimal",
						Range: types.Range{
							Min: -180.0,
							Max: 180.0,
						},
					},
					{
						Name: "name",
						Type: "string",
					},
				},
			},
			validate: func(t *testing.T, value interface{}) {
				tuple, ok := value.([]interface{})
				assert.True(t, ok)
				assert.Equal(t, 3, len(tuple))

				// Check types and ranges
				lat, ok := tuple[0].(float64)
				assert.True(t, ok)
				assert.GreaterOrEqual(t, lat, -90.0)
				assert.LessOrEqual(t, lat, 90.0)

				lon, ok := tuple[1].(float64)
				assert.True(t, ok)
				assert.GreaterOrEqual(t, lon, -180.0)
				assert.LessOrEqual(t, lon, 180.0)

				_, ok = tuple[2].(string)
				assert.True(t, ok)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Register Tuple generator
			types.RegisterGenerateTuple(func(g *types.TupleGenerator) interface{} {
				result := make([]interface{}, len(g.Config.Elements))
				for i, element := range g.Config.Elements {
					result[i] = generateColumnValue(element)
				}
				return result
			})

			generator := &types.TupleGenerator{Config: tt.config}
			value := generator.Generate()
			tt.validate(t, value)
		})
	}
}
