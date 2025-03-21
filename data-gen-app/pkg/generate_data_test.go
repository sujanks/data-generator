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
		rules          []Rule
		initialFields  map[string]interface{}
		expectedFields map[string]interface{}
	}{
		{
			name: "Simple time addition",
			rules: []Rule{
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
			rules: []Rule{
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
			rules: []Rule{
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
	baseTime := time.Date(2025, 3, 7, 12, 0, 0, 0, time.UTC)
	fields := map[string]interface{}{
		"created_on":  baseTime,
		"modified_on": baseTime.Add(time.Hour),
		"status":      "PENDING",
		"age":         30,
		"salary":      50000.0,
		"name":        "John Doe",
		"is_active":   true,
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
			name:       "String functions",
			expression: `contains(lower(fields.name), "john")`,
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
			name:       "Complex condition",
			expression: `fields.age > 25 && fields.status == "PENDING" && fields.is_active`,
			fields:     fields,
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evaluateExpression(tt.expression, tt.fields)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExprValueParsing(t *testing.T) {
	baseTime := time.Date(2025, 3, 7, 12, 0, 0, 0, time.UTC)
	fields := map[string]interface{}{
		"created_on":  baseTime,
		"modified_on": baseTime.Add(time.Hour),
		"status":      "PENDING",
		"age":         30,
		"salary":      50000.0,
	}

	tests := []struct {
		name   string
		value  string
		fields map[string]interface{}
		want   interface{}
	}{
		{
			name:   "Expression - simple math",
			value:  "${fields.age + 10}",
			fields: fields,
			want:   40.0, // expr evaluates numbers as float64
		},
		{
			name:   "Expression - time arithmetic",
			value:  "${addDuration(fields.created_on, '2h')}",
			fields: fields,
			want:   baseTime.Add(2 * time.Hour),
		},
		{
			name:   "Expression - string manipulation",
			value:  "${upper(trim('  test  '))}",
			fields: fields,
			want:   "TEST",
		},
		{
			name:   "Expression - conditional",
			value:  "${fields.age > 25 ? 'Adult' : 'Young'}",
			fields: fields,
			want:   "Adult",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseValue(tt.value, tt.fields)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExprRules(t *testing.T) {
	baseTime := time.Date(2025, 3, 7, 12, 0, 0, 0, time.UTC)
	fields := map[string]interface{}{
		"created_on": baseTime,
		"status":     "PENDING",
		"age":        30,
	}

	tests := []struct {
		name          string
		rules         []Rule
		initialFields map[string]interface{}
		wantFields    map[string]interface{}
	}{
		{
			name: "Complex rule with time arithmetic",
			rules: []Rule{
				{
					When: `fields.status == "PENDING" && fields.age > 25`,
					Then: map[string]string{
						"modified_on": "${addDuration(fields.created_on, '1h')}",
						"priority":    "${fields.age > 40 ? 'High' : 'Medium'}",
					},
				},
			},
			initialFields: fields,
			wantFields: map[string]interface{}{
				"created_on":  baseTime,
				"status":      "PENDING",
				"age":         30,
				"modified_on": baseTime.Add(time.Hour),
				"priority":    "Medium",
			},
		},
		{
			name: "Multiple rules with expressions",
			rules: []Rule{
				{
					When: "fields.age > 25",
					Then: map[string]string{
						"access_level": "${fields.age > 50 ? 'Senior' : 'Regular'}",
					},
				},
				{
					When: `fields.status == "PENDING"`,
					Then: map[string]string{
						"next_review": "${addDuration(fields.created_on, '24h')}",
						"status_code": "${upper(fields.status)}",
					},
				},
			},
			initialFields: fields,
			wantFields: map[string]interface{}{
				"created_on":   baseTime,
				"status":       "PENDING",
				"age":          30,
				"access_level": "Regular",
				"next_review":  baseTime.Add(24 * time.Hour),
				"status_code":  "PENDING",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy of initial fields
			testFields := make(map[string]interface{})
			for k, v := range tt.initialFields {
				testFields[k] = v
			}

			// Apply rules
			applyRules(tt.rules, testFields)

			// Check results
			assert.Equal(t, tt.wantFields, testFields)
		})
	}
}

func TestGenerateDataWithExprRules(t *testing.T) {
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
  - name: status
    type: string
    value: ["PENDING", "APPROVED"]
  - name: age
    type: int
    range:
      min: 20
      max: 60
  rules:
  - when: fields.age > 50
    then:
      status: "SENIOR"
      modified_on: "${addDuration(fields.created_on, '2h')}"
  - when: fields.status == "PENDING"
    then:
      review_date: "${addDuration(fields.created_on, '24h')}"
      priority: "${fields.age > 40 ? 'High' : 'Medium'}"
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

		// Verify age and related rules
		age, ok := record["age"].(int)
		assert.True(t, ok)
		assert.GreaterOrEqual(t, age, 20)
		assert.LessOrEqual(t, age, 60)

		// Verify status and related rules
		status := record["status"].(string)
		if age > 50 {
			assert.Equal(t, "SENIOR", status)
			modifiedOn, ok := record["modified_on"].(time.Time)
			assert.True(t, ok)
			assert.Equal(t, baseTime.Add(2*time.Hour), modifiedOn)
		} else if status == "PENDING" {
			reviewDate, ok := record["review_date"].(time.Time)
			assert.True(t, ok)
			assert.Equal(t, baseTime.Add(24*time.Hour), reviewDate)

			priority := record["priority"].(string)
			if age > 40 {
				assert.Equal(t, "High", priority)
			} else {
				assert.Equal(t, "Medium", priority)
			}
		}
	}
}
