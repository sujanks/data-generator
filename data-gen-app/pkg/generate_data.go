package pkg

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/expr-lang/expr"
	"github.com/sujanks/data-gen-app/pkg/sink"
	"gopkg.in/yaml.v3"
)

type Tables struct {
	Tables []Table `yaml:"tables"`
}

type Table struct {
	Name      string   `yaml:"name"`
	Priority  int      `yaml:"priority"`
	DependsOn string   `yaml:"depends_on,omitempty"`
	Columns   []Column `yaml:"columns"`
	Rules     []Rule   `yaml:"rules,omitempty"`
}

// Validation defines validation rules for a column
type Validation struct {
	Unique bool `yaml:"unique,omitempty"`
}

// Range defines min/max values for numeric and date fields
type Range struct {
	Min interface{} `yaml:"min,omitempty"`
	Max interface{} `yaml:"max,omitempty"`
}

// FieldConfig defines configuration for a specific JSON field
type FieldConfig struct {
	Name  string `yaml:"name"`
	Type  string `yaml:"type"`
	Range Range  `yaml:"range,omitempty"`
}

// JSONConfig is now just an array of field configurations
type JSONConfig []FieldConfig

// Rule defines a conditional rule with an expression and actions
type Rule struct {
	When      string            `yaml:"when"`      // Expression to evaluate
	Then      map[string]string `yaml:"then"`      // Field values to set when expression is true
	Otherwise map[string]string `yaml:"otherwise"` // Field values to set when expression is false
}

// Column defines a column in a table
type Column struct {
	Name       string     `yaml:"name"`
	Pattern    string     `yaml:"pattern,omitempty"`
	Value      []string   `yaml:"value,omitempty"`
	Type       string     `yaml:"type,omitempty"`
	Format     string     `yaml:"format,omitempty"`
	Mandatory  bool       `yaml:"mandatory"`
	Parent     bool       `yaml:"parent"`
	Foreign    string     `yaml:"foreign,omitempty"`
	Validation Validation `yaml:"validation,omitempty"`
	Range      Range      `yaml:"range,omitempty"`
	JSONConfig JSONConfig `yaml:"json_config,omitempty"`
	Rules      []Rule     `yaml:"rules,omitempty"` // Rules to apply on the column
}

const hashtag = '#'

func GenerateData(ds sink.DataSink, count int, profile string) {
	tables := readManifest(profile)
	sortedTables := sortTablesByDependency(tables.Tables)
	parentKeyValues := make(map[string][]string, 0)

	for _, table := range sortedTables {
		for i := 0; i < count; i++ {
			var tableData = make(map[string]interface{})

			// First pass: generate all basic values
			for _, col := range table.Columns {
				var colValue interface{}
				if !col.Mandatory {
					if col.Foreign != "" {
						colValue = gofakeit.RandomString(parentKeyValues[col.Foreign])
					} else if len(col.Value) > 0 {
						colValue = gofakeit.RandomString(col.Value)
					} else if col.Pattern != "" {
						colValue = replaceWithNumbers(col.Pattern)
					} else {
						colValue = generateColumnValue(col)
					}
				}
				tableData[col.Name] = colValue
			}

			// Second pass: apply rules
			for _, col := range table.Columns {
				if len(col.Rules) > 0 {
					applyRules(col.Rules, tableData)
				}
			}

			if table.Rules != nil {
				applyRules(table.Rules, tableData)
			}

			// Store parent values for foreign key references
			for _, col := range table.Columns {
				if col.Parent {
					keyName := fmt.Sprintf("%s.%s", table.Name, col.Name)
					parentKeyValues[keyName] = append(parentKeyValues[keyName], fmt.Sprint(tableData[col.Name]))
				}
			}

			ds.InsertRecord(table.Name, tableData)
		}
	}
	log.Printf("%d records inserted", count)
}

// generateColumnValue generates a value for a column based on its configuration
func generateColumnValue(col Column) interface{} {
	switch col.Type {
	case "float", "decimal":
		min, max := 0.0, 100.0
		if col.Range.Min != nil {
			if minVal, ok := col.Range.Min.(float64); ok {
				min = minVal
			}
		}
		if col.Range.Max != nil {
			if maxVal, ok := col.Range.Max.(float64); ok {
				max = maxVal
			}
		}
		return gofakeit.Float64Range(min, max)

	case "int":
		min, max := 0, 1000000
		if col.Range.Min != nil {
			if minVal, ok := col.Range.Min.(int); ok {
				min = minVal
			}
		}
		if col.Range.Max != nil {
			if maxVal, ok := col.Range.Max.(int); ok {
				max = maxVal
			}
		}
		return gofakeit.IntRange(min, max)

	case "sentence":
		return gofakeit.Sentence(5)

	case "bool":
		return gofakeit.Bool()

	case "date":
		format := "2006-01-02"
		if col.Format != "" {
			format = col.Format
		}
		if col.Range.Min != nil && col.Range.Max != nil {
			minTime, err1 := time.Parse(format, col.Range.Min.(string))
			maxTime, err2 := time.Parse(format, col.Range.Max.(string))
			if err1 == nil && err2 == nil {
				return gofakeit.DateRange(minTime, maxTime).Format(format)
			}
		}
		return time.Now().Format(format)

	case "timestamp":
		format := "2006-01-02 15:04:05"
		if col.Format != "" {
			format = col.Format
		}
		if col.Range.Min != nil && col.Range.Max != nil {
			minTime, err1 := time.Parse(format, col.Range.Min.(string))
			maxTime, err2 := time.Parse(format, col.Range.Max.(string))
			if err1 == nil && err2 == nil {
				return gofakeit.DateRange(minTime, maxTime)
			}
		}
		return time.Now()

	case "uuid":
		return gofakeit.UUID()

	case "string":
		if strings.Contains(col.Name, "name") {
			return gofakeit.Name()
		}
		return gofakeit.Word()

	case "json":
		return generateJSON(col.JSONConfig)

	default:
		if strings.Contains(col.Name, "name") {
			return gofakeit.Name()
		}
		return gofakeit.Word()
	}
}

// generateJSON generates a random JSON object based on configuration
func generateJSON(config JSONConfig) interface{} {
	// Create JSON object
	jsonObj := make(map[string]interface{})

	// Use configured fields
	if len(config) > 0 {
		for _, field := range config {
			jsonObj[field.Name] = generateValueByType(field.Type, field.Range)
		}
	} else {
		// Generate a random number of fields if no configuration provided
		numKeys := gofakeit.IntRange(1, 5)
		for i := 0; i < numKeys; i++ {
			field := gofakeit.Word()
			valueType := getRandomValueType()
			jsonObj[field] = generateValueByType(valueType, Range{})
		}
	}

	return jsonObj
}

// getRandomValueType returns a random value type for JSON fields
func getRandomValueType() string {
	types := []string{"string", "int", "float", "bool", "date", "email", "url"}
	return types[gofakeit.IntRange(0, len(types)-1)]
}

// generateValueByType generates a random value of specified type
func generateValueByType(valueType string, rangeConfig Range) interface{} {
	switch valueType {
	case "string":
		return gofakeit.Word()
	case "int":
		min, max := 0, 1000
		if rangeConfig.Min != nil {
			if minVal, ok := rangeConfig.Min.(int); ok {
				min = minVal
			}
		}
		if rangeConfig.Max != nil {
			if maxVal, ok := rangeConfig.Max.(int); ok {
				max = maxVal
			}
		}
		return gofakeit.IntRange(min, max)
	case "float":
		min, max := 0.0, 1000.0
		if rangeConfig.Min != nil {
			if minVal, ok := rangeConfig.Min.(float64); ok {
				min = minVal
			}
		}
		if rangeConfig.Max != nil {
			if maxVal, ok := rangeConfig.Max.(float64); ok {
				max = maxVal
			}
		}
		return gofakeit.Float64Range(min, max)
	case "bool":
		return gofakeit.Bool()
	case "date":
		return time.Now().Format("2006-01-02")
	case "email":
		return gofakeit.Email()
	case "url":
		return gofakeit.URL()
	default:
		return gofakeit.Word()
	}
}

func readManifest(filename string) Tables {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("error reading file %v ", err.Error())
	}

	var tables Tables
	err = yaml.NewDecoder(file).Decode(&tables)
	if err != nil {
		log.Fatalf("error reading file %v ", err.Error())
	}
	return tables
}

func replaceWithNumbers(str string) string {
	if str == "" {
		return str
	}
	bytestr := []byte(str)
	for i := 0; i < len(bytestr); i++ {
		if bytestr[i] == hashtag {
			bytestr[i] = byte(randDigit())
		}
	}
	if bytestr[0] == '0' {
		bytestr[0] = byte(gofakeit.IntN(8)+1) + '0'
	}
	return string(bytestr)
}

func randDigit() rune {
	return rune(byte(gofakeit.IntN(10)) + '0')
}

// sortTablesByDependency sorts tables based on their dependencies and priorities
func sortTablesByDependency(tables []Table) []Table {
	// Create dependency graph
	graph := make(map[string][]string)
	for _, table := range tables {
		if table.DependsOn != "" {
			graph[table.DependsOn] = append(graph[table.DependsOn], table.Name)
		}
	}

	// Create priority map
	priorities := make(map[string]int)
	for _, table := range tables {
		priorities[table.Name] = table.Priority
	}

	// Sort based on both dependencies and priorities
	sorted := make([]Table, len(tables))
	copy(sorted, tables)

	sort.SliceStable(sorted, func(i, j int) bool {
		// First check dependencies
		if sorted[i].DependsOn == sorted[j].Name {
			return false
		}
		if sorted[j].DependsOn == sorted[i].Name {
			return true
		}

		// Then check priorities
		return sorted[i].Priority > sorted[j].Priority
	})

	return sorted
}

// evaluateExpression evaluates an expression against field values using expr library
func evaluateExpression(expression string, fields map[string]interface{}) bool {
	// Add helper functions to the environment
	env := map[string]interface{}{
		"fields": fields,
		// Time helper functions
		"now":         time.Now,
		"parseTime":   func(layout, value string) time.Time { t, _ := time.Parse(layout, value); return t },
		"addDuration": func(t time.Time, d string) time.Time { dur, _ := time.ParseDuration(d); return t.Add(dur) },
		"format":      func(t time.Time, layout string) string { return t.Format(layout) },
		// Math helper functions
		"min": func(a, b float64) float64 {
			if a < b {
				return a
			}
			return b
		},
		"max": func(a, b float64) float64 {
			if a > b {
				return a
			}
			return b
		},
		// String helper functions
		"contains":  strings.Contains,
		"hasPrefix": strings.HasPrefix,
		"hasSuffix": strings.HasPrefix,
		"lower":     strings.ToLower,
		"upper":     strings.ToUpper,
		"trim":      strings.TrimSpace,
	}

	// Create options for the expression
	options := []expr.Option{
		expr.Env(env),
		expr.AllowUndefinedVariables(),
	}

	// Compile the expression
	program, err := expr.Compile(expression, options...)
	if err != nil {
		log.Printf("Error compiling expression: %v", err)
		return false
	}

	// Run the expression
	output, err := expr.Run(program, env)
	if err != nil {
		log.Printf("Error running expression: %v", err)
		return false
	}

	// Convert output to boolean
	result, ok := output.(bool)
	if !ok {
		log.Printf("Expression did not evaluate to boolean: %v", output)
		return false
	}

	return result
}

// parseValue converts string value to appropriate type using expr
func parseValue(value string, fields map[string]interface{}) interface{} {
	// If the value contains an expression (indicated by ${...})
	if strings.Contains(value, "${") && strings.Contains(value, "}") {
		// Extract the expression
		expression := strings.TrimPrefix(strings.TrimSuffix(value, "}"), "${")

		// Add helper functions to the environment
		env := map[string]interface{}{
			"fields": fields,
			// Time helper functions
			"now":         time.Now,
			"parseTime":   func(layout, value string) time.Time { t, _ := time.Parse(layout, value); return t },
			"addDuration": func(t time.Time, d string) time.Time { dur, _ := time.ParseDuration(d); return t.Add(dur) },
			"format":      func(t time.Time, layout string) string { return t.Format(layout) },
			// Math helper functions
			"min": func(a, b float64) float64 {
				if a < b {
					return a
				}
				return b
			},
			"max": func(a, b float64) float64 {
				if a > b {
					return a
				}
				return b
			},
			// String helper functions
			"contains":  strings.Contains,
			"hasPrefix": strings.HasPrefix,
			"hasSuffix": strings.HasPrefix,
			"lower":     strings.ToLower,
			"upper":     strings.ToUpper,
			"trim":      strings.TrimSpace,
		}

		// Create options for the expression
		options := []expr.Option{
			expr.Env(env),
			expr.AllowUndefinedVariables(),
		}

		// Compile and run the expression
		program, err := expr.Compile(expression, options...)
		if err != nil {
			log.Printf("Error compiling value expression: %v", err)
			return value
		}

		output, err := expr.Run(program, env)
		if err != nil {
			log.Printf("Error running value expression: %v", err)
			return value
		}

		return output
	}

	// Handle simple time arithmetic expressions like "fieldname + 1h"
	if strings.Contains(value, " + ") {
		parts := strings.Split(value, " + ")
		if len(parts) == 2 {
			baseField := strings.TrimSpace(parts[0])
			if baseValue, exists := fields[baseField]; exists {
				if baseTime, ok := baseValue.(time.Time); ok {
					duration := strings.TrimSpace(parts[1])
					if parsedDuration, err := time.ParseDuration(duration); err == nil {
						return baseTime.Add(parsedDuration)
					}
				}
			}
		}
	}

	// Try to parse as timestamp
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, value); err == nil {
			return t
		}
	}

	// Try to parse as int
	if i, err := strconv.Atoi(value); err == nil {
		return i
	}
	// Try to parse as float
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f
	}
	// Try to parse as bool
	if b, err := strconv.ParseBool(value); err == nil {
		return b
	}
	// Return as string if no other type matches
	return value
}

// applyRules applies the rules to the generated data
func applyRules(rules []Rule, fields map[string]interface{}) {
	for _, rule := range rules {
		if evaluateExpression(rule.When, fields) {
			// Apply 'then' values
			for field, value := range rule.Then {
				fields[field] = parseValue(value, fields)
			}
		} else if rule.Otherwise != nil {
			// Apply 'otherwise' values
			for field, value := range rule.Otherwise {
				fields[field] = parseValue(value, fields)
			}
		}
	}
}
