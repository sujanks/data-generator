package types

import (
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
)

// Schema represents the data generation schema
type Schema struct {
	Tables []Table `yaml:"tables"`
}

// Table represents a table in the schema
type Table struct {
	Name      string   `yaml:"name"`
	Priority  int      `yaml:"priority"`
	DependsOn string   `yaml:"depends_on,omitempty"`
	Columns   []Column `yaml:"columns"`
	Rules     []Rule   `yaml:"rules,omitempty"`
}

// Column represents a column in a table
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
	// Cassandra-specific fields
	KeyType     string      `yaml:"key_type,omitempty"`
	ValueType   string      `yaml:"value_type,omitempty"`
	ElementType string      `yaml:"element_type,omitempty"`
	MapConfig   MapConfig   `yaml:"map_config,omitempty"`
	SetConfig   SetConfig   `yaml:"set_config,omitempty"`
	UDTConfig   UDTConfig   `yaml:"udt_config,omitempty"`
	ListConfig  ListConfig  `yaml:"list_config,omitempty"`
	TupleConfig TupleConfig `yaml:"tuple_config,omitempty"`
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

// Rule defines a conditional rule with an expression and actions
type Rule struct {
	When      string            `yaml:"when"`      // Expression to evaluate
	Then      map[string]string `yaml:"then"`      // Field values to set when expression is true
	Otherwise map[string]string `yaml:"otherwise"` // Field values to set when expression is false
}

// FieldConfig defines configuration for a specific JSON field
type FieldConfig struct {
	Name  string `yaml:"name"`
	Type  string `yaml:"type"`
	Range Range  `yaml:"range,omitempty"`
}

// JSONConfig is an array of field configurations
type JSONConfig []FieldConfig

// Tables represents the root object in the YAML file
type Tables struct {
	Tables []Table `yaml:"tables"`
}

// Cassandra-specific configurations

// MapConfig defines configuration for map type
type MapConfig struct {
	MinEntries int      `yaml:"min_entries"`
	MaxEntries int      `yaml:"max_entries"`
	Keys       []string `yaml:"keys,omitempty"`
	Values     []string `yaml:"values,omitempty"`
	KeyType    string   `yaml:"key_type"`
	ValueType  string   `yaml:"value_type"`
}

// SetConfig defines configuration for set type
type SetConfig struct {
	MinElements int      `yaml:"min_elements"`
	MaxElements int      `yaml:"max_elements"`
	Values      []string `yaml:"values,omitempty"`
	ElementType string   `yaml:"element_type"`
	Pattern     string   `yaml:"pattern,omitempty"`
}

// UDTConfig defines configuration for user-defined type
type UDTConfig struct {
	Name   string   `yaml:"name"`
	Fields []Column `yaml:"fields"`
}

// ListConfig defines configuration for list type
type ListConfig struct {
	MinElements int      `yaml:"min_elements"`
	MaxElements int      `yaml:"max_elements"`
	Pattern     string   `yaml:"pattern,omitempty"`
	ElementType string   `yaml:"element_type"`
	Values      []string `yaml:"values,omitempty"`
}

// TupleConfig defines configuration for tuple type
type TupleConfig struct {
	Elements []Column `yaml:"elements"`
}

// ValueGenerator defines the interface for generating values
type ValueGenerator interface {
	Generate() interface{}
}

// BaseGenerator provides common functionality for all generators
type BaseGenerator struct {
	Config interface{}
}

// MapGenerator generates map values
type MapGenerator struct {
	BaseGenerator
	Config MapConfig
}

// Generate generates a random map
func (g *MapGenerator) Generate() interface{} {
	numEntries := gofakeit.IntRange(g.Config.MinEntries, g.Config.MaxEntries)
	result := make(map[string]interface{})

	// First, add all predefined keys if available
	if len(g.Config.Keys) > 0 {
		for _, key := range g.Config.Keys {
			if len(result) >= numEntries {
				break
			}
			value := g.generateValue()
			result[key] = value
		}
	}

	// Then add random entries until we reach the desired number
	for len(result) < numEntries {
		key := g.generateKey()
		value := g.generateValue()
		result[key.(string)] = value
	}

	return result
}

func (g *MapGenerator) generateKey() interface{} {
	if len(g.Config.Keys) > 0 {
		return gofakeit.RandomString(g.Config.Keys)
	}
	return generateRandomValue(g.Config.KeyType)
}

func (g *MapGenerator) generateValue() interface{} {
	if len(g.Config.Values) > 0 {
		return gofakeit.RandomString(g.Config.Values)
	}
	return generateRandomValue(g.Config.ValueType)
}

// SetGenerator generates set values
type SetGenerator struct {
	BaseGenerator
	Config SetConfig
}

// Generate generates a random set
func (g *SetGenerator) Generate() interface{} {
	numElements := gofakeit.IntRange(g.Config.MinElements, g.Config.MaxElements)
	result := make([]interface{}, 0, numElements)
	seen := make(map[interface{}]bool)

	for i := 0; i < numElements*2 && len(result) < numElements; i++ { // Try twice as many times to ensure we get enough unique values
		value := g.generateElement()
		valueStr := value.(string)
		if !seen[valueStr] {
			seen[valueStr] = true
			result = append(result, value)
		}
	}

	return result
}

func (g *SetGenerator) generateElement() interface{} {
	if len(g.Config.Values) > 0 {
		return gofakeit.RandomString(g.Config.Values)
	}
	return generateRandomValue(g.Config.ElementType)
}

// ListGenerator generates list values
type ListGenerator struct {
	BaseGenerator
	Config ListConfig
}

// Generate generates a random list
func (g *ListGenerator) Generate() interface{} {
	numElements := gofakeit.IntRange(g.Config.MinElements, g.Config.MaxElements)
	result := make([]interface{}, 0, numElements)

	for i := 0; i < numElements; i++ {
		value := g.generateElement()
		result = append(result, value)
	}

	return result
}

func (g *ListGenerator) generateElement() interface{} {
	if len(g.Config.Values) > 0 {
		return gofakeit.RandomString(g.Config.Values)
	}
	if g.Config.Pattern != "" {
		// Use the registered pattern handler if available
		if stringPatternHandler != nil {
			return stringPatternHandler(g.Config.Pattern)
		}
		// Otherwise, just return the pattern
		return g.Config.Pattern
	}
	return generateRandomValue(g.Config.ElementType)
}

// UDTGenerator generates UDT values
type UDTGenerator struct {
	BaseGenerator
	Config UDTConfig
}

// Function type for UDT generation
type UDTGenerateFunc func(g *UDTGenerator) interface{}

// Global variable to hold the UDT generation function
var udtGenerateFunc UDTGenerateFunc

// RegisterGenerateUDT registers a function for UDT generation
func RegisterGenerateUDT(fn UDTGenerateFunc) {
	udtGenerateFunc = fn
}

// Generate generates a random UDT
func (g *UDTGenerator) Generate() interface{} {
	if udtGenerateFunc != nil {
		return udtGenerateFunc(g)
	}
	// Default implementation as a fallback
	return make(map[string]interface{})
}

// TupleGenerator generates tuple values
type TupleGenerator struct {
	BaseGenerator
	Config TupleConfig
}

// Function type for Tuple generation
type TupleGenerateFunc func(g *TupleGenerator) interface{}

// Global variable to hold the Tuple generation function
var tupleGenerateFunc TupleGenerateFunc

// RegisterGenerateTuple registers a function for Tuple generation
func RegisterGenerateTuple(fn TupleGenerateFunc) {
	tupleGenerateFunc = fn
}

// Generate generates a random tuple
func (g *TupleGenerator) Generate() interface{} {
	if tupleGenerateFunc != nil {
		return tupleGenerateFunc(g)
	}
	// Default implementation as a fallback
	return make([]interface{}, len(g.Config.Elements))
}

// NumericGenerator generates numeric values with range constraints
type NumericGenerator struct {
	BaseGenerator
	Config  Range
	IsFloat bool
}

// Generate generates a random numeric value
func (g *NumericGenerator) Generate() interface{} {
	if g.IsFloat {
		min, max := 0.0, 100.0
		if g.Config.Min != nil {
			if minVal, ok := g.Config.Min.(float64); ok {
				min = minVal
			}
		}
		if g.Config.Max != nil {
			if maxVal, ok := g.Config.Max.(float64); ok {
				max = maxVal
			}
		}
		return gofakeit.Float64Range(min, max)
	} else {
		min, max := 0, 1000000
		if g.Config.Min != nil {
			if minVal, ok := g.Config.Min.(int); ok {
				min = minVal
			}
		}
		if g.Config.Max != nil {
			if maxVal, ok := g.Config.Max.(int); ok {
				max = maxVal
			}
		}
		return gofakeit.IntRange(min, max)
	}
}

// StringGenerator generates string values
type StringGenerator struct {
	BaseGenerator
	Column Column
}

// StringPatternHandler defines a function type for handling patterns in strings
type StringPatternHandler func(pattern string) string

// Global variable to hold the pattern handler function
var stringPatternHandler StringPatternHandler

// RegisterStringPatternHandler registers a function for handling string patterns
func RegisterStringPatternHandler(handler StringPatternHandler) {
	stringPatternHandler = handler
}

// Generate generates a random string value
func (g *StringGenerator) Generate() interface{} {
	if len(g.Column.Value) > 0 {
		return gofakeit.RandomString(g.Column.Value)
	}
	if g.Column.Pattern != "" {
		// Use the registered pattern handler if available
		if stringPatternHandler != nil {
			return stringPatternHandler(g.Column.Pattern)
		}
		// Otherwise, just return the pattern
		return g.Column.Pattern
	}
	if strings.Contains(g.Column.Name, "name") {
		return gofakeit.Name()
	}
	return gofakeit.Word()
}

// TimeGenerator generates time/date values
type TimeGenerator struct {
	BaseGenerator
	Column Column
}

// Function type for Time generation
type TimeGenerateFunc func(g *TimeGenerator) interface{}

// Global variable to hold the Time generation function
var timeGenerateFunc TimeGenerateFunc

// RegisterGenerateTime registers a function for Time generation
func RegisterGenerateTime(fn TimeGenerateFunc) {
	timeGenerateFunc = fn
}

// Generate generates a random time value
func (g *TimeGenerator) Generate() interface{} {
	if timeGenerateFunc != nil {
		return timeGenerateFunc(g)
	}
	// Default implementation as a fallback
	format := "2006-01-02 15:04:05"
	if g.Column.Format != "" {
		format = g.Column.Format
	}

	isDateOnly := g.Column.Type == "date"

	// Default to current time
	if isDateOnly {
		return time.Now().Format(format)
	}
	return time.Now()
}

// JSONGenerator generates JSON objects
type JSONGenerator struct {
	BaseGenerator
	Config JSONConfig
}

// Generate generates a random JSON object
func (g *JSONGenerator) Generate() interface{} {
	jsonObj := make(map[string]interface{})

	if len(g.Config) > 0 {
		for _, field := range g.Config {
			jsonObj[field.Name] = generateRandomValueWithRange(field.Type, field.Range)
		}
	} else {
		numKeys := gofakeit.IntRange(1, 5)
		for i := 0; i < numKeys; i++ {
			field := gofakeit.Word()
			valueType := getRandomValueType()
			jsonObj[field] = generateRandomValue(valueType)
		}
	}

	return jsonObj
}

// Helper functions

// generateRandomValue generates a random value of the specified type
func generateRandomValue(valueType string) interface{} {
	switch valueType {
	case "string":
		return gofakeit.Word()
	case "int":
		return gofakeit.IntRange(0, 1000)
	case "float":
		return gofakeit.Float64Range(0.0, 1000.0)
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

// generateRandomValueWithRange generates a random value of the specified type with range constraints
func generateRandomValueWithRange(valueType string, rangeConfig Range) interface{} {
	switch valueType {
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
	default:
		return generateRandomValue(valueType)
	}
}

// getRandomValueType returns a random value type for JSON fields
func getRandomValueType() string {
	types := []string{"string", "int", "float", "bool", "date", "email", "url"}
	return types[gofakeit.IntRange(0, len(types)-1)]
}
