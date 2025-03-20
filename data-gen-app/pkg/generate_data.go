package pkg

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
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

// JSONConfig defines the structure for JSON field generation
type JSONConfig struct {
	MinKeys int              `yaml:"min_keys,omitempty"`
	MaxKeys int              `yaml:"max_keys,omitempty"`
	Fields  []string         `yaml:"fields,omitempty"` // Optional predefined fields
	Types   []string         `yaml:"types,omitempty"`  // Optional value types for fields
	Ranges  map[string]Range `yaml:"ranges,omitempty"` // Field-specific ranges
}

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
}

const hashtag = '#'

func GenerateData(ds sink.DataSink, count int, profile string) {
	tables := readManifest(profile)
	sortedTables := sortTablesByDependency(tables.Tables)
	parentKeyValues := make(map[string][]string, 0)

	for _, table := range sortedTables {
		for i := 0; i < count; i++ {
			var tableData = make(map[string]interface{})
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

				if col.Parent {
					keyName := fmt.Sprintf("%s.%s", table.Name, col.Name)
					parentKeyValues[keyName] = append(parentKeyValues[keyName], fmt.Sprint(colValue))
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
	// Set defaults if not configured
	minKeys := 1
	maxKeys := 5
	if config.MinKeys > 0 {
		minKeys = config.MinKeys
	}
	if config.MaxKeys > 0 {
		maxKeys = config.MaxKeys
	}

	// Generate number of keys
	numKeys := gofakeit.IntRange(minKeys, maxKeys)

	// Create JSON object
	jsonObj := make(map[string]interface{})

	// Use predefined fields if available
	if len(config.Fields) > 0 {
		for i := 0; i < numKeys && i < len(config.Fields); i++ {
			field := config.Fields[i]
			var valueType string
			if i < len(config.Types) {
				valueType = config.Types[i]
			} else {
				valueType = getRandomValueType()
			}
			jsonObj[field] = generateValueByType(valueType, config.Ranges[field])
		}
	} else {
		// Generate random fields
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
