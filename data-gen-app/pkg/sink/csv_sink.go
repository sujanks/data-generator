package sink

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/sujanks/data-gen-app/pkg/types"
)

// CSVSink implements DataSink interface for CSV file output
type CSVSink struct {
	outputDir string
	writers   map[string]*csv.Writer
	files     map[string]*os.File
	headers   map[string][]string
	mu        sync.Mutex
	schema    *types.Schema
	tableMap  map[string]*types.Table // Cache for quick table lookup
}

// NewCSVSink creates a new CSV sink that writes to the specified directory
func NewCSVSink(outputDir string, schema *types.Schema) (*CSVSink, error) {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	// Create a map for quick table lookup
	tableMap := make(map[string]*types.Table)
	for i := range schema.Tables {
		table := &schema.Tables[i]
		tableMap[table.Name] = table
	}

	return &CSVSink{
		outputDir: outputDir,
		writers:   make(map[string]*csv.Writer),
		files:     make(map[string]*os.File),
		headers:   make(map[string][]string),
		schema:    schema,
		tableMap:  tableMap,
	}, nil
}

// InsertRecord writes a record to the appropriate CSV file
func (s *CSVSink) InsertRecord(tableName string, record map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get table from the map
	table, exists := s.tableMap[tableName]
	if !exists {
		return fmt.Errorf("table not found: %s", tableName)
	}

	if s.writers[tableName] == nil {
		file, err := os.Create(fmt.Sprintf("%s/%s.csv", s.outputDir, tableName))
		if err != nil {
			return err
		}
		writer := csv.NewWriter(file)
		s.writers[tableName] = writer
		s.files[tableName] = file

		// Write header
		var header []string
		for _, col := range table.Columns {
			header = append(header, col.Name)
		}
		if err := writer.Write(header); err != nil {
			return err
		}
	}

	// Write record in the same order as columns
	var values []string
	for _, col := range table.Columns {
		value := record[col.Name]
		values = append(values, formatValue(value))
	}

	return s.writers[tableName].Write(values)
}

// Close closes all open files
func (s *CSVSink) Close() error {
	var errors []string

	// Flush and close all writers and files
	for tableName, writer := range s.writers {
		writer.Flush()
		if err := writer.Error(); err != nil {
			errors = append(errors, fmt.Sprintf("failed to flush writer for table %s: %v", tableName, err))
		}

		if file, exists := s.files[tableName]; exists {
			if err := file.Close(); err != nil {
				errors = append(errors, fmt.Sprintf("failed to close file for table %s: %v", tableName, err))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("errors while closing CSV sink: %s", strings.Join(errors, "; "))
	}
	return nil
}

// formatValue converts a value to its string representation
func formatValue(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case int:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%.2f", v)
	case bool:
		return fmt.Sprintf("%v", v)
	case map[string]interface{}:
		// Sort keys for consistent output
		var keys []string
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var pairs []string
		for _, k := range keys {
			pairs = append(pairs, fmt.Sprintf("%s:%v", k, v[k]))
		}
		return fmt.Sprintf("{%s}", strings.Join(pairs, ","))
	default:
		return fmt.Sprintf("%v", v)
	}
}

// JSONToString converts a map to a JSON-like string representation
func JSONToString(data map[string]interface{}) (string, error) {
	// Get all keys and sort them
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build string with sorted keys
	parts := make([]string, 0, len(data))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%v", k, data[k]))
	}
	return "{" + strings.Join(parts, ",") + "}", nil
}
