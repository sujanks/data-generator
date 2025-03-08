package sink

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// CSVSink implements DataSink interface for CSV file output
type CSVSink struct {
	outputDir string
	writers   map[string]*csv.Writer
	files     map[string]*os.File
	headers   map[string][]string
}

// NewCSVSink creates a new CSV sink that writes to the specified directory
func NewCSVSink(outputDir string) (*CSVSink, error) {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	return &CSVSink{
		outputDir: outputDir,
		writers:   make(map[string]*csv.Writer),
		files:     make(map[string]*os.File),
		headers:   make(map[string][]string),
	}, nil
}

// InsertRecord writes a record to the appropriate CSV file
func (s *CSVSink) InsertRecord(tableName string, data map[string]interface{}) error {
	// Initialize writer if not exists
	if _, exists := s.writers[tableName]; !exists {
		// Create file
		filename := filepath.Join(s.outputDir, tableName+".csv")
		file, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("failed to create CSV file for table %s: %v", tableName, err)
		}
		s.files[tableName] = file

		// Create writer
		writer := csv.NewWriter(file)
		s.writers[tableName] = writer

		// Extract and sort headers
		var headers []string
		for k := range data {
			headers = append(headers, k)
		}
		sort.Strings(headers)
		s.headers[tableName] = headers

		// Write headers
		if err := writer.Write(headers); err != nil {
			return fmt.Errorf("failed to write headers for table %s: %v", tableName, err)
		}
	}

	// Convert data to string slice in header order
	var record []string
	for _, header := range s.headers[tableName] {
		value := data[header]
		record = append(record, formatValue(value))
	}

	// Write record
	if err := s.writers[tableName].Write(record); err != nil {
		return fmt.Errorf("failed to write record for table %s: %v", tableName, err)
	}

	// Flush after each write to ensure data is written to disk
	s.writers[tableName].Flush()
	return s.writers[tableName].Error()
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
	case int, int32, int64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%.2f", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case map[string]interface{}: // For JSON fields
		jsonStr, err := JSONToString(v)
		if err != nil {
			return fmt.Sprintf("error: %v", err)
		}
		return jsonStr
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
