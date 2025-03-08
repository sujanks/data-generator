package sink

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDataSink is a test implementation of DataSink
type TestDataSink struct {
	Records []map[string]interface{}
}

func (t *TestDataSink) InsertRecord(tableName string, data map[string]interface{}) error {
	t.Records = append(t.Records, data)
	return nil
}

func TestDataSinkInterface(t *testing.T) {
	// Create a test sink
	sink := &TestDataSink{
		Records: make([]map[string]interface{}, 0),
	}

	// Test data
	testData := map[string]interface{}{
		"id":   "TEST001",
		"name": "Test Record",
		"age":  30,
	}

	// Test inserting a record
	err := sink.InsertRecord("test_table", testData)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(sink.Records))
	assert.Equal(t, testData, sink.Records[0])

	// Test inserting multiple records
	testData2 := map[string]interface{}{
		"id":   "TEST002",
		"name": "Test Record 2",
		"age":  25,
	}

	err = sink.InsertRecord("test_table", testData2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(sink.Records))
	assert.Equal(t, testData2, sink.Records[1])
}
