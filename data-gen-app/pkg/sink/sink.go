package sink

// DataSink defines the interface for data output destinations
type DataSink interface {
	// InsertRecord inserts a single record into the sink
	InsertRecord(tableName string, data map[string]interface{}) error
}
