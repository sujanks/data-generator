package sink

type DataSink interface {
	InsertRcord(string, map[string]interface{})
}

