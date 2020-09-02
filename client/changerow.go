package client

type ChangeRow struct {
	Id         int64
	Db         string
	Table      string
	Timestamp  int64
	OldValue   map[string]string
	NewValue   map[string]string
	ChangeType int32
}

const ColumnId = "id"

