package client

type MsgBody struct {
	ChangeRows []ChangeRow
}

const (
	ChangeTypeInsert = 1 // table field insert event
	ChangeTypeUpdate = 2 // table field update event
	ChangeTypeDelete = 3 // talbe field delete event
)
