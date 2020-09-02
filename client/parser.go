package client

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	canalProto "github.com/withlin/canal-go/protocol"
	"strconv"
)

type Parser interface {
	Parse([]byte) (*MsgBody, error)
}

type messageParser struct {
}

func NewMessageParser() Parser {
	return &messageParser{}
}

// msg parse
func (m *messageParser) Parse(msg []byte) (msgBody *MsgBody, err error) {
	if len(msg) <= 0 {
		return nil, errors.New("message parse msg is empty")
	}

	defer func() {
		if p := recover(); p != nil {
			switch x := p.(type) {
			case error:
				err = x
				fmt.Printf("%v", err)
			default:
				err = errors.New("UnKnow Panic")
				fmt.Printf("%v", err)
			}
		}
	}()

	msgEntries, err := canalProto.Decode(msg, false)
	if err != nil {
		return nil, fmt.Errorf("Decode message error")
	}
	return m.parseEntry(msgEntries.Entries)
}

func GetChangedFields(oldValue, newValue map[string]string) []string {
	changed := make([]string, 0)
	for k, _ := range newValue {
		if oldv, ok := oldValue[k]; ok {
			if oldv != newValue[k] {
				changed = append(changed, k)
			}
		}
	}
	return changed
}

func GetChangeType(row ChangeRow) int32 {
	return row.ChangeType
}

func (m *messageParser) parseEntry(entries []canalProto.Entry) (*MsgBody, error) {
	var changeRows []ChangeRow
	for _, entry := range entries {
		if entry.GetEntryType() == canalProto.EntryType_TRANSACTIONBEGIN ||
			entry.GetEntryType() == canalProto.EntryType_TRANSACTIONEND {
			continue
		}

		rowChange := new(canalProto.RowChange)
		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		if err != nil {
			fmt.Printf("unmarshal entry store value error %v", err.Error())
			continue
		}
		if rowChange.GetIsDdl() {
			continue
		}

		changeRowsOfEntry := getAndFormatChangeRow(rowChange, entry)
		changeRows = append(changeRows, changeRowsOfEntry...)
	}

	return &MsgBody{ChangeRows: changeRows}, nil
}

// 获取并格式化ChangeRow
func getAndFormatChangeRow(rowChange *canalProto.RowChange, entry canalProto.Entry) []ChangeRow {
	rowDatas := rowChange.GetRowDatas()
	changeRows := make([]ChangeRow, len(rowDatas))
	header := entry.GetHeader()

	var err error
	err = errors.New("")
	for i, rowData := range rowDatas {
		changeRow := &ChangeRow{}
		rowDataBeforeColumns := rowData.GetBeforeColumns()
		id := ""
		beforeColumns := make(map[string]string)
		for _, column := range rowDataBeforeColumns {
			beforeColumns[column.GetName()] = column.GetValue()
			if column.GetName() == ColumnId {
				id = column.GetValue()
			}
		}

		rowDataAfterColumns := rowData.GetAfterColumns()
		afterColumns := make(map[string]string)
		for _, column := range rowDataAfterColumns {
			afterColumns[column.GetName()] = column.GetValue()
			if column.GetName() == ColumnId {
				id = column.GetValue()
			}
		}
		changeRow.Id, err = strconv.ParseInt(id, 10, 64)
		if err != nil {
			continue
		}
		changeRow.OldValue = beforeColumns
		changeRow.NewValue = afterColumns
		changeRow.ChangeType = int32(header.GetEventType())
		changeRow.Db = header.GetSchemaName()
		changeRow.Table = header.GetTableName()
		changeRow.Timestamp = header.GetExecuteTime()

		changeRows[i] = *changeRow
	}

	return changeRows
}
