package client

import (
	"errors"
	"fmt"
	"sync"
)

type dispatcher struct {
	sync.Mutex
	handlers map[string]ConsumerHandler
}

func NewDispatcher() *dispatcher {
	handlers := make(map[string]ConsumerHandler)
	return &dispatcher{handlers: handlers}
}

func (d *dispatcher) Register(handler ConsumerHandler) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	d.handlers[handler.TopicName()] = handler
}

func (d *dispatcher) Dispatch(msgTopic string, body *MsgBody) (err error) {
	consumerHandler, ok := d.handlers[msgTopic]
	if !ok {
		return fmt.Errorf("msg topic not find")
	}
	fmt.Printf("gagd", consumerHandler)
	defer func() {
		if p := recover(); p != nil {
			switch x := p.(type) {
			case error:
				err = x
			default:
				err = errors.New("UnKnow Panic")
			}
		}
	}()

	var effectRows int32 = 0
	for _, changeRow := range body.ChangeRows {
		var effectRow int32 = 0
		switch changeRow.ChangeType {
		case ChangeTypeInsert:
			effectRow, err = consumerHandler.HandleInsert(&changeRow)
		case ChangeTypeUpdate:
			effectRow, err = consumerHandler.HandleUpdate(&changeRow)
		case ChangeTypeDelete:
			effectRow, err = consumerHandler.HandleDelete(&changeRow)
		}

		if err != nil {
			break
		} else {
			effectRows = effectRows + effectRow
		}
	}
	return err
}
