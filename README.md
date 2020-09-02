# databus_client


Usage:
```
package main

import (
	"github.com/zhuxingtao/databus_client/client"
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
)

var (
	wg sync.WaitGroup
)

type Handler struct {
}

func (*Handler) TopicName() string {
	return "qingtian.node_info"
}

func (*Handler) HandleInsert(row *client.ChangeRow) (int32, error) {
	fmt.Printf("insert")
	return 1, nil
}

func (*Handler) HandleUpdate(row *client.ChangeRow) (int32, error) {
	fmt.Printf("update")
	return 2, nil
}

func (*Handler) HandleDelete(row *client.ChangeRow) (int32, error) {
	fmt.Printf("delete")
	return 3, nil
}

func main() {
	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, nil)
	if err != nil {
		panic(err)
	}
	topicName := "qingtian.node_info"
	partitionList, err := consumer.Partitions(topicName)
	if err != nil {
		panic(err)
	}
	handler := &Handler{} //you need implement 3 method : HandleUpdate, HandleDelete, HandleInsert
	dispatcher := client.NewDispatcher() // all events will be handled by dispatcher.Dispatch()
	dispatcher.Register(handler)
	for partition := range partitionList {
		pc, err := consumer.ConsumePartition(topicName, int32(partition), sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}
		defer pc.AsyncClose()
		wg.Add(1)
		go func(sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range pc.Messages() {
				//			fmt.Printf("%s---Partition:%d\nOffset:%d\nKey:%s\n Value:%s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
				msgParser := client.NewMessageParser()
				changeRows, err := msgParser.Parse(msg.Value)
				if err != nil {
					panic(err)
				}
				err = dispatcher.Dispatch(topicName, changeRows)
				if err != nil {
					fmt.Printf("ERR %v", err)
				}
			}

		}(pc)
	}
	wg.Wait()
	consumer.Close()
}
```
