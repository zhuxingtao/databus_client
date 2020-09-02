package client

type ConsumerHandler interface {
	TopicName() string
	HandleInsert(row *ChangeRow) (int32, error) //effect row,  error
	HandleUpdate(row *ChangeRow) (int32, error)
	HandleDelete(row *ChangeRow) (int32, error)
}
