package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func SetInterval(someFunc func(), milliseconds int, async bool) chan bool {
	interval := time.Duration(milliseconds) * time.Millisecond

	ticker := time.NewTicker(interval)
	clear := make(chan bool)

	go func() {
		for {
			select {
			case <-ticker.C:
				if async {
					go someFunc()
				} else {
					someFunc()
				}
			case <-clear:
				ticker.Stop()
				return
			}
		}
	}()

	return clear
}

type QueueMsg struct {
	MsgType string      `json:"type"`
	Payload interface{} `json:"data"`
}

func main() {
	fmt.Println("hello world")
}

type Config struct {
	queueUrls    []string
	messagesSize int
	messagesPool int
	checkInteval int
}

type QueuePolling struct {
	config  Config
	process []chan bool
	actions map[string]QueueAction
	queue   *SQS
}

type QueueAction interface {
	Push(*sqs.Message)
	Run() []*sqs.Message
}

func NewPool(config Config) *QueuePolling {
	queue := NewSQS(config.messagesSize)
	return &QueuePolling{
		config: config,
		queue:  queue,
	}
}

func (p *QueuePolling) Start() {
	go func() {
		processing := SetInterval(p.Run, p.config.checkInteval, false)
		p.process = append(p.process, processing)
	}()
}

func (p *QueuePolling) Stop() {
	for _, value := range p.process {
		value <- true
	}
}

func (p *QueuePolling) AddAction(name string, action QueueAction) {
	p.actions[name] = action
}

func (p *QueuePolling) GetAction(name string) QueueAction {
	action, ok := p.actions[name]
	if ok {
		return action
	}
	return nil
}

func (p *QueuePolling) QueueProcessor(receiveNumber int, url string, callback func([]*sqs.Message) []*sqs.Message) {
	if p.queue == nil {
		return
	}

	dataReceived := []*sqs.Message{}
	for i := 0; i < receiveNumber; i++ {
		data, err := p.queue.ReceiverMessage(url)
		if err != nil {
			continue
		}
		dataReceived = append(dataReceived, data...)
	}
	if len(dataReceived) <= 0 {
		return
	}

	messageDelete := callback(dataReceived)
	if len(messageDelete) <= 0 {
		return
	}

	deleteMessageBatch := []*sqs.DeleteMessageBatchRequestEntry{}
	for _, v := range messageDelete {
		deleteMessageBatch = append(
			deleteMessageBatch,
			&sqs.DeleteMessageBatchRequestEntry{
				Id:            v.MessageId,
				ReceiptHandle: v.ReceiptHandle,
			},
		)
		if len(deleteMessageBatch) == 10 {
			//TODO: handle when delete message error
			p.queue.DeleteBatchMessage(&url, deleteMessageBatch)
			deleteMessageBatch = []*sqs.DeleteMessageBatchRequestEntry{}
		}
	}
	if len(deleteMessageBatch) > 0 {
		p.queue.DeleteBatchMessage(&url, deleteMessageBatch)
	}
}

func (p *QueuePolling) getDataCallback(recevied []*sqs.Message) []*sqs.Message {
	retval := []*sqs.Message{}
	actionChanged := map[string]QueueAction{}
	for _, value := range recevied {
		data := QueueMsg{}
		err := json.Unmarshal([]byte(*value.Body), &data)
		if err != nil {
			retval = append(retval, value)
			continue
		}
		action := p.GetAction(data.MsgType)
		if action == nil {
			retval = append(retval, value)
			continue
		}

		action.Push(value)

		exist := actionChanged[data.MsgType]
		if exist == nil {
			actionChanged[data.MsgType] = action
		}
	}

	for _, value := range actionChanged {
		deletedData := value.Run()
		retval = append(retval, deletedData...)
	}

	return retval
}

func (p *QueuePolling) Run() {
	numberReceive := p.config.messagesPool / p.config.messagesSize
	for _, v := range p.config.queueUrls {
		p.QueueProcessor(numberReceive, v, p.getDataCallback)
	}
}
