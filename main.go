package squeue

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	jsoniter "github.com/json-iterator/go"
)

var (
	json     = jsoniter.ConfigCompatibleWithStandardLibrary
	maxRetry = 5
)

type QueueMessage = sqs.Message

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

type QueuePayload struct {
	MsgType string      `json:"type"`
	Payload interface{} `json:"data"`
}

type Config struct {
	MainQueue    string
	QueueUrls    []string
	MessagesSize int
	MessagesPool int
	CheckInteval int
}

type QueuePolling struct {
	config     Config
	process    []chan bool
	actions    map[string]QueueAction
	failoutMsg map[string]int
	queue      *SQS
}

type QueueAction interface {
	Push(*QueueMessage)
	Run() []*QueueMessage
	Handle(data interface{}) bool
}

func NewPool(config Config) *QueuePolling {
	queue := NewSQS(config.MessagesSize)
	return &QueuePolling{
		config:     config,
		queue:      queue,
		actions:    map[string]QueueAction{},
		process:    []chan bool{},
		failoutMsg: map[string]int{},
	}
}

func (p *QueuePolling) Start() {
	go func() {
		processing := SetInterval(p.Run, p.config.CheckInteval, false)
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
		fmt.Fprintln(os.Stderr, "Not have queue")
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
		data := QueuePayload{}
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
	numberReceive := p.config.MessagesPool / p.config.MessagesSize
	for _, v := range p.config.QueueUrls {
		p.QueueProcessor(numberReceive, v, p.getDataCallback)
	}
}

func (p *QueuePolling) StartPolling() {
	go func() {
		chnMessages := make(chan *sqs.Message, p.config.MessagesSize)
		go p.queue.pollSqs(p.config.MainQueue, p.config.CheckInteval, chnMessages)

		for msg := range chnMessages {
			isDone := p.ProcessMessage(msg)
			if isDone {
				_ = p.queue.DeleteMessage(aws.String(p.config.MainQueue), msg.ReceiptHandle)
				continue
			}
		}
	}()
}

func (p *QueuePolling) ProcessMessage(msg *QueueMessage) bool {
	data := QueuePayload{}
	err := json.Unmarshal([]byte(*msg.Body), &data)
	if err != nil {
		return false
	}
	action := p.GetAction(data.MsgType)
	if action == nil {
		return false
	}
	return action.Handle(data.Payload)
}

func AwsSession() *session.Session {
	return session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
}
