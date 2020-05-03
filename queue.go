package squeue

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SQS struct {
	client *sqs.SQS
	size   int
}

func NewSQS(size int) *SQS {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	client := sqs.New(sess)
	return &SQS{
		client: client,
		size:   size,
	}
}

func (c *SQS) ReceiverMessage(url string) ([]*sqs.Message, error) {
	result, err := c.client.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            &url,
		MaxNumberOfMessages: aws.Int64(int64(c.size)),
	})

	if err != nil {
		fmt.Fprintln(os.Stderr, "error when receive message", err)
		return nil, err
	}

	return result.Messages, nil
}

func (c *SQS) pollSqs(url string, time int, chn chan<- *sqs.Message) {
	for {
		output, err := c.client.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            &url,
			MaxNumberOfMessages: aws.Int64(int64(c.size)),
			WaitTimeSeconds:     aws.Int64(int64(time / 1000)),
		})

		if err != nil {
			fmt.Fprintln(os.Stderr, "error when receive message", err)
			continue
		}

		for _, message := range output.Messages {
			chn <- message
		}
	}
}

func (c *SQS) DeleteMessage(url, receiptHandle *string) error {
	_, err := c.client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      url,
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error when delete message from sqs: ", err)
		return err
	}
	return nil
}

func (c *SQS) DeleteBatchMessage(url *string, batch []*sqs.DeleteMessageBatchRequestEntry) error {
	_, err := c.client.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
		Entries:  batch,
		QueueUrl: url,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error when delete message from sqs: ", err)
		return err
	}
	return nil
}
