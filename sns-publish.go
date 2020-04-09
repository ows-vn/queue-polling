package squeue

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
)

func SNSClient() *sns.SNS {
	return sns.New(AwsSession())
}

func SendSNSMessage(topic, typeMsg string, data interface{}) error {
	msgObj := QueuePayload{
		MsgType: typeMsg,
		Payload: data,
	}
	msg, err := json.Marshal(msgObj)
	if err != nil {
		return err
	}
	input := sns.PublishInput{
		Message:  aws.String(string(msg)),
		TopicArn: aws.String(topic),
	}
	_, err = SNSClient().Publish(&input)
	if err != nil {
		return err
	}

	return nil
}
