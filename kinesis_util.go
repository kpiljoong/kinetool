package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func newSession() *session.Session {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	return sess
}

func newClient(sess *session.Session) *kinesis.Kinesis {
	return kinesis.New(sess)
}

func newFirehoseClient(sess *session.Session) *firehose.Firehose {
	return firehose.New(sess)
}

// SafeShard structure
type SafeShard struct {
	ShardId         string
	ParentShardId   string
	StartingHashKey string
	EndingHashKey   string
}

func ParseShard(s *kinesis.Shard) *SafeShard {
	var ss = &SafeShard{}
	if s.ShardId != nil {
		ss.ShardId = *s.ShardId
	}
	if s.ParentShardId != nil {
		ss.ParentShardId = *s.ParentShardId
	}
	if s.HashKeyRange != nil {
		if s.HashKeyRange.StartingHashKey != nil {
			ss.StartingHashKey = *s.HashKeyRange.StartingHashKey
		}
		if s.HashKeyRange.EndingHashKey != nil {
			ss.EndingHashKey = *s.HashKeyRange.EndingHashKey
		}
	}
	return ss
}

func getShards(client *kinesis.Kinesis, streamName string) ([]string, error) {
	params := &kinesis.DescribeStreamInput{
		StreamName: &streamName,
	}
	resp, err := client.DescribeStream(params)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	shards := make([]string, 0, len(resp.StreamDescription.Shards))
	for _, s := range resp.StreamDescription.Shards {
		shards = append(shards, *s.ShardId)
	}
	return shards, nil
}

func getShardIterator(client *kinesis.Kinesis, streamName string, shardId string) (*string, error) {
	shardIter, err := client.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardId),
		ShardIteratorType: aws.String("LATEST"),
		StreamName:        aws.String(streamName),
	})
	if err != nil {
		return nil, err
	}
	return shardIter.ShardIterator, nil
}
