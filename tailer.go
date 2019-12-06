package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

func readShard(messages chan string, client *kinesis.Kinesis, streamName string, shardId string) {
	println("Shard Id: " + shardId)

	iter, err := getShardIterator(client, streamName, shardId)
	if err != nil {
		println(err)
		panic(err)
	}

	for {
		getParams := kinesis.GetRecordsInput{
			ShardIterator: iter,
		}
		resp, err := client.GetRecords(&getParams)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		for _, r := range resp.Records {
			messages <- fmt.Sprintf("[%s] %s: %s", shardId, *r.SequenceNumber, r.Data)
		}

		iter = resp.NextShardIterator
		if len(resp.Records) == 0 {
			time.Sleep(1000 * time.Millisecond)
		}
	}
}

// TailStream tails the given stream
// FORMAT: SEQUENCE_ID: DATA
func TailStream(streamName string) {
	println("FORMAT -> SEQUENCE_ID: DATA")
	client := newClient(newSession())

	shardIds, err := getShards(client, streamName)
	if err != nil {
		println("Err")
		panic(err)
	}

	messages := make(chan string, 1000)

	for _, shardId := range shardIds {
		go readShard(messages, client, streamName, shardId)
	}

	for {
		fmt.Println(<-messages)
	}
}
