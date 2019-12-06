package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

func genRecords(messages chan string, client *kinesis.Kinesis, streamName string, rate int) {
	for {
		records := make([]*kinesis.PutRecordsRequestEntry, 0, rate)
		partitionKey := strconv.Itoa(rand.Intn(10000000000))
		for i := 0; i < rate; i++ {
			records = append(records, &kinesis.PutRecordsRequestEntry{
				Data:         []byte(RandomString(5)),
				PartitionKey: &partitionKey,
			})
		}

		record := kinesis.PutRecordsInput{
			Records:    records,
			StreamName: &streamName,
		}
		_, err := client.PutRecords(&record)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		messages <- string("Put records")
		time.Sleep(1000 * time.Millisecond)
	}
}

// Put dummy records to the given stream
func PutDummy(streamName string, rate int) {
	client := newClient(newSession())

	messages := make(chan string, rate)

	go genRecords(messages, client, streamName, rate)

	accumRecords := 0
	for {
		accumRecords += rate
		fmt.Println(<-messages + " [" + strconv.Itoa(accumRecords) + "]")
	}
}
