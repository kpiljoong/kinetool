package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/jedib0t/go-pretty/table"
)

// Returns the list of streams
func ListStreams() {
	client := newClient(newSession())
	params := &kinesis.ListStreamsInput{}
	resp, err := client.ListStreams(params)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if len(resp.StreamNames) < 1 {
		fmt.Println("No streams")
	} else {
		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Stream"})
		for _, s := range resp.StreamNames {
			t.AppendRow([]interface{}{*s})
		}
		t.Render()
	}
}

// Prints the list of shards of the given stream
func ListShards(streamName string) {
	client := newClient(newSession())
	params := &kinesis.ListShardsInput{
		StreamName: &streamName,
	}
	resp, err := client.ListShards(params)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if len(resp.Shards) < 1 {
		fmt.Println("No shards")
	} else {
		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Shard ID", "Parent Shard ID", "Starting HashKey", "Ending HashKey"})
		for _, s := range resp.Shards {
			var ss = ParseShard(s)
			t.AppendRow([]interface{}{ss.ShardId, ss.ParentShardId, ss.StartingHashKey, ss.EndingHashKey})
		}
		t.Render()
	}
}
