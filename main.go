package main

import (
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/urfave/cli"
)

var sess *session.Session
var region string

func main() {
	app := &cli.App{
		Name:  "kinetool",
		Usage: "Tool for Amazon Kinesis Streams",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "region, r",
				Usage:       "region",
				Destination: &region,
			},
		},
		Commands: []cli.Command{
			{
				Name:  "ls",
				Usage: "list of streams",
				Action: func(c *cli.Context) error {
					ListStreams()
					return nil
				},
			},
			{
				Name:    "lss",
				Aliases: []string{"lss"},
				Usage:   "list of shards of the given stream",
				Action: func(c *cli.Context) error {
					args := c.Args()
					ListShards(args.First())
					return nil
				},
			},
			{
				Name:    "tail",
				Aliases: []string{"tail"},
				Usage:   "tail a stream",
				Action: func(c *cli.Context) error {
					args := c.Args()
					TailStream(args.First())
					return nil
				},
			},
			{
				Name:    "put-dummy",
				Aliases: []string{"put-dummy"},
				Usage:   "generate and put test records, put-dummy STREAM_NAME PARTITION_KEY",
				Action: func(c *cli.Context) error {
					args := c.Args()
					PutDummy(args.First(), 100)
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
