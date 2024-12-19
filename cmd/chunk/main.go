package main

import (
	"context"
	"fmt"
	"github.com/urfave/cli/v3"
	"log"
	"os"
)

func main() {
	var input, output string
	var size int64

	cmd := &cli.Command{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "input",
				Usage:       "input file for chunking",
				Aliases:     []string{"i"},
				Destination: &input,
			},
			&cli.StringFlag{
				Name:        "output",
				Usage:       "ouptut directory for chunks",
				Value:       "out",
				Aliases:     []string{"o"},
				Destination: &output,
			},
			&cli.IntFlag{
				Name:        "size",
				Usage:       "size of chunk",
				Aliases:     []string{"s"},
				Destination: &size,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			fmt.Printf("input: %s\n", input)
			fmt.Printf("output: %s\n", output)
			fmt.Printf("size: %d\n", size)
			return nil
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
