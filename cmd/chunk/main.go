package main

import (
	"context"
	"log"
	"os"
	"path"
	"strings"

	"github.com/johannessarpola/go-chunker/internal/chunk"
	"github.com/urfave/cli/v3"
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

			td := path.Join(input)
			base := path.Base(input)
			ext := path.Ext(input)

			source, err := chunk.ReadFile(td)
			if err != nil {
				return err
			}

			o := chunk.Output{
				Prefix: strings.Replace(base, ext, "", 1),
				Dir:    output,
				Ext:    ext,
			}

			workers := 10
			pw := chunk.NewParWriter(workers)

			err = pw.Run(source, o)
			if err != nil {
				return err
			}
			return nil
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}

}
