package main

import (
	"context"
	"log"
	"math"
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

			inputPath := path.Join(input)
			var base, ext string

			isDir, err := chunk.IsDir(inputPath)
			if err != nil {
				return err
			}

			var source chunk.Source[string]
			if isDir {

				base = path.Base(inputPath)
				ext, err = chunk.GetFirstExtensionInDir(inputPath)
				if err != nil {
					return err
				}

				source, err = chunk.NewDirectorySource(inputPath)
				if err != nil {
					return err
				}

			} else {
				base = path.Base(inputPath)
				ext = path.Ext(inputPath)

				source, err = chunk.NewFileSource(inputPath)
				if err != nil {
					return err
				}
			}

			o := chunk.Output{
				Prefix: strings.Replace(base, ext, "", 1),
				Dir:    output,
				Ext:    ext,
			}

			total, err := source.Total()
			if err != nil {
				return err
			}

			workers := math.Ceil(float64(total) / float64(size))
			pw := chunk.NewParWriter(int(workers), total)

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
