package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"runtime/debug"
	"strings"

	"github.com/johannessarpola/go-chunker/internal/chunk"
	"github.com/urfave/cli/v3"
)

var version = ""
var Verbose bool

func getVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	if info.Main.Version == "" {
		return version
	}
	return info.Main.Version
}

func createOutputDir(path string) error {
	verbosePrint("creating output dir %s\n", path)
	return os.Mkdir(path, os.ModePerm)
}

func verbosePrint(format string, args ...any) {
	if Verbose {
		fmt.Printf(format, args...)
	}
}

func main() {
	var input, output string
	var size int64
	var meta bool

	cmd := &cli.Command{
		Name:    "chunk",
		Usage:   "Chunks the input directory or file into chunks",
		Version: getVersion(),
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
			&cli.BoolFlag{
				Name:        "meta",
				Usage:       "meta",
				Aliases:     []string{"m"},
				Value:       false,
				Destination: &meta,
			},
			&cli.BoolFlag{
				Name:        "verbose",
				Usage:       "verbose",
				Aliases:     []string{"v"},
				Destination: &Verbose,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {

			inputPath := path.Join(input)
			var base, ext string

			isDir, err := chunk.IsDir(inputPath)
			if err != nil {
				verbosePrint("could not check if input %s is directory or not\n", inputPath)
				return err
			}

			var source chunk.Source[string]
			if isDir {

				base = path.Base(inputPath)
				ext, err = chunk.GetFirstExtensionInDir(inputPath)
				if err != nil {
					verbosePrint("could not get extension from input directory from %s\n", inputPath)
					return err
				}

				source, err = chunk.NewDirectorySource(inputPath)
				if err != nil {
					verbosePrint("could not create directory source from %s\n", inputPath)
					return err
				}

			} else {
				base = path.Base(inputPath)
				ext = path.Ext(inputPath)

				source, err = chunk.NewFileSource(inputPath)
				if err != nil {
					verbosePrint("could not create file source from %s\n", inputPath)
					return err
				}
			}

			err = createOutputDir(output)
			if err != nil && err.Error() != "mkdir temp: file exists" {
				verbosePrint("failed to create directory %s\n", output)
				return err
			}
			o := chunk.Output{
				Prefix: strings.Replace(base, ext, "", 1),
				Dir:    output,
				Ext:    ext,
			}

			total, err := source.Total()
			verbosePrint("determined the total rows to be %d\n", total)
			if err != nil {
				verbosePrint("could not get total count from input %s\n", inputPath)
				return err
			}

			workers := math.Ceil(float64(total) / float64(size))
			pw := chunk.NewParWriter(int(workers), total, meta)

			verbosePrint("running writers for output files to directory %s with extension %s and prefix %s\n", o.Dir, o.Ext, o.Prefix)
			err = pw.Run(source, o)
			if err != nil {
				verbosePrint("error running writers for output for %s with extension %s and prefix %s\n", o.Dir, o.Ext, o.Prefix)
				return err
			}
			return nil
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}

}
