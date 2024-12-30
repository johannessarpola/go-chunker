package chunk

import (
	"bufio"
	"os"

	"github.com/johannessarpola/gollections/result"
)

func asyncCountLines(filepath string) <-chan result.Result[int64] {
	rs := make(chan result.Result[int64], 1)
	go func() {
		file, err := os.Open(filepath)
		if err != nil {
			rs <- result.NewErr[int64](err)
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		lineCount := int64(0)

		for scanner.Scan() {
			lineCount++
		}

		if err := scanner.Err(); err != nil {
			rs <- result.NewErr[int64](err)
			return
		}

		rs <- result.NewOk(lineCount)
		close(rs)
	}()
	return rs
}
