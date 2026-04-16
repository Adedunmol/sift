package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/Adedunmol/sift/checkpoint"
	"github.com/Adedunmol/sift/evaluator"
	"github.com/Adedunmol/sift/output"
	"github.com/Adedunmol/sift/parser"
	"io"
	"net/http"
	"os"
)

func main() {
	os.Exit(CLI(os.Args[1:]))
}

func CLI(args []string) int {
	var app appEnv

	err := app.fromArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "argument error: %v\n", err)
		return 2
	}

	if err = app.run(); err != nil {
		fmt.Fprintf(os.Stderr, "runtime error: %v\n", err)
		return 1
	}

	return 0
}

type appEnv struct {
	client   http.Client
	archive  string
	criteria string
}

const DefaultCriteria = ""

func (a *appEnv) fromArgs(args []string) error {
	fl := flag.NewFlagSet("sift", flag.ContinueOnError)

	fl.StringVar(
		&a.archive, "a", "tweets.js", "name of the archive",
	)

	fl.StringVar(
		&a.criteria, "c", DefaultCriteria, "criteria to filter tweets",
	)

	return fl.Parse(args)
}

func (a *appEnv) run() error {

	gem := evaluator.NewGemini()

	file, err := os.Open(a.archive)
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	defer file.Close()

	cp, err := checkpoint.New("")
	if err != nil {
		return fmt.Errorf("checkpoint init: %w", err)
	}

	stream, err := parser.NewStream(file, cp.Offset())
	if err != nil {
		return fmt.Errorf("parser init: %w", err)
	}

	outFile, exists, err := output.OpenFile("output.csv")
	if err != nil {
		return fmt.Errorf("open csv: %w", err)
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)

	if err := output.WriteHeader(writer, exists); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	const batchSize = 100
	var tweets []*parser.Tweet

	for {
		tweet, err := stream.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("stream read: %w", err)
		}

		tweets = append(tweets, tweet)

		if len(tweets) >= batchSize {
			if err := a.processBatch(writer, cp, stream, tweets, gem); err != nil {
				return err
			}
			tweets = tweets[:0]
		}
	}

	if len(tweets) > 0 {
		if err := a.processBatch(writer, cp, stream, tweets, gem); err != nil {
			return err
		}
	}

	return nil
}

func (a *appEnv) processBatch(
	writer *csv.Writer,
	cp *checkpoint.Manager,
	stream *parser.Stream,
	tweets []*parser.Tweet,
	evaluator evaluator.Processor,
) error {

	filteredTweets, err := evaluator.Process(tweets)
	if err != nil {
		return fmt.Errorf("process tweets: %w", err)
	}

	if err := output.WriteTweets(writer, filteredTweets); err != nil {
		return fmt.Errorf("write csv: %w", err)
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush csv: %w", err)
	}

	if err := cp.Save(stream.Offset()); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return nil
}
