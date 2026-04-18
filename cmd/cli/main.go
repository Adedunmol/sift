package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/Adedunmol/sift/core/evaluator"
	"io"
	"os"

	"github.com/Adedunmol/sift/core/checkpoint"
	"github.com/Adedunmol/sift/core/output"
	"github.com/Adedunmol/sift/core/parser"
)

func main() {
	os.Exit(CLI(os.Args[1:]))
}

// CLI parses args, wires real dependencies, and runs the app.
// Returns 0 on success, 1 on runtime error, 2 on argument error.
// Separated from main() so tests can call it directly with controlled args.
func CLI(args []string) int {
	cfg, err := parseArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "argument error: %v\n", err)
		return 2
	}

	app, err := newApp(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init error: %v\n", err)
		return 1
	}
	//defer app.writer.Close()

	if err := app.run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "runtime error: %v\n", err)
		return 1
	}

	return 0
}

// config holds the raw values parsed from CLI flags.
type config struct {
	archivePath    string
	checkpointPath string
	outputPath     string
	username       string
	criteria       evaluator.Criteria
}

func parseArgs(args []string) (config, error) {
	var cfg config

	fl := flag.NewFlagSet("sift", flag.ContinueOnError)

	fl.StringVar(&cfg.archivePath, "a", "tweets.js", "path to the X/Twitter archive JS file")
	fl.StringVar(&cfg.checkpointPath, "cp", ".sift-checkpoint.json", "path to checkpoint file")
	fl.StringVar(&cfg.outputPath, "o", "output.csv", "path to output CSV file")
	fl.StringVar(&cfg.username, "u", "", "X/Twitter username (used to build tweet URLs)")

	if err := fl.Parse(args); err != nil {
		return config{}, err
	}

	return cfg, nil
}

// app holds injected dependencies. All fields are interfaces so tests
// can substitute fakes without touching the filesystem or network.
type app struct {
	stream    streamer
	processor evaluator.Processor
	store     checkpoint.Store
	writer    output.Writer
	cleanup   func()
}

// streamer is a local interface over parser.Stream so tests can inject
// a fake tweet source. Only the methods the processing loop uses are included.
type streamer interface {
	Next(ctx context.Context) (*parser.Tweet, error)
	Offset() int64
}

// newApp wires the real filesystem and network dependencies for production use.
func newApp(cfg config) (*app, error) {
	store, err := checkpoint.NewFileStore(cfg.checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("checkpoint init: %w", err)
	}

	archiveFile, err := os.Open(cfg.archivePath)
	if err != nil {
		return nil, fmt.Errorf("open archive: %w", err)
	}

	stream, err := parser.NewStream(archiveFile, store.Offset(), cfg.username)
	if err != nil {
		archiveFile.Close()
		return nil, fmt.Errorf("parser init: %w", err)
	}

	writer, err := output.NewFileWriter(cfg.outputPath)
	if err != nil {
		archiveFile.Close()
		return nil, fmt.Errorf("output init: %w", err)
	}

	proc := evaluator.NewGemini(evaluator.GeminiConfig{
		Criteria: cfg.criteria,
	})

	return &app{
		stream:    stream,
		processor: proc,
		store:     store,
		writer:    writer,
		cleanup:   func() { archiveFile.Close() },
	}, nil
}

const batchSize = 100

// run executes the main processing loop.
// ctx controls cancellation — the worker passes a job-scoped context;
// the CLI passes context.Background().
func (a *app) run(ctx context.Context) error {
	defer a.cleanup()

	var batch []*parser.Tweet

	for {
		tweet, err := a.stream.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("stream read: %w", err)
		}

		batch = append(batch, tweet)

		if len(batch) >= batchSize {
			if err := a.processBatch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Flush the final partial batch.
	if len(batch) > 0 {
		if err := a.processBatch(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

// processBatch evaluates one batch of tweets, writes flagged results,
// and saves the checkpoint. All three steps use injected interfaces so
// tests can verify behaviour without filesystem or network access.
func (a *app) processBatch(ctx context.Context, batch []*parser.Tweet) error {
	flagged, err := a.processor.Process(ctx, batch)
	if err != nil {
		return fmt.Errorf("process batch: %w", err)
	}

	if len(flagged) > 0 {
		if err := a.writer.Write(flagged); err != nil {
			return fmt.Errorf("write output: %w", err)
		}

		if err := a.writer.Flush(); err != nil {
			return fmt.Errorf("flush output: %w", err)
		}
	}

	if err := a.store.Save(a.stream.Offset()); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return nil
}
