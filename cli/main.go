package main

import (
	"flag"
	"fmt"
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
		return 2
	}

	if err = app.run(); err != nil {
		fmt.Fprintf(os.Stderr, "runtime error: %v", err)
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
		&a.criteria, "c", DefaultCriteria, "the criteria to check tweets for",
	)
	if err := fl.Parse(args); err != nil {
		return err
	}
	return nil
}

func (a *appEnv) run() error {
	return nil
}
