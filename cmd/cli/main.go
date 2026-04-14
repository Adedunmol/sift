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

//file, err := os.Open("tweets.js")
//if err != nil {
//return err
//}
//defer file.Close()
//
//cp, err := checkpoint.New() // checkpoint.json
//if err != nil {
//return err
//}
//
//stream, err := parser.NewStream(file, cp.Offset())
//if err != nil {
//return err
//}
//
//// --- CSV SETUP ---
//outFile, exists, err := csvout.OpenFile("output.csv")
//if err != nil {
//return err
//}
//defer outFile.Close()
//
//writer := csv.NewWriter(outFile)
//
//// write header only once
//if err := csvout.WriteHeader(writer, exists); err != nil {
//return err
//}
//
//// --- PROCESSING LOOP ---
//batchSize := 100
//var tweets []Tweet
//
//for {
//tweet, err := stream.Next()
//if err == io.EOF {
//break
//}
//if err != nil {
//return err
//}
//
//tweets = append(tweets, tweet)
//
//// process in batches
//if len(tweets) >= batchSize {
//filteredTweets, err := gemini.Process(tweets)
//if err != nil {
//return err
//}
//
//if err := csvout.WriteTweets(writer, filteredTweets); err != nil {
//return err
//}
//
//writer.Flush()
//if err := writer.Error(); err != nil {
//return err
//}
//
//// save checkpoint AFTER successful write
//if err := cp.Save(stream.Offset()); err != nil {
//return err
//}
//
//tweets = tweets[:0] // reset batch
//}
//}
//
//// --- HANDLE REMAINING ---
//if len(tweets) > 0 {
//filteredTweets, err := gemini.Process(tweets)
//if err != nil {
//return err
//}
//
//if err := csvout.WriteTweets(writer, filteredTweets); err != nil {
//return err
//}
//
//writer.Flush()
//if err := writer.Error(); err != nil {
//return err
//}
//
//if err := cp.Save(stream.Offset()); err != nil {
//return err
//}
//}
