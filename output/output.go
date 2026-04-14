package output

import (
	"encoding/csv"
	"os"
	"strconv"

	"github.com/Adedunmol/sift/parser"
)

// OpenFile opens a CSV file in append mode.
// Creates it if it doesn't exist.
func OpenFile(path string) (*os.File, bool, error) {
	_, err := os.Stat(path)

	fileExists := err == nil

	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return nil, false, err
	}

	return f, fileExists, nil
}

func WriteHeader(w *csv.Writer, fileAlreadyExists bool) error {
	if fileAlreadyExists {
		return nil
	}
	return w.Write([]string{"id", "text"})
}

// WriteTweets appends tweets to CSV
func WriteTweets(w *csv.Writer, tweets []parser.Tweet) error {
	for _, t := range tweets {
		err := w.Write([]string{
			strconv.FormatInt(t.ID, 10),
			t.Text,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
