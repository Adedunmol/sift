package output

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/Adedunmol/sift/core/parser"
)

// FileWriter is a Writer backed by a local CSV file.
// Used by the CLI. The worker uses a different Writer implementation
// that targets object storage.
type FileWriter struct {
	file *os.File
	csv  *csv.Writer
}

// NewFileWriter opens path in append mode, creating it if it does not exist.
// If the file is new, the CSV header row is written immediately.
// Call Flush() and then close the underlying file when done.
func NewFileWriter(path string) (*FileWriter, error) {
	_, statErr := os.Stat(path)
	fileExists := statErr == nil

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open output file %q: %w", path, err)
	}

	w := &FileWriter{
		file: f,
		csv:  csv.NewWriter(f),
	}

	if !fileExists {
		if err := w.writeHeader(); err != nil {
			f.Close()
			return nil, err
		}
	}

	return w, nil
}

func (w *FileWriter) writeHeader() error {
	if err := w.csv.Write([]string{"id", "url", "text", "delete"}); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	return nil
}

// Write appends one CSV row per tweet. The delete column is set to "false"
// by default — the user reviews the CSV and marks rows for deletion manually.
func (w *FileWriter) Write(tweets []parser.Tweet) error {
	for _, t := range tweets {
		row := []string{
			strconv.FormatInt(t.ID, 10),
			t.URL(),
			t.Text,
			"false",
		}
		if err := w.csv.Write(row); err != nil {
			return fmt.Errorf("write csv row for tweet %d: %w", t.ID, err)
		}
	}
	return nil
}

// Flush commits any buffered CSV data to the underlying file.
// Must be called before the program exits or the file is closed.
func (w *FileWriter) Flush() error {
	w.csv.Flush()
	if err := w.csv.Error(); err != nil {
		return fmt.Errorf("flush csv writer: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying file.
// Satisfies io.Closer — useful when deferring cleanup.
func (w *FileWriter) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close output file: %w", err)
	}
	return nil
}
