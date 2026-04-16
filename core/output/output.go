// Package output writes flagged tweets to a CSV sink.
//
// The Writer interface decouples the CSV format from the storage backend.
// The CLI uses a FileWriter backed by a local file; the worker uses a
// different Writer implementation that streams to object storage.
package output

import (
	"github.com/Adedunmol/sift/core/parser"
)

// Writer is the interface both the CLI and worker satisfy.
// Implementations must be safe for sequential use by a single goroutine
// (the processing loop calls Write in order; callers are responsible for
// their own synchronisation if multiple goroutines share a Writer).
type Writer interface {
	// Write appends flagged tweets to the output sink.
	Write(tweets []parser.Tweet) error

	// Flush ensures all buffered data is committed to the sink.
	// Must be called before closing the underlying resource.
	Flush() error
}
