// Package checkpoint manages resumable processing state.
//
// The Store interface decouples checkpoint logic from the storage backend,
// allowing the CLI to use a local file while a backend worker uses Postgres
// or Redis — without changing any processing logic.
package checkpoint

import (
	"time"
)

// Checkpoint holds the current processing state for a job.
type Checkpoint struct {
	Offset    int64     `json:"offset"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Store is the interface both the CLI and worker satisfy.
// The CLI passes a FileStore; the worker passes a database-backed store.
type Store interface {
	// Save atomically persists the current offset.
	Save(offset int64) error

	// Offset returns the last saved offset, or 0 if none exists.
	Offset() int64
}
