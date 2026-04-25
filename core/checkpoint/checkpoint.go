package checkpoint

import "time"

// Checkpoint holds the current processing state for a job.
type Checkpoint struct {
	PartIndex int       `json:"part_index"`
	Offset    int64     `json:"offset"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Store is the interface both the CLI and worker satisfy.
type Store interface {
	// Save atomically persists the current part index and byte offset.
	Save(partIndex int, offset int64) error

	// Current returns the last saved checkpoint.
	// Returns a zero-value Checkpoint (PartIndex: 0, Offset: 0) if none exists.
	Current() Checkpoint
}
