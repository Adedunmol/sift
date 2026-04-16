package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// FileStore is a Store backed by a local file.
// Used by the CLI. The worker uses a different Store implementation
// backed by Postgres or Redis.
type FileStore struct {
	path string
	mu   sync.Mutex
	cp   Checkpoint
}

// NewFileStore creates a FileStore at the given path and loads any
// existing checkpoint from disk. If no checkpoint file exists, it
// starts from offset 0.
func NewFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path}

	if err := s.load(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *FileStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.cp = Checkpoint{Offset: 0}
			return nil
		}
		return fmt.Errorf("open checkpoint: %w", err)
	}

	return json.Unmarshal(data, &s.cp)
}

// Save atomically writes the offset to disk using a tmp file + rename,
// preventing partial writes from corrupting checkpoint state.
func (s *FileStore) Save(offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cp.Offset = offset
	s.cp.UpdatedAt = time.Now().UTC()

	data, err := json.MarshalIndent(s.cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	tmpFile := s.path + ".tmp"

	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("write tmp checkpoint: %w", err)
	}

	if err := os.Rename(tmpFile, s.path); err != nil {
		return fmt.Errorf("atomic rename checkpoint: %w", err)
	}

	return nil
}

// Offset returns the last saved offset.
func (s *FileStore) Offset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cp.Offset
}
