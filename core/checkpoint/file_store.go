package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// FileStore is a Store backed by a local file. Used by the CLI.
type FileStore struct {
	path string
	mu   sync.Mutex
	cp   Checkpoint
}

// NewFileStore creates a FileStore at the given path and loads any
// existing checkpoint from disk. Starts from zero if none exists.
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
			s.cp = Checkpoint{}
			return nil
		}
		return fmt.Errorf("open checkpoint: %w", err)
	}
	return json.Unmarshal(data, &s.cp)
}

// Save atomically writes the part index and offset to disk using
// a tmp file + rename, preventing partial writes from corrupting state.
func (s *FileStore) Save(partIndex int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cp.PartIndex = partIndex
	s.cp.Offset = offset
	s.cp.UpdatedAt = time.Now().UTC()

	data, err := json.MarshalIndent(s.cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write tmp checkpoint: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("atomic rename checkpoint: %w", err)
	}

	return nil
}

// Current returns the last saved checkpoint.
func (s *FileStore) Current() Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cp
}
