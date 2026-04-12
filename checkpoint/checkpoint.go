package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type Checkpoint struct {
	Offset    int64     `json:"offset"`
	File      string    `json:"file"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Manager struct {
	path string
	mu   sync.Mutex
	cp   Checkpoint
}

func (m *Manager) load() error {
	data, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			m.cp = Checkpoint{Offset: 0}
			return nil
		}
		return fmt.Errorf("open checkpoint: %w", err)
	}

	return json.Unmarshal(data, &m.cp)
}

func (m *Manager) Save(offset int64, file string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cp.Offset = offset
	m.cp.File = file
	m.cp.UpdatedAt = time.Now().UTC()

	data, err := json.MarshalIndent(m.cp, "", "  ")
	if err != nil {
		return err
	}

	tmpFile := m.path + ".tmp"

	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpFile, m.path)
}

func (m *Manager) Offset() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cp.Offset
}

func New(path string) (*Manager, error) {
	m := &Manager{path: path}

	if err := m.load(); err != nil {
		return nil, err
	}

	return m, nil
}
