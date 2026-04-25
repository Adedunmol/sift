package checkpoint_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Adedunmol/sift/core/checkpoint"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func tempPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "checkpoint.json")
}

func writeCheckpoint(t *testing.T, path string, cp checkpoint.Checkpoint) {
	t.Helper()
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		t.Fatalf("writeCheckpoint: marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("writeCheckpoint: write: %v", err)
	}
}

func readCheckpoint(t *testing.T, path string) checkpoint.Checkpoint {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("readCheckpoint: read: %v", err)
	}
	var cp checkpoint.Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		t.Fatalf("readCheckpoint: unmarshal: %v", err)
	}
	return cp
}

// ---------------------------------------------------------------------------
// NewFileStore / load
// ---------------------------------------------------------------------------

func TestNewFileStore_NoExistingFile(t *testing.T) {
	store, err := checkpoint.NewFileStore(tempPath(t))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	cp := store.Current()
	if cp.PartIndex != 0 || cp.Offset != 0 {
		t.Errorf("expected zero checkpoint, got %+v", cp)
	}
}

func TestNewFileStore_LoadsExistingCheckpoint(t *testing.T) {
	path := tempPath(t)
	writeCheckpoint(t, path, checkpoint.Checkpoint{PartIndex: 1, Offset: 42})

	store, err := checkpoint.NewFileStore(path)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	cp := store.Current()
	if cp.PartIndex != 1 || cp.Offset != 42 {
		t.Errorf("expected {PartIndex:1, Offset:42}, got %+v", cp)
	}
}

func TestNewFileStore_LoadsZeroCheckpoint(t *testing.T) {
	path := tempPath(t)
	writeCheckpoint(t, path, checkpoint.Checkpoint{PartIndex: 0, Offset: 0})

	store, err := checkpoint.NewFileStore(path)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	cp := store.Current()
	if cp.PartIndex != 0 || cp.Offset != 0 {
		t.Errorf("expected zero checkpoint, got %+v", cp)
	}
}

func TestNewFileStore_InvalidJSON(t *testing.T) {
	path := tempPath(t)
	if err := os.WriteFile(path, []byte("{not valid json"), 0644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	_, err := checkpoint.NewFileStore(path)
	if err == nil {
		t.Fatal("expected error for corrupt JSON, got nil")
	}
}

func TestNewFileStore_EmptyFile(t *testing.T) {
	path := tempPath(t)
	if err := os.WriteFile(path, []byte{}, 0644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	_, err := checkpoint.NewFileStore(path)
	if err == nil {
		t.Fatal("expected error for empty file, got nil")
	}
}

// ---------------------------------------------------------------------------
// Save
// ---------------------------------------------------------------------------

func TestSave_CreatesFileOnFirstSave(t *testing.T) {
	path := tempPath(t)
	store, err := checkpoint.NewFileStore(path)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := store.Save(0, 10); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("expected checkpoint file to exist after Save")
	}
}

func TestSave_PersistsPartIndexAndOffset(t *testing.T) {
	path := tempPath(t)
	store, err := checkpoint.NewFileStore(path)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := store.Save(2, 99); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cp := readCheckpoint(t, path)
	if cp.PartIndex != 2 || cp.Offset != 99 {
		t.Errorf("expected {PartIndex:2, Offset:99}, got %+v", cp)
	}
}

func TestSave_UpdatesInMemoryCheckpoint(t *testing.T) {
	store, err := checkpoint.NewFileStore(tempPath(t))
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := store.Save(1, 55); err != nil {
		t.Fatalf("Save: %v", err)
	}
	cp := store.Current()
	if cp.PartIndex != 1 || cp.Offset != 55 {
		t.Errorf("expected {PartIndex:1, Offset:55}, got %+v", cp)
	}
}

func TestSave_OverwritesPreviousCheckpoint(t *testing.T) {
	path := tempPath(t)
	store, _ := checkpoint.NewFileStore(path)

	_ = store.Save(0, 10)
	if err := store.Save(1, 200); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cp := readCheckpoint(t, path)
	if cp.PartIndex != 1 || cp.Offset != 200 {
		t.Errorf("expected {PartIndex:1, Offset:200}, got %+v", cp)
	}
}

func TestSave_SetsUpdatedAt(t *testing.T) {
	path := tempPath(t)
	before := time.Now().UTC().Add(-time.Second)

	store, _ := checkpoint.NewFileStore(path)
	if err := store.Save(0, 1); err != nil {
		t.Fatalf("Save: %v", err)
	}

	after := time.Now().UTC().Add(time.Second)
	cp := readCheckpoint(t, path)
	if cp.UpdatedAt.Before(before) || cp.UpdatedAt.After(after) {
		t.Errorf("UpdatedAt %v outside expected window [%v, %v]", cp.UpdatedAt, before, after)
	}
}

func TestSave_ZeroCheckpoint(t *testing.T) {
	path := tempPath(t)
	store, _ := checkpoint.NewFileStore(path)

	_ = store.Save(2, 100)
	if err := store.Save(0, 0); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cp := readCheckpoint(t, path)
	if cp.PartIndex != 0 || cp.Offset != 0 {
		t.Errorf("expected zero checkpoint, got %+v", cp)
	}
}

func TestSave_LargeOffset(t *testing.T) {
	path := tempPath(t)
	store, _ := checkpoint.NewFileStore(path)

	const big int64 = 1<<62 - 1
	if err := store.Save(0, big); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cp := readCheckpoint(t, path)
	if cp.Offset != big {
		t.Errorf("expected offset %d, got %d", big, cp.Offset)
	}
}

func TestSave_NoTmpFileLeft(t *testing.T) {
	path := tempPath(t)
	store, _ := checkpoint.NewFileStore(path)

	if err := store.Save(0, 7); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("expected .tmp file to be gone after Save, but it still exists")
	}
}

func TestSave_ReloadAfterSave(t *testing.T) {
	path := tempPath(t)

	store1, _ := checkpoint.NewFileStore(path)
	_ = store1.Save(3, 333)

	store2, err := checkpoint.NewFileStore(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	cp := store2.Current()
	if cp.PartIndex != 3 || cp.Offset != 333 {
		t.Errorf("expected {PartIndex:3, Offset:333}, got %+v", cp)
	}
}

// ---------------------------------------------------------------------------
// Current
// ---------------------------------------------------------------------------

func TestCurrent_DefaultIsZero(t *testing.T) {
	store, _ := checkpoint.NewFileStore(tempPath(t))
	cp := store.Current()
	if cp.PartIndex != 0 || cp.Offset != 0 {
		t.Errorf("expected zero checkpoint, got %+v", cp)
	}
}

func TestCurrent_ReflectsLastSave(t *testing.T) {
	store, _ := checkpoint.NewFileStore(tempPath(t))

	cases := []struct {
		part   int
		offset int64
	}{
		{0, 1}, {0, 50}, {1, 1000}, {2, 0}, {2, 999},
	}
	for _, c := range cases {
		_ = store.Save(c.part, c.offset)
		cp := store.Current()
		if cp.PartIndex != c.part || cp.Offset != c.offset {
			t.Errorf("after Save(%d, %d): Current() = %+v", c.part, c.offset, cp)
		}
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

func TestSave_ConcurrentSafety(t *testing.T) {
	store, _ := checkpoint.NewFileStore(tempPath(t))

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			_ = store.Save(i%3, int64(i))
			_ = store.Current()
		}()
	}
	wg.Wait()

	cp := store.Current()
	if cp.Offset < 0 || cp.Offset >= goroutines {
		t.Errorf("Current().Offset = %d is outside expected range [0, %d)", cp.Offset, goroutines)
	}
}

func TestSave_ConcurrentCurrentConsistency(t *testing.T) {
	store, _ := checkpoint.NewFileStore(tempPath(t))

	type entry struct {
		part   int
		offset int64
	}
	valid := map[int64]bool{0: true}
	var mu sync.Mutex

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 1; i <= goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			mu.Lock()
			valid[int64(i)] = true
			mu.Unlock()
			_ = store.Save(0, int64(i))
		}()
	}
	wg.Wait()

	got := store.Current()
	mu.Lock()
	ok := valid[got.Offset]
	mu.Unlock()

	if !ok {
		t.Errorf("Current().Offset returned %d, which was never saved", got.Offset)
	}
}
