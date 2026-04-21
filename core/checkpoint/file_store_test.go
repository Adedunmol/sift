package checkpoint_test

import (
	"encoding/json"
	"github.com/Adedunmol/sift/core/checkpoint"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// tempPath returns a path inside t.TempDir() that does NOT exist yet.
func tempPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "checkpoint.json")
}

// writeCheckpoint writes a Checkpoint as JSON to path.
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

// readCheckpoint reads and unmarshals the checkpoint at path.
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

// TestNewFileStore_NoExistingFile verifies that a missing checkpoint file is
// treated as offset 0 rather than an error.
func TestNewFileStore_NoExistingFile(t *testing.T) {
	path := tempPath(t)

	store, err := checkpoint.NewFileStore(path)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if store.Offset() != 0 {
		t.Errorf("expected offset 0 for new store, got %d", store.Offset())
	}
}

// TestNewFileStore_LoadsExistingCheckpoint verifies that an existing checkpoint
// file is read and its offset is restored.
func TestNewFileStore_LoadsExistingCheckpoint(t *testing.T) {
	path := tempPath(t)
	writeCheckpoint(t, path, checkpoint.Checkpoint{Offset: 42})

	store, err := checkpoint.NewFileStore(path)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if store.Offset() != 42 {
		t.Errorf("expected offset 42, got %d", store.Offset())
	}
}

// TestNewFileStore_LoadsZeroOffset verifies that a checkpoint persisted with
// offset 0 is not mistaken for a missing file.
func TestNewFileStore_LoadsZeroOffset(t *testing.T) {
	path := tempPath(t)
	writeCheckpoint(t, path, checkpoint.Checkpoint{Offset: 0})

	store, err := checkpoint.NewFileStore(path)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if store.Offset() != 0 {
		t.Errorf("expected offset 0, got %d", store.Offset())
	}
}

// TestNewFileStore_InvalidJSON verifies that a checkpoint file with corrupt
// JSON returns an error instead of silently producing offset 0.
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

// TestNewFileStore_EmptyFile verifies that an empty file (zero bytes) returns
// an error rather than producing a zero-value checkpoint.
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

// TestSave_CreatesFileOnFirstSave verifies that Save creates the checkpoint
// file when it did not previously exist.
func TestSave_CreatesFileOnFirstSave(t *testing.T) {
	path := tempPath(t)
	store, err := checkpoint.NewFileStore(path)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := store.Save(10); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("expected checkpoint file to exist after Save")
	}
}

// TestSave_PersistsOffset verifies that the offset written to disk matches
// what was passed to Save.
func TestSave_PersistsOffset(t *testing.T) {
	path := tempPath(t)
	store, err := checkpoint.NewFileStore(path)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := store.Save(99); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cp := readCheckpoint(t, path)
	if cp.Offset != 99 {
		t.Errorf("expected persisted offset 99, got %d", cp.Offset)
	}
}

// TestSave_UpdatesInMemoryOffset verifies that Offset() reflects the new value
// immediately after Save returns.
func TestSave_UpdatesInMemoryOffset(t *testing.T) {
	path := tempPath(t)
	store, err := checkpoint.NewFileStore(path)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := store.Save(55); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if store.Offset() != 55 {
		t.Errorf("expected in-memory offset 55, got %d", store.Offset())
	}
}

// TestSave_OverwritesPreviousOffset verifies that a second Save replaces the
// first persisted offset.
func TestSave_OverwritesPreviousOffset(t *testing.T) {
	path := tempPath(t)
	store, _ := checkpoint.NewFileStore(path)

	_ = store.Save(10)
	if err := store.Save(200); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cp := readCheckpoint(t, path)
	if cp.Offset != 200 {
		t.Errorf("expected offset 200 after overwrite, got %d", cp.Offset)
	}
}

// TestSave_SetsUpdatedAt verifies that UpdatedAt is populated and reasonably
// close to the current time.
func TestSave_SetsUpdatedAt(t *testing.T) {
	path := tempPath(t)
	before := time.Now().UTC().Add(-time.Second)

	store, _ := checkpoint.NewFileStore(path)
	if err := store.Save(1); err != nil {
		t.Fatalf("Save: %v", err)
	}

	after := time.Now().UTC().Add(time.Second)
	cp := readCheckpoint(t, path)

	if cp.UpdatedAt.Before(before) || cp.UpdatedAt.After(after) {
		t.Errorf("UpdatedAt %v is outside expected window [%v, %v]", cp.UpdatedAt, before, after)
	}
}

// TestSave_ZeroOffset verifies that offset 0 can be explicitly saved and is
// not confused with an uninitialised state.
func TestSave_ZeroOffset(t *testing.T) {
	path := tempPath(t)
	store, _ := checkpoint.NewFileStore(path)

	_ = store.Save(100)
	if err := store.Save(0); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cp := readCheckpoint(t, path)
	if cp.Offset != 0 {
		t.Errorf("expected persisted offset 0, got %d", cp.Offset)
	}
}

// TestSave_LargeOffset verifies that very large int64 offsets are handled
// correctly without truncation.
func TestSave_LargeOffset(t *testing.T) {
	path := tempPath(t)
	store, _ := checkpoint.NewFileStore(path)

	const big int64 = 1<<62 - 1
	if err := store.Save(big); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cp := readCheckpoint(t, path)
	if cp.Offset != big {
		t.Errorf("expected offset %d, got %d", big, cp.Offset)
	}
}

// TestSave_NoTmpFileLeft verifies that the .tmp staging file is cleaned up
// after a successful Save (atomic rename moves it).
func TestSave_NoTmpFileLeft(t *testing.T) {
	path := tempPath(t)
	store, _ := checkpoint.NewFileStore(path)

	if err := store.Save(7); err != nil {
		t.Fatalf("Save: %v", err)
	}

	tmpFile := path + ".tmp"
	if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
		t.Errorf("expected .tmp file to be gone after Save, but it still exists")
	}
}

// TestSave_ReloadAfterSave verifies end-to-end persistence: a new FileStore
// opened on the same path after Save returns the correct offset.
func TestSave_ReloadAfterSave(t *testing.T) {
	path := tempPath(t)

	store1, _ := checkpoint.NewFileStore(path)
	_ = store1.Save(333)

	store2, err := checkpoint.NewFileStore(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if store2.Offset() != 333 {
		t.Errorf("expected reloaded offset 333, got %d", store2.Offset())
	}
}

// ---------------------------------------------------------------------------
// Offset
// ---------------------------------------------------------------------------

// TestOffset_DefaultIsZero verifies that a freshly created store with no
// pre-existing file returns 0.
func TestOffset_DefaultIsZero(t *testing.T) {
	store, _ := checkpoint.NewFileStore(tempPath(t))
	if store.Offset() != 0 {
		t.Errorf("expected 0, got %d", store.Offset())
	}
}

// TestOffset_ReflectsLastSave verifies that Offset always reflects the most
// recent Save call.
func TestOffset_ReflectsLastSave(t *testing.T) {
	store, _ := checkpoint.NewFileStore(tempPath(t))

	for _, offset := range []int64{1, 50, 1000, 0, 999} {
		_ = store.Save(offset)
		if got := store.Offset(); got != offset {
			t.Errorf("after Save(%d): Offset() = %d", offset, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

// TestSave_ConcurrentSafety verifies that concurrent calls to Save and Offset
// do not cause data races. Run with -race.
func TestSave_ConcurrentSafety(t *testing.T) {
	store, _ := checkpoint.NewFileStore(tempPath(t))

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			_ = store.Save(int64(i))
			_ = store.Offset()
		}()
	}

	wg.Wait()

	// After all writes, Offset must equal one of the values written (0‥19).
	got := store.Offset()
	if got < 0 || got >= goroutines {
		t.Errorf("Offset() = %d is outside the expected range [0, %d)", got, goroutines)
	}
}

// TestSave_ConcurrentOffsetConsistency verifies that Offset never returns a
// value that was never passed to Save (i.e. no torn reads).
func TestSave_ConcurrentOffsetConsistency(t *testing.T) {
	store, _ := checkpoint.NewFileStore(tempPath(t))

	valid := map[int64]bool{0: true}
	var mu sync.Mutex

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 1; i <= goroutines; i++ {
		i := int64(i)
		go func() {
			defer wg.Done()
			mu.Lock()
			valid[i] = true
			mu.Unlock()
			_ = store.Save(i)
		}()
	}

	wg.Wait()

	got := store.Offset()
	mu.Lock()
	ok := valid[got]
	mu.Unlock()

	if !ok {
		t.Errorf("Offset() returned %d, which was never saved", got)
	}
}
