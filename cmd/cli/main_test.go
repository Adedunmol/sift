package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Adedunmol/sift/core/checkpoint"
	"github.com/Adedunmol/sift/core/parser"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

type fakeStreamer struct {
	tweets  []*parser.Tweet
	pos     int
	offsets []int64
	err     error
}

func (f *fakeStreamer) Next(_ context.Context) (*parser.Tweet, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.pos >= len(f.tweets) {
		return nil, io.EOF
	}
	tw := f.tweets[f.pos]
	f.pos++
	return tw, nil
}

func (f *fakeStreamer) Offset() int64 {
	if f.pos == 0 || len(f.offsets) == 0 {
		return 0
	}
	idx := f.pos - 1
	if idx >= len(f.offsets) {
		idx = len(f.offsets) - 1
	}
	return f.offsets[idx]
}

type fakeProcessor struct {
	flagged []*parser.Tweet
	err     error
	calls   int
}

func (f *fakeProcessor) Process(_ context.Context, _ []*parser.Tweet) ([]*parser.Tweet, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.flagged, nil
}

type saveCall struct {
	partIndex int
	offset    int64
}

type fakeStore struct {
	saves []saveCall
	cp    checkpoint.Checkpoint
	err   error
}

func (f *fakeStore) Save(partIndex int, offset int64) error {
	if f.err != nil {
		return f.err
	}
	f.saves = append(f.saves, saveCall{partIndex, offset})
	f.cp = checkpoint.Checkpoint{PartIndex: partIndex, Offset: offset}
	return nil
}

func (f *fakeStore) Current() checkpoint.Checkpoint { return f.cp }

type fakeWriter struct {
	written  [][]*parser.Tweet
	flushes  int
	writeErr error
	flushErr error
}

func (f *fakeWriter) Write(tweets []*parser.Tweet) error {
	if f.writeErr != nil {
		return f.writeErr
	}
	cp := make([]*parser.Tweet, len(tweets))
	copy(cp, tweets)
	f.written = append(f.written, cp)
	return nil
}

func (f *fakeWriter) Flush() error {
	if f.flushErr != nil {
		return f.flushErr
	}
	f.flushes++
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func makeTweets(n int) []*parser.Tweet {
	tweets := make([]*parser.Tweet, n)
	for i := range tweets {
		tweets[i] = &parser.Tweet{ID: int64(i + 1), Text: fmt.Sprintf("tweet %d", i+1)}
	}
	return tweets
}

func makeOffsets(n int) []int64 {
	offsets := make([]int64, n)
	for i := range offsets {
		offsets[i] = int64((i + 1) * 100)
	}
	return offsets
}

func newTestApp(stream streamer, partIndex int, proc *fakeProcessor, store *fakeStore, writer *fakeWriter) *app {
	return &app{
		stream:    stream,
		partIndex: partIndex,
		processor: proc,
		store:     store,
		writer:    writer,
		cleanup:   func() {},
	}
}

// buildArchive writes a well-formed part0 archive to dir and returns its path.
func buildArchive(t *testing.T, dir string, tweets []struct{ id, text string }) string {
	t.Helper()
	parts := make([]string, len(tweets))
	for i, tw := range tweets {
		parts[i] = fmt.Sprintf(`{"tweet":{"id":"%s","full_text":"%s"}}`, tw.id, tw.text)
	}
	content := "window.YTD.tweets.part0 = [" + strings.Join(parts, ",") + "]"
	path := filepath.Join(dir, "tweets.js")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("buildArchive: %v", err)
	}
	return path
}

// ---------------------------------------------------------------------------
// CLI — argument parsing and exit codes
// ---------------------------------------------------------------------------

func TestCLI_MissingArchiveFileReturnsOne(t *testing.T) {
	dir := t.TempDir()
	args := []string{
		"-a", filepath.Join(dir, "nonexistent.js"),
		"-cp", filepath.Join(dir, "cp.json"),
		"-o", filepath.Join(dir, "out.csv"),
	}
	if got := CLI(args); got != 1 {
		t.Errorf("CLI() = %d, want 1", got)
	}
}

func TestCLI_InvalidFlagReturnsTwo(t *testing.T) {
	if got := CLI([]string{"--unknown-flag"}); got != 2 {
		t.Errorf("CLI() = %d, want 2", got)
	}
}

func TestCLI_ValidEmptyArchiveReturnsZero(t *testing.T) {
	dir := t.TempDir()
	args := []string{
		"-a", buildArchive(t, dir, nil),
		"-cp", filepath.Join(dir, "cp.json"),
		"-o", filepath.Join(dir, "out.csv"),
	}
	if got := CLI(args); got != 0 {
		t.Errorf("CLI() = %d, want 0", got)
	}
}

func TestCLI_CorruptCheckpointReturnsOne(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp.json")
	if err := os.WriteFile(cpPath, []byte("{corrupt"), 0644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	args := []string{
		"-a", filepath.Join(dir, "tweets.js"),
		"-cp", cpPath,
		"-o", filepath.Join(dir, "out.csv"),
	}
	if got := CLI(args); got != 1 {
		t.Errorf("CLI() = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// run — processing loop behaviour
// ---------------------------------------------------------------------------

func TestRun_EmptyStreamSavesNoCheckpoints(t *testing.T) {
	store := &fakeStore{}
	a := newTestApp(&fakeStreamer{}, 0, &fakeProcessor{}, store, &fakeWriter{})

	if err := a.run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.saves) != 0 {
		t.Errorf("expected 0 saves for empty stream, got %d", len(store.saves))
	}
}

func TestRun_SinglePartialBatchSavesOnce(t *testing.T) {
	store := &fakeStore{}
	a := newTestApp(
		&fakeStreamer{tweets: makeTweets(3), offsets: makeOffsets(3)},
		0, &fakeProcessor{}, store, &fakeWriter{},
	)

	if err := a.run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 3 tweets < batchSize(100) — flushed as one partial batch at EOF.
	if len(store.saves) != 1 {
		t.Errorf("expected 1 save, got %d", len(store.saves))
	}
}

func TestRun_TwoBatchesSavesTwice(t *testing.T) {
	// 150 tweets = one full batch of 100 + one partial batch of 50.
	store := &fakeStore{}
	a := newTestApp(
		&fakeStreamer{tweets: makeTweets(150), offsets: makeOffsets(150)},
		0, &fakeProcessor{}, store, &fakeWriter{},
	)

	if err := a.run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.saves) != 2 {
		t.Errorf("expected 2 saves (full + partial batch), got %d", len(store.saves))
	}
}

func TestRun_ExactlyBatchSizeSavesOnce(t *testing.T) {
	// 100 tweets hits the >= batchSize boundary exactly; batch resets to empty,
	// then EOF leaves len(batch)==0 so the trailing flush guard does not fire.
	store := &fakeStore{}
	a := newTestApp(
		&fakeStreamer{tweets: makeTweets(100), offsets: makeOffsets(100)},
		0, &fakeProcessor{}, store, &fakeWriter{},
	)

	if err := a.run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.saves) != 1 {
		t.Errorf("expected 1 save for exactly batchSize tweets, got %d", len(store.saves))
	}
}

func TestRun_StreamErrorPropagates(t *testing.T) {
	streamErr := errors.New("read failure")
	a := newTestApp(&fakeStreamer{err: streamErr}, 0, &fakeProcessor{}, &fakeStore{}, &fakeWriter{})

	if err := a.run(context.Background()); !errors.Is(err, streamErr) {
		t.Errorf("expected stream error in chain, got %v", err)
	}
}

func TestRun_CancelledContextStopsLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	a := newTestApp(&fakeStreamer{err: ctx.Err()}, 0, &fakeProcessor{}, &fakeStore{}, &fakeWriter{})

	if err := a.run(ctx); err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestRun_ProcessorErrorPropagates(t *testing.T) {
	procErr := errors.New("evaluator failure")
	a := newTestApp(
		&fakeStreamer{tweets: makeTweets(1), offsets: makeOffsets(1)},
		0, &fakeProcessor{err: procErr}, &fakeStore{}, &fakeWriter{},
	)

	if err := a.run(context.Background()); !errors.Is(err, procErr) {
		t.Errorf("expected processor error in chain, got %v", err)
	}
}

func TestRun_CheckpointSavedWithCorrectPartIndex(t *testing.T) {
	store := &fakeStore{}
	const partIndex = 2
	a := newTestApp(
		&fakeStreamer{tweets: makeTweets(1), offsets: []int64{42}},
		partIndex, &fakeProcessor{}, store, &fakeWriter{},
	)

	if err := a.run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.saves) == 0 {
		t.Fatal("expected at least one save")
	}
	if got := store.saves[0].partIndex; got != partIndex {
		t.Errorf("saved partIndex = %d, want %d", got, partIndex)
	}
}

func TestRun_CheckpointOffsetMatchesStreamOffset(t *testing.T) {
	store := &fakeStore{}
	a := newTestApp(
		&fakeStreamer{tweets: makeTweets(3), offsets: []int64{111, 222, 333}},
		0, &fakeProcessor{}, store, &fakeWriter{},
	)

	if err := a.run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Single partial batch — saved once with the offset after the last tweet.
	if got := store.saves[0].offset; got != 333 {
		t.Errorf("saved offset = %d, want 333", got)
	}
}

// ---------------------------------------------------------------------------
// processBatch — write/flush/save interactions
// ---------------------------------------------------------------------------

func TestProcessBatch_NoFlaggedTweetsSkipsWriteAndFlush(t *testing.T) {
	writer := &fakeWriter{}
	store := &fakeStore{}
	a := newTestApp(&fakeStreamer{offsets: []int64{10}}, 0, &fakeProcessor{}, store, writer)

	if err := a.processBatch(context.Background(), makeTweets(2)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(writer.written) != 0 {
		t.Errorf("expected no writes for unflagged batch, got %d", len(writer.written))
	}
	if writer.flushes != 0 {
		t.Errorf("expected no flushes for unflagged batch, got %d", writer.flushes)
	}
	// Checkpoint must still be saved even when nothing is flagged.
	if len(store.saves) != 1 {
		t.Errorf("expected 1 checkpoint save, got %d", len(store.saves))
	}
}

func TestProcessBatch_FlaggedTweetsAreWrittenAndFlushed(t *testing.T) {
	flagged := makeTweets(2)
	writer := &fakeWriter{}
	a := newTestApp(
		&fakeStreamer{offsets: []int64{50}},
		0, &fakeProcessor{flagged: flagged}, &fakeStore{}, writer,
	)

	if err := a.processBatch(context.Background(), makeTweets(5)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(writer.written) != 1 {
		t.Fatalf("expected 1 Write call, got %d", len(writer.written))
	}
	if len(writer.written[0]) != 2 {
		t.Errorf("expected 2 flagged tweets written, got %d", len(writer.written[0]))
	}
	if writer.flushes != 1 {
		t.Errorf("expected 1 Flush call, got %d", writer.flushes)
	}
}

func TestProcessBatch_WriteErrorPropagates(t *testing.T) {
	writeErr := errors.New("disk full")
	a := newTestApp(
		&fakeStreamer{offsets: []int64{1}},
		0, &fakeProcessor{flagged: makeTweets(1)}, &fakeStore{},
		&fakeWriter{writeErr: writeErr},
	)

	if err := a.processBatch(context.Background(), makeTweets(1)); !errors.Is(err, writeErr) {
		t.Errorf("expected write error in chain, got %v", err)
	}
}

func TestProcessBatch_FlushErrorPropagates(t *testing.T) {
	flushErr := errors.New("flush failure")
	a := newTestApp(
		&fakeStreamer{offsets: []int64{1}},
		0, &fakeProcessor{flagged: makeTweets(1)}, &fakeStore{},
		&fakeWriter{flushErr: flushErr},
	)

	if err := a.processBatch(context.Background(), makeTweets(1)); !errors.Is(err, flushErr) {
		t.Errorf("expected flush error in chain, got %v", err)
	}
}

func TestProcessBatch_SaveErrorPropagates(t *testing.T) {
	saveErr := errors.New("store unavailable")
	a := newTestApp(
		&fakeStreamer{offsets: []int64{1}},
		0, &fakeProcessor{}, &fakeStore{err: saveErr}, &fakeWriter{},
	)

	if err := a.processBatch(context.Background(), makeTweets(1)); !errors.Is(err, saveErr) {
		t.Errorf("expected save error in chain, got %v", err)
	}
}

func TestProcessBatch_SaveNotCalledAfterWriteError(t *testing.T) {
	store := &fakeStore{}
	a := newTestApp(
		&fakeStreamer{offsets: []int64{1}},
		0, &fakeProcessor{flagged: makeTweets(1)}, store,
		&fakeWriter{writeErr: errors.New("write failed")},
	)

	a.processBatch(context.Background(), makeTweets(1))
	if len(store.saves) != 0 {
		t.Errorf("expected no checkpoint save after write failure, got %d", len(store.saves))
	}
}

// ---------------------------------------------------------------------------
// CLI end-to-end — real files, real parser
// ---------------------------------------------------------------------------

func TestCLI_CheckpointFileCreatedAfterRun(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp.json")
	args := []string{
		"-a", buildArchive(t, dir, []struct{ id, text string }{{"1", "hello"}}),
		"-cp", cpPath,
		"-o", filepath.Join(dir, "out.csv"),
	}
	if got := CLI(args); got != 0 {
		t.Fatalf("CLI() = %d, want 0", got)
	}
	if _, err := os.Stat(cpPath); os.IsNotExist(err) {
		t.Error("expected checkpoint file to exist after successful run")
	}
}

func TestCLI_ResumeFromCheckpointExitsCleanly(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp.json")
	args := []string{
		"-a", buildArchive(t, dir, []struct{ id, text string }{
			{"1", "first tweet"}, {"2", "second tweet"},
		}),
		"-cp", cpPath,
		"-o", filepath.Join(dir, "out.csv"),
	}

	// First run — processes all tweets and writes checkpoint at EOF offset.
	if got := CLI(args); got != 0 {
		t.Fatalf("first CLI() = %d, want 0", got)
	}
	// Second run — checkpoint is at EOF so the stream yields nothing immediately.
	if got := CLI(args); got != 0 {
		t.Fatalf("second CLI() = %d, want 0", got)
	}
}
