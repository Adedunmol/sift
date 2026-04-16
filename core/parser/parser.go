// Package parser reads a Twitter archive JS file as a stream of Tweets.
//
// Twitter archives ship as a JS file with the format:
//
//	window.YTD.tweets.part0 = [ ... ]
//
// NewStream strips the JS assignment prefix and positions the decoder at
// the opening '['. Subsequent Next() calls decode one tweet at a time,
// keeping memory usage constant regardless of archive size.
package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

var jsPrefix = []byte("window.YTD.tweets.part0 = ")

// Stream decodes tweets from a Twitter archive one at a time.
// It tracks the byte offset after each decoded tweet so the caller
// can persist a checkpoint and resume mid-archive after a crash.
type Stream struct {
	dec      *json.Decoder
	offset   int64
	username string
}

// NewStream creates a Stream from r.
//
// If startOffset is 0, NewStream strips the JS assignment prefix and
// positions the decoder at the opening '[' of the tweet array.
//
// If startOffset > 0, NewStream seeks directly to that byte position.
// The caller is responsible for ensuring startOffset is a value
// previously returned by Offset() — it must point to the start of a
// JSON object boundary, never to a mid-token position.
//
// username is embedded into each decoded Tweet so URL() produces
// canonical links. Pass an empty string if unavailable.
func NewStream(r io.ReadSeeker, startOffset int64, username string) (*Stream, error) {
	if startOffset > 0 {
		if _, err := r.Seek(startOffset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek to offset %d: %w", startOffset, err)
		}
		return &Stream{
			dec:      json.NewDecoder(r),
			offset:   startOffset,
			username: username,
		}, nil
	}

	// Strip the JS assignment prefix so the decoder sees raw JSON.
	buf := make([]byte, len(jsPrefix)+1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read prefix: %w", err)
	}

	buf = bytes.TrimPrefix(buf, jsPrefix)
	if len(buf) == 0 || buf[0] != '[' {
		return nil, fmt.Errorf("expected '[' after JS prefix, got %q", buf)
	}

	return &Stream{
		dec:      json.NewDecoder(io.MultiReader(bytes.NewReader(buf), r)),
		offset:   0,
		username: username,
	}, nil
}

// Next decodes and returns the next tweet in the archive.
//
// Returns io.EOF when the array is exhausted — callers should treat
// this as clean termination, not an error.
//
// ctx is checked before each decode so a long-running parse in a
// worker can be cancelled cleanly without waiting for the next I/O boundary.
func (s *Stream) Next(ctx context.Context) (*Tweet, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if !s.dec.More() {
		return nil, io.EOF
	}

	var raw rawTweet
	if err := s.dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode tweet at offset %d: %w", s.offset, err)
	}

	s.offset = s.dec.InputOffset()

	return mapTweet(&raw, s.username)
}

// Offset returns the byte offset in the source reader immediately after
// the last successfully decoded tweet. Persist this value to a checkpoint
// Store to enable resumption after a crash.
func (s *Stream) Offset() int64 {
	return s.offset
}
