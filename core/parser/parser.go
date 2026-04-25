// Package parser reads a Twitter archive JS file as a stream of Tweets.
//
// Twitter archives ship as JS files with the format:
//
//	window.YTD.tweets.part0 = [ ... ]
//	window.YTD.tweets.part1 = [ ... ]
//
// NewStream strips the JS assignment prefix for the given part index and
// positions the decoder at the opening '['. Subsequent Next() calls decode
// one tweet at a time, keeping memory usage constant regardless of archive size.
package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// Stream decodes tweets from a single archive part file one at a time.
type Stream struct {
	dec      *json.Decoder
	offset   int64
	username string
}

// NewStream creates a Stream from r for the given partIndex.
//
// If startOffset is 0, NewStream strips the JS assignment prefix and
// positions the decoder at the opening '[' of the tweet array.
//
// If startOffset > 0, NewStream seeks directly to that byte position.
// The caller must ensure startOffset was previously returned by Offset()
// — it must land on a JSON object boundary, never mid-token.
//
// username is embedded into each decoded Tweet for URL construction.
func NewStream(r io.ReadSeeker, partIndex int, startOffset int64, username string) (*Stream, error) {
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

	prefix := buildPrefix(partIndex)

	buf := make([]byte, len(prefix)+1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read prefix: %w", err)
	}

	buf = bytes.TrimPrefix(buf, prefix)
	if len(buf) == 0 || buf[0] != '[' {
		return nil, fmt.Errorf("part%d: expected '[' after JS prefix, got %q", partIndex, buf)
	}

	return &Stream{
		dec:      json.NewDecoder(io.MultiReader(bytes.NewReader(buf), r)),
		offset:   0,
		username: username,
	}, nil
}

// buildPrefix constructs the JS assignment prefix for a given part index,
// e.g. []byte("window.YTD.tweets.part2 = ") for partIndex 2.
func buildPrefix(partIndex int) []byte {
	return []byte(fmt.Sprintf("window.YTD.tweets.part%d = ", partIndex))
}

// Next decodes and returns the next tweet in the archive part.
//
// Returns io.EOF when the array is exhausted — callers should treat
// this as clean termination and advance to the next part file.
//
// ctx is checked before each decode so a long-running parse in a
// worker can be cancelled without waiting for the next I/O boundary.
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

// Offset returns the byte offset immediately after the last successfully
// decoded tweet. Persist this alongside the part index to enable resumption.
func (s *Stream) Offset() int64 {
	return s.offset
}
