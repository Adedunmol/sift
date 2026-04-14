package parser

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

var jsPrefix = []byte("window.YTD.tweets.part0 = ")

type Stream struct {
	dec    *json.Decoder
	offset int64
}

func NewStream(r io.ReadSeeker, startOffset int64) (*Stream, error) {
	if startOffset > 0 {
		if _, err := r.Seek(startOffset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek to offset %d: %w", startOffset, err)
		}
		return &Stream{
			dec:    json.NewDecoder(r),
			offset: startOffset,
		}, nil
	}

	buf := make([]byte, len(jsPrefix)+1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("reading prefix: %w", err)
	}
	buf = bytes.TrimPrefix(buf, jsPrefix)
	if len(buf) == 0 || buf[0] != '[' {
		return nil, fmt.Errorf("expected '[' after JS prefix, got %q", buf)
	}

	return &Stream{
		dec:    json.NewDecoder(io.MultiReader(bytes.NewReader(buf), r)),
		offset: 0,
	}, nil
}

func (s *Stream) Next() (*Tweet, error) {
	if !s.dec.More() {
		return nil, io.EOF
	}
	var raw rawTweet
	if err := s.dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decoding tweet: %w", err)
	}
	s.offset = s.dec.InputOffset()
	return mapTweet(&raw)
}

func (s *Stream) Offset() int64 {
	return s.offset
}
