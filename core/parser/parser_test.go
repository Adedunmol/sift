package parser_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/Adedunmol/sift/core/parser"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// archiveReader wraps a raw tweet JSON array in the JS assignment prefix
// that a real Twitter archive file uses.
func archiveReader(jsonArray string) io.ReadSeeker {
	src := "window.YTD.tweets.part0 = " + jsonArray
	return bytes.NewReader([]byte(src))
}

// rawTweetJSON builds the nested JSON object that appears inside the archive
// array for a single tweet.
func rawTweetJSON(id, text string) string {
	return `{"tweet":{"id":"` + id + `","full_text":"` + text + `"}}`
}

// singleTweetArchive returns a ReadSeeker for an archive with one tweet.
func singleTweetArchive(id, text string) io.ReadSeeker {
	return archiveReader("[" + rawTweetJSON(id, text) + "]")
}

// multiTweetArchive returns a ReadSeeker for an archive with several tweets.
func multiTweetArchive(tweets [][2]string) io.ReadSeeker {
	parts := make([]string, len(tweets))
	for i, tw := range tweets {
		parts[i] = rawTweetJSON(tw[0], tw[1])
	}
	return archiveReader("[" + strings.Join(parts, ",") + "]")
}

// drainStream reads all tweets from s until io.EOF or an error.
func drainStream(t *testing.T, s *parser.Stream) []*parser.Tweet {
	t.Helper()
	var out []*parser.Tweet
	for {
		tw, err := s.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("drainStream: unexpected error: %v", err)
		}
		out = append(out, tw)
	}
	return out
}

// ---------------------------------------------------------------------------
// NewStream — startOffset == 0 (prefix-stripping path)
// ---------------------------------------------------------------------------

func TestNewStream_ValidPrefixSucceeds(t *testing.T) {
	r := singleTweetArchive("1", "hello")
	_, err := parser.NewStream(r, 0, "user")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewStream_MissingPrefixReturnsError(t *testing.T) {
	// No "window.YTD..." prefix — raw JSON array instead.
	r := bytes.NewReader([]byte(`[{"tweet":{"id":"1","full_text":"hi"}}]`))
	_, err := parser.NewStream(r, 0, "user")
	if err == nil {
		t.Fatal("expected error for missing JS prefix, got nil")
	}
}

func TestNewStream_PrefixPresentButNoOpenBracketReturnsError(t *testing.T) {
	// Prefix is correct but the array bracket is replaced by something else.
	r := bytes.NewReader([]byte("window.YTD.tweets.part0 = {\"tweet\":{}}"))
	_, err := parser.NewStream(r, 0, "user")
	if err == nil {
		t.Fatal("expected error when '[' is absent after prefix, got nil")
	}
}

func TestNewStream_EmptyReaderReturnsError(t *testing.T) {
	r := bytes.NewReader([]byte{})
	_, err := parser.NewStream(r, 0, "user")
	if err == nil {
		t.Fatal("expected error for empty reader, got nil")
	}
}

func TestNewStream_TruncatedPrefixReturnsError(t *testing.T) {
	// Only part of the expected prefix is present.
	r := bytes.NewReader([]byte("window.YTD.tweets"))
	_, err := parser.NewStream(r, 0, "user")
	if err == nil {
		t.Fatal("expected error for truncated prefix, got nil")
	}
}

func TestNewStream_EmptyArraySucceeds(t *testing.T) {
	r := archiveReader("[]")
	s, err := parser.NewStream(r, 0, "user")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// First Next should immediately return io.EOF.
	_, err = s.Next(context.Background())
	if err != io.EOF {
		t.Errorf("expected io.EOF for empty array, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// NewStream — startOffset > 0 (seek path)
// ---------------------------------------------------------------------------

func TestNewStream_StartOffsetSeeksCorrectly(t *testing.T) {
	// Build a two-tweet archive, read the first tweet to find its end offset,
	// then open a second stream at that offset to read only the second tweet.
	r := multiTweetArchive([][2]string{{"1", "first"}, {"2", "second"}})

	s1, err := parser.NewStream(r, 0, "user")
	if err != nil {
		t.Fatalf("s1: %v", err)
	}
	_, err = s1.Next(context.Background())
	if err != nil {
		t.Fatalf("s1.Next: %v", err)
	}
	checkpoint := s1.Offset()

	// Seek the same reader to the checkpoint and open a new stream.
	s2, err := parser.NewStream(r, checkpoint, "user")
	if err != nil {
		t.Fatalf("s2: %v", err)
	}

	tw, err := s2.Next(context.Background())
	if err != nil {
		t.Fatalf("s2.Next: %v", err)
	}
	if tw.ID != 2 {
		t.Errorf("expected tweet ID 2 after seek, got %d", tw.ID)
	}
}

func TestNewStream_InvalidSeekOffsetReturnsError(t *testing.T) {
	// errSeeker always returns an error from Seek.
	r := &errSeeker{Reader: bytes.NewReader([]byte("data"))}
	_, err := parser.NewStream(r, 99, "user")
	if err == nil {
		t.Fatal("expected error for failed seek, got nil")
	}
}

// errSeeker is an io.ReadSeeker whose Seek always fails.
type errSeeker struct {
	*bytes.Reader
}

func (e *errSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, io.ErrUnexpectedEOF
}

// ---------------------------------------------------------------------------
// Next — decoding
// ---------------------------------------------------------------------------

func TestNext_DecodesIDAndText(t *testing.T) {
	r := singleTweetArchive("123456789", "hello world")
	s, _ := parser.NewStream(r, 0, "")

	tw, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tw.ID != 123456789 {
		t.Errorf("ID = %d, want 123456789", tw.ID)
	}
	if tw.Text != "hello world" {
		t.Errorf("Text = %q, want %q", tw.Text, "hello world")
	}
}

func TestNext_PopulatesUsername(t *testing.T) {
	r := singleTweetArchive("1", "text")
	s, _ := parser.NewStream(r, 0, "alice")

	tw, _ := s.Next(context.Background())
	if tw.Username != "alice" {
		t.Errorf("Username = %q, want %q", tw.Username, "alice")
	}
}

func TestNext_EmptyUsernamePreserved(t *testing.T) {
	r := singleTweetArchive("1", "text")
	s, _ := parser.NewStream(r, 0, "")

	tw, _ := s.Next(context.Background())
	if tw.Username != "" {
		t.Errorf("Username = %q, want empty string", tw.Username)
	}
}

func TestNext_ReturnsEOFAfterLastTweet(t *testing.T) {
	r := singleTweetArchive("1", "only tweet")
	s, _ := parser.NewStream(r, 0, "")

	s.Next(context.Background()) // consume the one tweet

	_, err := s.Next(context.Background())
	if err != io.EOF {
		t.Errorf("expected io.EOF after last tweet, got %v", err)
	}
}

func TestNext_ReturnsEOFConsistentlyAfterExhaustion(t *testing.T) {
	r := singleTweetArchive("1", "tweet")
	s, _ := parser.NewStream(r, 0, "")

	s.Next(context.Background())

	for i := 0; i < 3; i++ {
		_, err := s.Next(context.Background())
		if err != io.EOF {
			t.Errorf("call %d: expected io.EOF, got %v", i+1, err)
		}
	}
}

func TestNext_DecodesMultipleTweetsInOrder(t *testing.T) {
	r := multiTweetArchive([][2]string{
		{"10", "first"},
		{"20", "second"},
		{"30", "third"},
	})
	s, _ := parser.NewStream(r, 0, "")

	tweets := drainStream(t, s)

	if len(tweets) != 3 {
		t.Fatalf("expected 3 tweets, got %d", len(tweets))
	}
	wantIDs := []int64{10, 20, 30}
	for i, tw := range tweets {
		if tw.ID != wantIDs[i] {
			t.Errorf("tweet[%d].ID = %d, want %d", i, tw.ID, wantIDs[i])
		}
	}
}

func TestNext_DecodesTweetWithSpecialCharactersInText(t *testing.T) {
	// JSON-escape characters that commonly appear in tweets.
	r := archiveReader(`[{"tweet":{"id":"1","full_text":"hello\nworld\t\u0026 \"quoted\""}}]`)
	s, _ := parser.NewStream(r, 0, "")

	tw, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "hello\nworld\t& \"quoted\""
	if tw.Text != want {
		t.Errorf("Text = %q, want %q", tw.Text, want)
	}
}

func TestNext_NonNumericIDReturnsError(t *testing.T) {
	r := archiveReader(`[{"tweet":{"id":"not-a-number","full_text":"text"}}]`)
	s, _ := parser.NewStream(r, 0, "")

	_, err := s.Next(context.Background())
	if err == nil {
		t.Fatal("expected error for non-numeric ID, got nil")
	}
}

func TestNext_EmptyIDReturnsError(t *testing.T) {
	r := archiveReader(`[{"tweet":{"id":"","full_text":"text"}}]`)
	s, _ := parser.NewStream(r, 0, "")

	_, err := s.Next(context.Background())
	if err == nil {
		t.Fatal("expected error for empty ID, got nil")
	}
}

func TestNext_MalformedJSONReturnsError(t *testing.T) {
	r := archiveReader(`[{"tweet":{"id":1,"full_text":"text"}`) // unclosed array
	s, _ := parser.NewStream(r, 0, "")

	s.Next(context.Background()) // first tweet decodes fine (decoder is lenient here)
	_, err := s.Next(context.Background())
	// Either an error or EOF is acceptable depending on decoder behaviour,
	// but it must not return a non-nil tweet.
	// The key assertion: if More() returns true and Decode fails, we get an error.
	// If More() sees the unclosed array as "no more", we get EOF. Both are correct.
	if err == nil {
		t.Fatal("expected error or EOF for malformed JSON, got nil")
	}
}

// ---------------------------------------------------------------------------
// Next — context cancellation
// ---------------------------------------------------------------------------

func TestNext_CancelledContextReturnsError(t *testing.T) {
	r := singleTweetArchive("1", "text")
	s, _ := parser.NewStream(r, 0, "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling Next

	_, err := s.Next(ctx)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestNext_ValidContextAllowsDecode(t *testing.T) {
	r := singleTweetArchive("42", "tweet text")
	s, _ := parser.NewStream(r, 0, "")

	tw, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tw == nil {
		t.Fatal("expected non-nil tweet, got nil")
	}
}

// ---------------------------------------------------------------------------
// Offset
// ---------------------------------------------------------------------------

func TestOffset_IsZeroBeforeAnyNext(t *testing.T) {
	r := singleTweetArchive("1", "text")
	s, _ := parser.NewStream(r, 0, "")

	if s.Offset() != 0 {
		t.Errorf("initial Offset() = %d, want 0", s.Offset())
	}
}

func TestOffset_AdvancesAfterEachNext(t *testing.T) {
	r := multiTweetArchive([][2]string{
		{"1", "first"},
		{"2", "second"},
		{"3", "third"},
	})
	s, _ := parser.NewStream(r, 0, "")

	prev := s.Offset()
	for i := 0; ; i++ {
		_, err := s.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		curr := s.Offset()
		if curr <= prev {
			t.Errorf("Offset did not advance after tweet %d: before=%d after=%d", i, prev, curr)
		}
		prev = curr
	}
}

func TestOffset_MatchesCheckpointAfterResume(t *testing.T) {
	// Full drain on s1, then resume from the first tweet's checkpoint and
	// verify the second stream produces the same remaining tweets.
	r := multiTweetArchive([][2]string{
		{"100", "first"},
		{"200", "second"},
	})

	s1, _ := parser.NewStream(r, 0, "")
	_, _ = s1.Next(context.Background()) // consume tweet 1
	checkpoint := s1.Offset()

	s2, err := parser.NewStream(r, checkpoint, "")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}

	tw, err := s2.Next(context.Background())
	if err != nil {
		t.Fatalf("resume Next: %v", err)
	}
	if tw.ID != 200 {
		t.Errorf("resumed tweet ID = %d, want 200", tw.ID)
	}

	_, err = s2.Next(context.Background())
	if err != io.EOF {
		t.Errorf("expected EOF after last resumed tweet, got %v", err)
	}
}

func TestOffset_DoesNotChangeOnEOF(t *testing.T) {
	r := singleTweetArchive("1", "text")
	s, _ := parser.NewStream(r, 0, "")

	s.Next(context.Background())
	afterLast := s.Offset()

	s.Next(context.Background()) // returns EOF
	afterEOF := s.Offset()

	if afterEOF != afterLast {
		t.Errorf("Offset changed after EOF: before=%d after=%d", afterLast, afterEOF)
	}
}

// ---------------------------------------------------------------------------
// Tweet.URL
// ---------------------------------------------------------------------------

func TestURL_WithUsername(t *testing.T) {
	tw := parser.Tweet{ID: 123, Username: "alice"}
	want := "https://twitter.com/alice/status/123"
	if got := tw.URL(); got != want {
		t.Errorf("URL() = %q, want %q", got, want)
	}
}

func TestURL_WithoutUsername(t *testing.T) {
	tw := parser.Tweet{ID: 456, Username: ""}
	want := "https://twitter.com/i/web/status/456"
	if got := tw.URL(); got != want {
		t.Errorf("URL() = %q, want %q", got, want)
	}
}

func TestURL_LargeID(t *testing.T) {
	const id int64 = 1234567890123456789
	tw := parser.Tweet{ID: id, Username: "bob"}
	want := "https://twitter.com/bob/status/1234567890123456789"
	if got := tw.URL(); got != want {
		t.Errorf("URL() = %q, want %q", got, want)
	}
}

func TestURL_ZeroIDWithUsername(t *testing.T) {
	tw := parser.Tweet{ID: 0, Username: "carol"}
	want := "https://twitter.com/carol/status/0"
	if got := tw.URL(); got != want {
		t.Errorf("URL() = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// Long archive — decoder stability over many tweets
// ---------------------------------------------------------------------------

// generateArchive builds a ReadSeeker containing n tweets with sequential
// IDs starting at 1 and synthetic text bodies.
func generateArchive(t *testing.T, n int) io.ReadSeeker {
	t.Helper()
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = rawTweetJSON(
			fmt.Sprintf("%d", i+1),
			fmt.Sprintf("tweet body number %d with some extra padding text to vary length", i+1),
		)
	}
	return archiveReader("[" + strings.Join(parts, ",") + "]")
}

func TestNext_DecodesLargeArchiveWithCorrectCount(t *testing.T) {
	const n = 10_000
	r := generateArchive(t, n)
	s, err := parser.NewStream(r, 0, "user")
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	tweets := drainStream(t, s)

	if len(tweets) != n {
		t.Errorf("decoded %d tweets, want %d", len(tweets), n)
	}
}

func TestNext_LargeArchiveAllIDsUnique(t *testing.T) {
	const n = 10_000
	r := generateArchive(t, n)
	s, _ := parser.NewStream(r, 0, "")

	seen := make(map[int64]bool, n)
	for {
		tw, err := s.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if seen[tw.ID] {
			t.Fatalf("duplicate ID %d", tw.ID)
		}
		seen[tw.ID] = true
	}

	if len(seen) != n {
		t.Errorf("saw %d unique IDs, want %d", len(seen), n)
	}
}

func TestNext_LargeArchiveUsernameConsistentOnEveryTweet(t *testing.T) {
	const n = 5_000
	r := generateArchive(t, n)
	s, _ := parser.NewStream(r, 0, "archiveuser")

	for i := 0; ; i++ {
		tw, err := s.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tweet %d: %v", i, err)
		}
		if tw.Username != "archiveuser" {
			t.Fatalf("tweet %d: Username = %q, want archiveuser", i, tw.Username)
		}
	}
}

func TestNext_LargeArchiveOffsetStrictlyIncreases(t *testing.T) {
	const n = 1_000
	r := generateArchive(t, n)
	s, _ := parser.NewStream(r, 0, "")

	prev := s.Offset()
	for i := 0; ; i++ {
		_, err := s.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tweet %d: %v", i, err)
		}
		curr := s.Offset()
		if curr <= prev {
			t.Fatalf("tweet %d: Offset did not advance (before=%d after=%d)", i, prev, curr)
		}
		prev = curr
	}
}

func TestNext_LargeArchiveEOFOnlyOnce(t *testing.T) {
	const n = 500
	r := generateArchive(t, n)
	s, _ := parser.NewStream(r, 0, "")

	drainStream(t, s) // exhaust all tweets

	for i := 0; i < 5; i++ {
		_, err := s.Next(context.Background())
		if err != io.EOF {
			t.Errorf("call %d after exhaustion: expected io.EOF, got %v", i+1, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Seek correctness — resume produces exactly the right remaining tweets
// ---------------------------------------------------------------------------

func TestSeek_ResumeMidArchiveYieldsCorrectRemainingTweets(t *testing.T) {
	// Build a 10-tweet archive, checkpoint after tweet 4, then verify that a
	// resumed stream produces exactly tweets 5-10 — nothing skipped, nothing
	// duplicated.
	const total = 10
	tweets := make([][2]string, total)
	for i := 0; i < total; i++ {
		tweets[i] = [2]string{
			fmt.Sprintf("%d", i+1),
			fmt.Sprintf("text %d", i+1),
		}
	}
	r := multiTweetArchive(tweets)

	// First pass: consume 4 tweets and capture the checkpoint.
	s1, _ := parser.NewStream(r, 0, "")
	for i := 0; i < 4; i++ {
		if _, err := s1.Next(context.Background()); err != nil {
			t.Fatalf("s1.Next[%d]: %v", i, err)
		}
	}
	checkpoint := s1.Offset()

	// Second pass: resume from the checkpoint.
	s2, err := parser.NewStream(r, checkpoint, "")
	if err != nil {
		t.Fatalf("NewStream resume: %v", err)
	}

	remaining := drainStream(t, s2)

	if len(remaining) != 6 {
		t.Fatalf("expected 6 remaining tweets, got %d", len(remaining))
	}
	for i, tw := range remaining {
		wantID := int64(i + 5) // tweets 5-10
		if tw.ID != wantID {
			t.Errorf("remaining[%d].ID = %d, want %d", i, tw.ID, wantID)
		}
	}
}

func TestSeek_ResumeFromFirstCheckpointSkipsOnlyFirstTweet(t *testing.T) {
	r := multiTweetArchive([][2]string{
		{"1", "first"},
		{"2", "second"},
		{"3", "third"},
	})

	s1, _ := parser.NewStream(r, 0, "")
	s1.Next(context.Background()) // consume tweet 1
	checkpoint := s1.Offset()

	s2, _ := parser.NewStream(r, checkpoint, "")
	remaining := drainStream(t, s2)

	if len(remaining) != 2 {
		t.Fatalf("expected 2 remaining tweets, got %d", len(remaining))
	}
	if remaining[0].ID != 2 || remaining[1].ID != 3 {
		t.Errorf("IDs = %v, want [2 3]", []int64{remaining[0].ID, remaining[1].ID})
	}
}

func TestSeek_ResumeFromLastCheckpointYieldsEOFImmediately(t *testing.T) {
	r := multiTweetArchive([][2]string{
		{"1", "only"},
		{"2", "last"},
	})

	s1, _ := parser.NewStream(r, 0, "")
	drainStream(t, s1) // consume everything
	checkpoint := s1.Offset()

	s2, err := parser.NewStream(r, checkpoint, "")
	if err != nil {
		t.Fatalf("NewStream resume: %v", err)
	}

	_, err = s2.Next(context.Background())
	if err != io.EOF {
		t.Errorf("expected io.EOF after resuming at end, got %v", err)
	}
}

func TestSeek_ResumeAtEachCheckpointProducesCorrectTail(t *testing.T) {
	// For every possible resume point in a 5-tweet archive, verify the tail
	// is exactly right. This catches any off-by-one in how offsets are recorded.
	const total = 5
	tweets := make([][2]string, total)
	for i := 0; i < total; i++ {
		tweets[i] = [2]string{fmt.Sprintf("%d", i+1), fmt.Sprintf("t%d", i+1)}
	}
	r := multiTweetArchive(tweets)

	// Collect checkpoints after each tweet.
	s0, _ := parser.NewStream(r, 0, "")
	checkpoints := make([]int64, total)
	for i := 0; i < total; i++ {
		if _, err := s0.Next(context.Background()); err != nil {
			t.Fatalf("collecting checkpoint %d: %v", i, err)
		}
		checkpoints[i] = s0.Offset()
	}

	// Resume from each checkpoint and verify the remaining tail.
	for resumeAfter := 0; resumeAfter < total; resumeAfter++ {
		s, err := parser.NewStream(r, checkpoints[resumeAfter], "")
		if err != nil {
			t.Fatalf("resume after tweet %d: %v", resumeAfter+1, err)
		}

		got := drainStream(t, s)
		wantCount := total - resumeAfter - 1

		if len(got) != wantCount {
			t.Errorf("resume after tweet %d: got %d tweets, want %d",
				resumeAfter+1, len(got), wantCount)
			continue
		}

		for i, tw := range got {
			wantID := int64(resumeAfter + i + 2)
			if tw.ID != wantID {
				t.Errorf("resume after tweet %d: got[%d].ID = %d, want %d",
					resumeAfter+1, i, tw.ID, wantID)
			}
		}
	}
}

func TestSeek_LargeArchiveResumeFromMidpoint(t *testing.T) {
	const total = 2_000
	const resumeAfter = 1_000

	tweets := make([][2]string, total)
	for i := 0; i < total; i++ {
		tweets[i] = [2]string{fmt.Sprintf("%d", i+1), fmt.Sprintf("body %d", i+1)}
	}
	r := multiTweetArchive(tweets)

	// Consume the first half and capture the checkpoint.
	s1, _ := parser.NewStream(r, 0, "")
	for i := 0; i < resumeAfter; i++ {
		if _, err := s1.Next(context.Background()); err != nil {
			t.Fatalf("s1.Next[%d]: %v", i, err)
		}
	}
	checkpoint := s1.Offset()

	// Resume and drain the second half.
	s2, err := parser.NewStream(r, checkpoint, "")
	if err != nil {
		t.Fatalf("NewStream resume: %v", err)
	}
	second := drainStream(t, s2)

	if len(second) != total-resumeAfter {
		t.Fatalf("expected %d tweets in second half, got %d", total-resumeAfter, len(second))
	}

	// First tweet of the second half must be ID resumeAfter+1.
	if second[0].ID != int64(resumeAfter+1) {
		t.Errorf("first resumed tweet ID = %d, want %d", second[0].ID, resumeAfter+1)
	}
	// Last tweet must be ID total.
	if second[len(second)-1].ID != int64(total) {
		t.Errorf("last resumed tweet ID = %d, want %d", second[len(second)-1].ID, total)
	}
}
