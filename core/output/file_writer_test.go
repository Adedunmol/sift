package output_test

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Adedunmol/sift/core/output"
	"github.com/Adedunmol/sift/core/parser"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func tempPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "tweets.csv")
}

func readCSV(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("readCSV open: %v", err)
	}
	defer f.Close()

	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("readCSV parse: %v", err)
	}
	return rows
}

func mustClose(t *testing.T, w *output.FileWriter) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// makeTweet builds a *parser.Tweet with the given fields.
func makeTweet(id int64, text, username string) *parser.Tweet {
	return &parser.Tweet{ID: id, Text: text, Username: username}
}

// makeTweets builds a slice of n tweet pointers.
func makeTweets(tweets ...*parser.Tweet) []*parser.Tweet {
	return tweets
}

// ---------------------------------------------------------------------------
// NewFileWriter
// ---------------------------------------------------------------------------

func TestNewFileWriter_CreatesFileWhenAbsent(t *testing.T) {
	path := tempPath(t)
	w, err := output.NewFileWriter(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer mustClose(t, w)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("expected file to be created, but it does not exist")
	}
}

func TestNewFileWriter_WritesHeaderOnNewFile(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	mustClose(t, w)

	rows := readCSV(t, path)
	if len(rows) == 0 {
		t.Fatal("expected at least the header row, got none")
	}
	want := []string{"id", "url", "text", "delete"}
	header := rows[0]
	if len(header) != len(want) {
		t.Fatalf("header len = %d, want %d", len(header), len(want))
	}
	for i, col := range want {
		if header[i] != col {
			t.Errorf("header[%d] = %q, want %q", i, header[i], col)
		}
	}
}

func TestNewFileWriter_DoesNotWriteHeaderOnExistingFile(t *testing.T) {
	path := tempPath(t)

	w1, _ := output.NewFileWriter(path)
	mustClose(t, w1)

	w2, err := output.NewFileWriter(path)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	mustClose(t, w2)

	rows := readCSV(t, path)
	headerCount := 0
	for _, row := range rows {
		if len(row) > 0 && row[0] == "id" {
			headerCount++
		}
	}
	if headerCount != 1 {
		t.Errorf("found %d header rows, want exactly 1", headerCount)
	}
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

func TestWrite_EmptySliceWritesNoRows(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	if err := w.Write([]*parser.Tweet{}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	mustClose(t, w)

	rows := readCSV(t, path)
	if len(rows) != 1 {
		t.Errorf("expected 1 row (header only), got %d", len(rows))
	}
}

func TestWrite_NilSliceWritesNoRows(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	if err := w.Write(nil); err != nil {
		t.Fatalf("Write: %v", err)
	}
	mustClose(t, w)

	rows := readCSV(t, path)
	if len(rows) != 1 {
		t.Errorf("expected 1 row (header only), got %d", len(rows))
	}
}

func TestWrite_SingleTweetProducesOneDataRow(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(99, "hello world", "alice")))
	mustClose(t, w)

	rows := readCSV(t, path)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestWrite_IDColumnMatchesTweetID(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(42, "text", "user")))
	mustClose(t, w)

	rows := readCSV(t, path)
	if rows[1][0] != "42" {
		t.Errorf("id column = %q, want %q", rows[1][0], "42")
	}
}

func TestWrite_URLColumnMatchesTweetURL(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	tw := makeTweet(7, "text", "bob")
	w.Write(makeTweets(tw))
	mustClose(t, w)

	rows := readCSV(t, path)
	if rows[1][1] != tw.URL() {
		t.Errorf("url column = %q, want %q", rows[1][1], tw.URL())
	}
}

func TestWrite_TextColumnMatchesTweetText(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(1, "the tweet body", "user")))
	mustClose(t, w)

	rows := readCSV(t, path)
	if rows[1][2] != "the tweet body" {
		t.Errorf("text column = %q, want %q", rows[1][2], "the tweet body")
	}
}

func TestWrite_DeleteColumnIsAlwaysFalse(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(
		makeTweet(1, "a", "u"),
		makeTweet(2, "b", "u"),
		makeTweet(3, "c", "u"),
	))
	mustClose(t, w)

	rows := readCSV(t, path)
	for _, row := range rows[1:] {
		if row[3] != "false" {
			t.Errorf("delete column = %q, want %q", row[3], "false")
		}
	}
}

func TestWrite_MultipleTweetsWrittenInOrder(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	tweets := []*parser.Tweet{
		makeTweet(10, "first", "u"),
		makeTweet(20, "second", "u"),
		makeTweet(30, "third", "u"),
	}
	w.Write(tweets)
	mustClose(t, w)

	rows := readCSV(t, path)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	for i, tw := range tweets {
		got := rows[i+1][0]
		want := strconv.FormatInt(tw.ID, 10)
		if got != want {
			t.Errorf("row %d id = %q, want %q", i+1, got, want)
		}
	}
}

func TestWrite_EachRowHasFourColumns(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(
		makeTweet(1, "a", "u"),
		makeTweet(2, "b", "u"),
	))
	mustClose(t, w)

	rows := readCSV(t, path)
	for i, row := range rows {
		if len(row) != 4 {
			t.Errorf("row %d has %d columns, want 4", i, len(row))
		}
	}
}

func TestWrite_TextWithCommasAndQuotesIsEscapedCorrectly(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(1, `He said, "hello, world"`, "u")))
	mustClose(t, w)

	rows := readCSV(t, path)
	want := `He said, "hello, world"`
	if rows[1][2] != want {
		t.Errorf("text = %q, want %q", rows[1][2], want)
	}
}

func TestWrite_TextWithNewlinesIsPreserved(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(1, "line one\nline two", "u")))
	mustClose(t, w)

	rows := readCSV(t, path)
	if rows[1][2] != "line one\nline two" {
		t.Errorf("text = %q, want newline preserved", rows[1][2])
	}
}

func TestWrite_EmptyTextFieldWrittenCorrectly(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(1, "", "u")))
	mustClose(t, w)

	rows := readCSV(t, path)
	if rows[1][2] != "" {
		t.Errorf("text = %q, want empty string", rows[1][2])
	}
}

func TestWrite_URLWithNoUsernameUsesWebFallback(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(55, "text", "")))
	mustClose(t, w)

	rows := readCSV(t, path)
	wantURL := "https://twitter.com/i/web/status/55"
	if rows[1][1] != wantURL {
		t.Errorf("url = %q, want %q", rows[1][1], wantURL)
	}
}

func TestWrite_MultipleCallsAppendRows(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(1, "first batch", "u")))
	w.Write(makeTweets(makeTweet(2, "second batch", "u")))
	mustClose(t, w)

	rows := readCSV(t, path)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[1][0] != "1" || rows[2][0] != "2" {
		t.Errorf("row IDs = [%s %s], want [1 2]", rows[1][0], rows[2][0])
	}
}

func TestWrite_LargeBatchAllRowsPresent(t *testing.T) {
	const n = 10_000
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)

	tweets := make([]*parser.Tweet, n)
	for i := range tweets {
		tweets[i] = makeTweet(int64(i+1), "body", "user")
	}
	if err := w.Write(tweets); err != nil {
		t.Fatalf("Write: %v", err)
	}
	mustClose(t, w)

	rows := readCSV(t, path)
	if len(rows) != n+1 {
		t.Errorf("expected %d rows, got %d", n+1, len(rows))
	}
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

func TestFlush_DataVisibleAfterFlushWithoutClose(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(1, "flush test", "u")))

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	rows := readCSV(t, path)
	if len(rows) < 2 {
		t.Fatalf("expected data row after Flush, got %d rows", len(rows))
	}
	w.Close()
}

func TestFlush_CalledMultipleTimesDoesNotError(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(1, "text", "u")))

	for i := 0; i < 3; i++ {
		if err := w.Flush(); err != nil {
			t.Fatalf("Flush call %d: %v", i+1, err)
		}
	}
	w.Close()
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func TestClose_FlushesAndClosesFile(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(1, "close test", "u")))

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rows := readCSV(t, path)
	if len(rows) < 2 {
		t.Fatalf("expected data row after Close, got %d rows", len(rows))
	}
}

func TestClose_DataNotLostWithoutExplicitFlush(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(42, "no explicit flush", "u")))
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rows := readCSV(t, path)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d — data may have been lost", len(rows))
	}
	if rows[1][0] != "42" {
		t.Errorf("id = %q, want 42", rows[1][0])
	}
}

// ---------------------------------------------------------------------------
// Append mode — reopening an existing file
// ---------------------------------------------------------------------------

func TestAppend_ReopenedFileRetainsPreviousRows(t *testing.T) {
	path := tempPath(t)

	w1, _ := output.NewFileWriter(path)
	w1.Write(makeTweets(makeTweet(1, "first session", "u")))
	mustClose(t, w1)

	w2, _ := output.NewFileWriter(path)
	w2.Write(makeTweets(makeTweet(2, "second session", "u")))
	mustClose(t, w2)

	rows := readCSV(t, path)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows after two sessions, got %d", len(rows))
	}
	if rows[1][0] != "1" || rows[2][0] != "2" {
		t.Errorf("row IDs = [%s %s], want [1 2]", rows[1][0], rows[2][0])
	}
}

func TestAppend_HeaderAppearsExactlyOnceAcrossMultipleSessions(t *testing.T) {
	path := tempPath(t)

	for i := 0; i < 5; i++ {
		w, err := output.NewFileWriter(path)
		if err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
		w.Write(makeTweets(makeTweet(int64(i+1), "text", "u")))
		mustClose(t, w)
	}

	rows := readCSV(t, path)
	headerCount := 0
	for _, row := range rows {
		if len(row) > 0 && row[0] == "id" {
			headerCount++
		}
	}
	if headerCount != 1 {
		t.Errorf("header appears %d times across 5 sessions, want 1", headerCount)
	}
}

func TestAppend_TotalRowCountCorrectAfterMultipleSessions(t *testing.T) {
	path := tempPath(t)
	const sessions = 4
	const tweetsPerSession = 3

	for s := 0; s < sessions; s++ {
		w, _ := output.NewFileWriter(path)
		tweets := make([]*parser.Tweet, tweetsPerSession)
		for i := range tweets {
			tweets[i] = makeTweet(int64(s*tweetsPerSession+i+1), "text", "u")
		}
		w.Write(tweets)
		mustClose(t, w)
	}

	rows := readCSV(t, path)
	want := 1 + sessions*tweetsPerSession
	if len(rows) != want {
		t.Errorf("total rows = %d, want %d", len(rows), want)
	}
}

// ---------------------------------------------------------------------------
// Implements output.Writer interface
// ---------------------------------------------------------------------------

func TestFileWriter_ImplementsWriterInterface(t *testing.T) {
	path := tempPath(t)
	w, err := output.NewFileWriter(path)
	if err != nil {
		t.Fatalf("NewFileWriter: %v", err)
	}
	defer w.Close()

	var _ output.Writer = w
}

// ---------------------------------------------------------------------------
// Column order is stable
// ---------------------------------------------------------------------------

func TestWrite_ColumnOrderIsIDURLTextDelete(t *testing.T) {
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	tw := makeTweet(7, "some text", "carol")
	w.Write(makeTweets(tw))
	mustClose(t, w)

	rows := readCSV(t, path)
	row := rows[1]

	if row[0] != "7" {
		t.Errorf("col[0] (id) = %q, want %q", row[0], "7")
	}
	if row[1] != tw.URL() {
		t.Errorf("col[1] (url) = %q, want %q", row[1], tw.URL())
	}
	if row[2] != "some text" {
		t.Errorf("col[2] (text) = %q, want %q", row[2], "some text")
	}
	if row[3] != "false" {
		t.Errorf("col[3] (delete) = %q, want %q", row[3], "false")
	}
}

// ---------------------------------------------------------------------------
// Unicode
// ---------------------------------------------------------------------------

func TestWrite_UnicodeTextRoundTrips(t *testing.T) {
	cases := []string{
		"こんにちは世界",
		"مرحبا بالعالم",
		"emoji 🔥🎉✅",
		"mixed: hello 日本語 🌍",
	}

	for _, text := range cases {
		text := text
		t.Run(text[:min(len(text), 20)], func(t *testing.T) {
			path := tempPath(t)
			w, _ := output.NewFileWriter(path)
			w.Write(makeTweets(makeTweet(1, text, "u")))
			mustClose(t, w)

			rows := readCSV(t, path)
			if rows[1][2] != text {
				t.Errorf("text = %q, want %q", rows[1][2], text)
			}
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// Large text body
// ---------------------------------------------------------------------------

func TestWrite_VeryLongTextBody(t *testing.T) {
	longText := strings.Repeat("a", 100_000)
	path := tempPath(t)
	w, _ := output.NewFileWriter(path)
	w.Write(makeTweets(makeTweet(1, longText, "u")))
	mustClose(t, w)

	rows := readCSV(t, path)
	if rows[1][2] != longText {
		t.Errorf("long text not preserved: got len=%d, want len=%d", len(rows[1][2]), len(longText))
	}
}
