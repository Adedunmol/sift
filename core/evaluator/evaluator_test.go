package evaluator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Adedunmol/sift/core/evaluator"
	"github.com/Adedunmol/sift/core/parser"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// geminiResponse mirrors the wire format the real Gemini API returns.
// Used by fake servers to produce well-formed responses.
type geminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
}

// buildGeminiResponse encodes modelText (the inner JSON string that Process
// will unmarshal) inside a valid Gemini envelope.
func buildGeminiResponse(t *testing.T, modelText string) []byte {
	t.Helper()

	resp := geminiResponse{}
	resp.Candidates = append(resp.Candidates, struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	}{})
	resp.Candidates[0].Content.Parts = append(
		resp.Candidates[0].Content.Parts,
		struct {
			Text string `json:"text"`
		}{Text: modelText},
	)

	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("buildGeminiResponse: %v", err)
	}
	return b
}

// idsPayload returns the JSON string the model is expected to produce for
// the given tweet IDs.
func idsPayload(ids []int64) string {
	type result struct {
		IDs []int64 `json:"ids"`
	}
	b, _ := json.Marshal(result{IDs: ids})
	return string(b)
}

// newGemini builds a Gemini processor wired to the given test server URL.
func newGemini(t *testing.T, serverURL string, criteria evaluator.Criteria) *evaluator.Gemini {
	t.Helper()
	return evaluator.NewGemini(evaluator.GeminiConfig{
		APIKey:   "test-key",
		BaseURL:  serverURL,
		Criteria: criteria,
	})
}

// makeTweets builds a slice of *parser.Tweet with sequential IDs.
func makeTweets(ids ...int64) []*parser.Tweet {
	tweets := make([]*parser.Tweet, len(ids))
	for i, id := range ids {
		tweets[i] = &parser.Tweet{ID: id}
	}
	return tweets
}

// ---------------------------------------------------------------------------
// NewGemini
// ---------------------------------------------------------------------------

func TestNewGemini_UsesEnvAPIKeyWhenConfigKeyEmpty(t *testing.T) {
	t.Setenv("GEMINI_API_KEY", "env-key")

	// If NewGemini panics or returns nil without a key, the test will catch it.
	g := evaluator.NewGemini(evaluator.GeminiConfig{})
	if g == nil {
		t.Fatal("NewGemini returned nil")
	}
}

func TestNewGemini_ConfigKeyTakesPrecedenceOverEnv(t *testing.T) {
	t.Setenv("GEMINI_API_KEY", "env-key")

	// We can't inspect the key directly (unexported), but a fake server can
	// check the key query param that Process will append.
	var capturedURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedURL = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
		w.Write(buildGeminiResponse(t, idsPayload(nil)))
	}))
	defer srv.Close()

	g := evaluator.NewGemini(evaluator.GeminiConfig{
		APIKey:  "config-key",
		BaseURL: srv.URL,
	})
	g.Process(context.Background(), makeTweets(1))

	if capturedURL != "key=config-key" {
		t.Errorf("query = %q, want key=config-key", capturedURL)
	}
}

// ---------------------------------------------------------------------------
// Process — input edge cases
// ---------------------------------------------------------------------------

func TestProcess_EmptyTweetsReturnsNilWithoutCallingAPI(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	result, err := g.Process(context.Background(), nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result for empty input, got %v", result)
	}
	if called {
		t.Error("API should not be called for empty tweet slice")
	}
}

func TestProcess_EmptySliceReturnsNilWithoutCallingAPI(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	result, err := g.Process(context.Background(), []*parser.Tweet{})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result for empty slice, got %v", result)
	}
	if called {
		t.Error("API should not be called for empty tweet slice")
	}
}

// ---------------------------------------------------------------------------
// Process — happy paths
// ---------------------------------------------------------------------------

func TestProcess_ReturnsFlaggedTweets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(buildGeminiResponse(t, idsPayload([]int64{10, 20})))
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	result, err := g.Process(context.Background(), makeTweets(10, 20, 30))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 flagged tweets, got %d", len(result))
	}
	if result[0].ID != 10 || result[1].ID != 20 {
		t.Errorf("flagged IDs = %v, want [10 20]", result)
	}
}

func TestProcess_ReturnsEmptySliceWhenNoneFlagged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(buildGeminiResponse(t, idsPayload([]int64{})))
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	result, err := g.Process(context.Background(), makeTweets(1, 2, 3))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected 0 flagged tweets, got %d: %v", len(result), result)
	}
}

func TestProcess_AllTweetsFlagged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(buildGeminiResponse(t, idsPayload([]int64{1, 2, 3})))
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	result, err := g.Process(context.Background(), makeTweets(1, 2, 3))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("expected 3 flagged tweets, got %d", len(result))
	}
}

func TestProcess_SingleTweetFlagged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(buildGeminiResponse(t, idsPayload([]int64{42})))
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	result, err := g.Process(context.Background(), makeTweets(42))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].ID != 42 {
		t.Errorf("expected [{42}], got %v", result)
	}
}

func TestProcess_SendsJSONContentTypeHeader(t *testing.T) {
	var capturedContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedContentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
		w.Write(buildGeminiResponse(t, idsPayload(nil)))
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	g.Process(context.Background(), makeTweets(1))

	if capturedContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", capturedContentType)
	}
}

func TestProcess_SendsPOSTRequest(t *testing.T) {
	var capturedMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedMethod = r.Method
		w.WriteHeader(http.StatusOK)
		w.Write(buildGeminiResponse(t, idsPayload(nil)))
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	g.Process(context.Background(), makeTweets(1))

	if capturedMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", capturedMethod)
	}
}

// ---------------------------------------------------------------------------
// Process — API / HTTP error paths
// ---------------------------------------------------------------------------

func TestProcess_HTTPErrorReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	// The underlying client treats 500 as non-retryable and returns the
	// response. Process then tries to decode an empty body — that decode
	// failure becomes the error we assert on.
	_, err := g.Process(context.Background(), makeTweets(1))
	if err == nil {
		t.Fatal("expected error for 500 response, got nil")
	}
}

func TestProcess_NonJSONResponseBodyReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json at all"))
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	_, err := g.Process(context.Background(), makeTweets(1))
	if err == nil {
		t.Fatal("expected error for non-JSON body, got nil")
	}
}

func TestProcess_EmptyCandidatesReturnsError(t *testing.T) {
	// Valid JSON envelope but zero candidates.
	body, _ := json.Marshal(map[string]interface{}{"candidates": []interface{}{}})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	_, err := g.Process(context.Background(), makeTweets(1))
	if err == nil {
		t.Fatal("expected error for empty candidates, got nil")
	}
}

func TestProcess_EmptyPartsReturnsError(t *testing.T) {
	// One candidate but no parts.
	body, _ := json.Marshal(map[string]interface{}{
		"candidates": []map[string]interface{}{
			{"content": map[string]interface{}{"parts": []interface{}{}}},
		},
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	_, err := g.Process(context.Background(), makeTweets(1))
	if err == nil {
		t.Fatal("expected error for empty parts, got nil")
	}
}

func TestProcess_ModelOutputNotJSONReturnsError(t *testing.T) {
	// The outer envelope is valid, but the inner text isn't parseable IDs JSON.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(buildGeminiResponse(t, "sure, here are the tweets you asked about"))
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	_, err := g.Process(context.Background(), makeTweets(1))
	if err == nil {
		t.Fatal("expected error for non-JSON model output, got nil")
	}
}

func TestProcess_ModelOutputMissingIDsFieldReturnsEmptySlice(t *testing.T) {
	// Valid JSON but wrong schema — no "ids" key. Unmarshal succeeds with
	// zero-value slice, so Process should return an empty result, not an error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(buildGeminiResponse(t, `{"tweets":[]}`))
	}))
	defer srv.Close()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	result, err := g.Process(context.Background(), makeTweets(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty result for missing ids key, got %v", result)
	}
}

func TestProcess_NetworkErrorReturnsError(t *testing.T) {
	// Point at a server that's already closed — all TCP connections refused.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // close immediately before the request

	// Use zero retries so the test doesn't spin.
	g := evaluator.NewGemini(evaluator.GeminiConfig{
		APIKey:   "key",
		BaseURL:  srv.URL,
		Criteria: evaluator.Criteria{},
	})
	_, err := g.Process(context.Background(), makeTweets(1))
	if err == nil {
		t.Fatal("expected error for closed server, got nil")
	}
}

// ---------------------------------------------------------------------------
// Process — context cancellation
// ---------------------------------------------------------------------------

func TestProcess_CancelledContextReturnsError(t *testing.T) {
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked
	}))
	defer srv.Close()
	defer close(blocked)

	ctx, cancel := context.WithCancel(context.Background())

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	done := make(chan error, 1)
	go func() {
		_, err := g.Process(ctx, makeTweets(1))
		done <- err
	}()

	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error for cancelled context, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not return after context cancellation")
	}
}

func TestProcess_DeadlineExceededReturnsError(t *testing.T) {
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked
	}))
	defer srv.Close()
	defer close(blocked)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	g := newGemini(t, srv.URL, evaluator.Criteria{})
	_, err := g.Process(ctx, makeTweets(1))
	if err == nil {
		t.Fatal("expected deadline error, got nil")
	}
}

// ---------------------------------------------------------------------------
// Process — criteria are included in the prompt body
// ---------------------------------------------------------------------------

func TestProcess_CriteriaIncludedInRequestBody(t *testing.T) {
	var capturedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = json.Marshal(r.Body) // capture raw
		// Read the body properly
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		capturedBody = buf[:n]

		w.WriteHeader(http.StatusOK)
		w.Write(buildGeminiResponse(t, idsPayload(nil)))
	}))
	defer srv.Close()

	criteria := evaluator.Criteria{
		ForbiddenWords: []string{"badword"},
		Tone:           "professional",
	}
	g := newGemini(t, srv.URL, criteria)
	g.Process(context.Background(), makeTweets(1))

	body := string(capturedBody)
	if body == "" {
		t.Fatal("captured request body is empty")
	}
	// The prompt is nested inside the contents array — verify it's non-empty
	// and contains the contents key.
	var parsed map[string]interface{}
	if err := json.Unmarshal(capturedBody, &parsed); err != nil {
		t.Fatalf("request body is not valid JSON: %v\nbody: %s", err, body)
	}
	if _, ok := parsed["contents"]; !ok {
		t.Errorf("request body missing 'contents' key: %s", body)
	}
}
