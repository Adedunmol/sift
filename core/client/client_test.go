package client_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	client "github.com/Adedunmol/sift/core/client" // replace with your actual module path
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// fastConfig returns a Config with near-zero delays so tests finish quickly.
func fastConfig(maxRetries int) client.Config {
	return client.Config{
		Timeout:       2 * time.Second,
		MaxRetries:    maxRetries,
		RetryDelay:    time.Millisecond,
		MaxRetryDelay: 5 * time.Millisecond,
	}
}

// newRequest builds a GET request pointed at the given URL.
// Body is always nil for these tests, so re-use across retries is safe.
func newRequest(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("newRequest: %v", err)
	}
	return req
}

// statusServer returns a test server that always responds with the given code.
func statusServer(code int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(code)
	}))
}

// countingServer returns a test server that responds with successive status
// codes from the codes slice; the call counter is exposed via the returned
// pointer. If more requests arrive than codes, it responds with the last code.
func countingServer(codes []int) (*httptest.Server, *int) {
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := count
		if idx >= len(codes) {
			idx = len(codes) - 1
		}
		count++
		w.WriteHeader(codes[idx])
	}))
	return srv, &count
}

// ---------------------------------------------------------------------------
// New / Config
// ---------------------------------------------------------------------------

// TestNew_DoesNotPanic verifies that New accepts a zero-value Config without
// panicking. Behavioural correctness is covered by the Do tests below.
func TestNew_DoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("New panicked: %v", r)
		}
	}()
	client.New(client.Config{})
}

// TestNew_TimeoutApplied verifies that the configured timeout is honoured by
// Do. A server that hangs indefinitely is used; the client's own timeout
// should cause the request to fail.
func TestNew_TimeoutApplied(t *testing.T) {
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked // never unblocks during this test
	}))
	defer srv.Close()
	defer close(blocked)

	c := client.New(client.Config{
		Timeout:    50 * time.Millisecond,
		MaxRetries: 0,
	})

	_, err := c.Do(context.Background(), newRequest(t, srv.URL))
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

// ---------------------------------------------------------------------------
// Do — success paths
// ---------------------------------------------------------------------------

func TestDo_SuccessOnFirstAttempt(t *testing.T) {
	srv, count := countingServer([]int{http.StatusOK})
	defer srv.Close()

	c := client.New(fastConfig(3))
	resp, err := c.Do(context.Background(), newRequest(t, srv.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if *count != 1 {
		t.Errorf("server hit %d times, want 1", *count)
	}
}

func TestDo_NonRetryableStatusReturnedImmediately(t *testing.T) {
	// 404 is not retryable — should be returned on the first attempt.
	srv, count := countingServer([]int{http.StatusNotFound})
	defer srv.Close()

	c := client.New(fastConfig(3))
	resp, err := c.Do(context.Background(), newRequest(t, srv.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	if *count != 1 {
		t.Errorf("server hit %d times, want 1 (no retry)", *count)
	}
}

func TestDo_SuccessAfterRetries(t *testing.T) {
	// Fail twice with 503, then succeed.
	srv, count := countingServer([]int{
		http.StatusServiceUnavailable,
		http.StatusServiceUnavailable,
		http.StatusOK,
	})
	defer srv.Close()

	c := client.New(fastConfig(3))
	resp, err := c.Do(context.Background(), newRequest(t, srv.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if *count != 3 {
		t.Errorf("server hit %d times, want 3", *count)
	}
}

// ---------------------------------------------------------------------------
// Do — retry exhaustion
// ---------------------------------------------------------------------------

func TestDo_ExhaustsRetriesOnPersistentRetryableStatus(t *testing.T) {
	srv := statusServer(http.StatusServiceUnavailable)
	defer srv.Close()

	c := client.New(fastConfig(2)) // 1 attempt + 2 retries = 3 total
	_, err := c.Do(context.Background(), newRequest(t, srv.URL))
	if err == nil {
		t.Fatal("expected error after exhausted retries, got nil")
	}
	if !strings.Contains(err.Error(), "3 attempts") {
		t.Errorf("error %q should mention attempt count", err.Error())
	}
}

func TestDo_ExhaustsRetriesOnNetworkError(t *testing.T) {
	c := client.New(fastConfig(2))
	req := newRequest(t, "http://127.0.0.1:1") // port 1 is always refused
	_, err := c.Do(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for refused connection, got nil")
	}
	if !strings.Contains(err.Error(), "3 attempts") {
		t.Errorf("error %q should mention attempt count", err.Error())
	}
}

func TestDo_TotalAttemptsIsMaxRetriesPlusOne(t *testing.T) {
	srv, count := countingServer([]int{
		http.StatusTooManyRequests,
		http.StatusTooManyRequests,
		http.StatusTooManyRequests,
		http.StatusTooManyRequests,
	})
	defer srv.Close()

	c := client.New(fastConfig(3)) // MaxRetries = 3 → 4 total attempts
	c.Do(context.Background(), newRequest(t, srv.URL))

	if *count != 4 {
		t.Errorf("server hit %d times, want 4 (1 + 3 retries)", *count)
	}
}

func TestDo_ZeroRetriesMakesExactlyOneAttempt(t *testing.T) {
	srv, count := countingServer([]int{http.StatusBadGateway})
	defer srv.Close()

	c := client.New(fastConfig(0))
	c.Do(context.Background(), newRequest(t, srv.URL))

	if *count != 1 {
		t.Errorf("server hit %d times, want 1", *count)
	}
}

// ---------------------------------------------------------------------------
// Do — retryable vs non-retryable status codes
// ---------------------------------------------------------------------------

func TestDo_RetryableStatusCodesAreRetried(t *testing.T) {
	retryable := []int{
		http.StatusTooManyRequests,    // 429
		http.StatusBadGateway,         // 502
		http.StatusServiceUnavailable, // 503
		http.StatusGatewayTimeout,     // 504
	}

	for _, code := range retryable {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			// First response is the retryable code; second is 200.
			srv, count := countingServer([]int{code, http.StatusOK})
			defer srv.Close()

			c := client.New(fastConfig(1))
			resp, err := c.Do(context.Background(), newRequest(t, srv.URL))
			if err != nil {
				t.Fatalf("code %d: unexpected error: %v", code, err)
			}
			resp.Body.Close()

			if *count != 2 {
				t.Errorf("code %d: server hit %d times, want 2", code, *count)
			}
		})
	}
}

func TestDo_NonRetryableStatusCodesAreNotRetried(t *testing.T) {
	nonRetryable := []int{
		http.StatusOK,
		http.StatusCreated,
		http.StatusNoContent,
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusUnprocessableEntity,
		http.StatusInternalServerError, // 500 is intentionally not retryable
	}

	for _, code := range nonRetryable {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			srv, count := countingServer([]int{code})
			defer srv.Close()

			c := client.New(fastConfig(3))
			resp, err := c.Do(context.Background(), newRequest(t, srv.URL))
			if err != nil {
				t.Fatalf("code %d: unexpected error: %v", code, err)
			}
			resp.Body.Close()

			if *count != 1 {
				t.Errorf("code %d: server hit %d times, want 1", code, *count)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Do — context cancellation and deadline
// ---------------------------------------------------------------------------

func TestDo_ContextAlreadyCancelled(t *testing.T) {
	srv := statusServer(http.StatusOK)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before Do is called

	c := client.New(fastConfig(3))
	_, err := c.Do(ctx, newRequest(t, srv.URL))
	if err == nil {
		t.Fatal("expected error for pre-cancelled context, got nil")
	}
}

func TestDo_ContextCancelledDuringBackoff(t *testing.T) {
	// Long backoff ensures the cancel fires while Do is sleeping between retries.
	cfg := client.Config{
		Timeout:       2 * time.Second,
		MaxRetries:    5,
		RetryDelay:    500 * time.Millisecond,
		MaxRetryDelay: 2 * time.Second,
	}

	srv := statusServer(http.StatusServiceUnavailable)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := client.New(cfg)
	done := make(chan error, 1)
	go func() {
		_, err := c.Do(ctx, newRequest(t, srv.URL))
		done <- err
	}()

	// Give the first attempt time to complete, then cancel mid-backoff.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected context error, got nil")
		}
		if err != context.Canceled {
			t.Errorf("want context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Do did not honour context cancellation within 3 s")
	}
}

func TestDo_ContextDeadlineExceeded(t *testing.T) {
	cfg := client.Config{
		Timeout:       2 * time.Second,
		MaxRetries:    5,
		RetryDelay:    500 * time.Millisecond,
		MaxRetryDelay: 2 * time.Second,
	}

	srv := statusServer(http.StatusServiceUnavailable)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	c := client.New(cfg)
	_, err := c.Do(ctx, newRequest(t, srv.URL))
	if err == nil {
		t.Fatal("expected deadline error, got nil")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("want context.DeadlineExceeded, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Do — backoff timing (observable via elapsed wall time)
//
// These tests cover the doubling and cap behaviours without accessing the
// unexported backoff method. They measure elapsed wall time and assert it
// falls within a generous window to stay stable on slow CI machines.
// ---------------------------------------------------------------------------

func TestDo_BackoffDelayDoublesAcrossRetries(t *testing.T) {
	cfg := client.Config{
		Timeout:       5 * time.Second,
		MaxRetries:    2,
		RetryDelay:    50 * time.Millisecond,
		MaxRetryDelay: time.Second,
	}
	// Expected delays: attempt-1 backoff = 50ms, attempt-2 backoff = 100ms → ≥150ms total.
	srv := statusServer(http.StatusServiceUnavailable)
	defer srv.Close()

	c := client.New(cfg)
	start := time.Now()
	c.Do(context.Background(), newRequest(t, srv.URL))
	elapsed := time.Since(start)

	if elapsed < 150*time.Millisecond {
		t.Errorf("elapsed %v < 150ms minimum — backoff may not be doubling", elapsed)
	}
	if elapsed > time.Second {
		t.Errorf("elapsed %v > 1s — unexpectedly slow", elapsed)
	}
}

func TestDo_BackoffCappedAtMaxRetryDelay(t *testing.T) {
	cfg := client.Config{
		Timeout:       5 * time.Second,
		MaxRetries:    3,
		RetryDelay:    100 * time.Millisecond,
		MaxRetryDelay: 120 * time.Millisecond, // cap kicks in from attempt-2 onward
	}
	// Delays with cap:    100 + 120 + 120 = 340ms.
	// Delays without cap: 100 + 200 + 400 = 700ms.
	srv := statusServer(http.StatusServiceUnavailable)
	defer srv.Close()

	c := client.New(cfg)
	start := time.Now()
	c.Do(context.Background(), newRequest(t, srv.URL))
	elapsed := time.Since(start)

	if elapsed >= 700*time.Millisecond {
		t.Errorf("elapsed %v ≥ 700ms suggests the cap is not being applied", elapsed)
	}
	if elapsed < 100*time.Millisecond {
		t.Errorf("elapsed %v < 100ms — a retry may have been skipped", elapsed)
	}
}
