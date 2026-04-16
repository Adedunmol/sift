// Package httpclient provides a retrying HTTP client with exponential backoff.
//
// Extracted from the classifier package so it can be reused across
// any service that makes outbound HTTP calls (Gemini, webhooks, etc.).
package evaluator

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Config controls retry and timeout behaviour.
type Config struct {
	Timeout       time.Duration
	MaxRetries    int
	RetryDelay    time.Duration // base delay for exponential backoff
	MaxRetryDelay time.Duration // upper cap on backoff delay
}

// Client wraps net/http.Client with retry logic.
type Client struct {
	hc     http.Client
	config Config
}

// New returns a Client configured with cfg.
func New(cfg Config) *Client {
	return &Client{
		hc: http.Client{
			Timeout: cfg.Timeout,
		},
		config: cfg,
	}
}

// Do executes req, retrying on transient errors up to MaxRetries times.
// The caller's context is respected on every attempt — if it is cancelled
// or times out, Do returns immediately with ctx.Err().
func (c *Client) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	var (
		resp *http.Response
		err  error
	)

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {

		if attempt > 0 {
			delay := c.backoff(attempt - 1)

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		// Clone the request per attempt so the body can be re-read.
		// Callers must ensure the body is re-readable (e.g. bytes.NewReader).
		resp, err = c.hc.Do(req.WithContext(ctx))

		if err == nil && !isRetryable(resp.StatusCode) {
			return resp, nil
		}

		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}

	if err != nil {
		return nil, fmt.Errorf("after %d attempts: %w", c.config.MaxRetries+1, err)
	}

	// Exhausted retries but last response was a retryable status code.
	return resp, fmt.Errorf("after %d attempts: retryable status persists", c.config.MaxRetries+1)
}

// backoff computes exponential delay for the given attempt index (0-based),
// capped at MaxRetryDelay.
func (c *Client) backoff(attempt int) time.Duration {
	delay := c.config.RetryDelay << attempt

	if delay > c.config.MaxRetryDelay {
		return c.config.MaxRetryDelay
	}

	return delay
}

// isRetryable reports whether the HTTP status code warrants a retry.
func isRetryable(status int) bool {
	switch status {
	case http.StatusTooManyRequests,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}
