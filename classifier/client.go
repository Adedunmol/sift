package classifier

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Config struct {
	Timeout       time.Duration
	MaxRetries    int
	RetryDelay    time.Duration // base delay
	MaxRetryDelay time.Duration // max cap
}

type Client struct {
	hc     http.Client
	config Config
}

func New(cfg Config) *Client {
	return &Client{
		hc: http.Client{
			Timeout: cfg.Timeout,
		},
		config: cfg,
	}
}

func (c *Client) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	var (
		resp *http.Response
		err  error
	)

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {

		// Apply backoff delay (skip first attempt)
		if attempt > 0 {
			delay := c.backoff(attempt - 1)

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		resp, err = c.hc.Do(req)

		// Success: no error and not retryable status
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

	return resp, nil
}

func (c *Client) backoff(attempt int) time.Duration {
	delay := c.config.RetryDelay << attempt

	if delay > c.config.MaxRetryDelay {
		return c.config.MaxRetryDelay
	}

	return delay
}

// isRetryable determines if a status code should be retried
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
