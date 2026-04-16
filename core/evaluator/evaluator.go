// Package classifier evaluates tweets against user-defined criteria
// using an external AI model.
//
// The Processor interface is the seam between processing logic and the
// model backend. The CLI and worker both depend on Processor — the
// concrete Gemini implementation is injected at startup.
package evaluator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	client2 "github.com/Adedunmol/sift/core/client"
	"net/http"
	"os"
	"time"

	"github.com/Adedunmol/sift/core/parser"
)

// Processor evaluates a batch of tweets and returns those that match
// the configured criteria. Implementations must be safe for concurrent use.
type Processor interface {
	Process(ctx context.Context, tweets []*parser.Tweet) ([]parser.Tweet, error)
}

// Criteria defines what the model should flag for deletion.
// It is passed in by the caller rather than hardcoded, so the CLI
// can load it from a config file and the worker can load it from the job payload.
type Criteria struct {
	ForbiddenWords    []string `json:"forbidden_words"`
	ProfessionalCheck bool     `json:"professional_check"`
	Tone              string   `json:"tone"`
	ExcludePolitics   bool     `json:"exclude_politics"`
}

// Gemini is a Processor backed by Google's Gemini API.
type Gemini struct {
	baseURL  string
	apiKey   string
	client   *client2.Client
	criteria Criteria
}

// GeminiConfig holds everything needed to construct a Gemini processor.
type GeminiConfig struct {
	// APIKey defaults to the GEMINI_API_KEY env var if empty.
	APIKey   string
	Criteria Criteria
}

// NewGemini returns a Gemini Processor using cfg.
// If cfg.APIKey is empty, GEMINI_API_KEY is read from the environment.
func NewGemini(cfg GeminiConfig) *Gemini {
	apiKey := cfg.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("GEMINI_API_KEY")
	}

	return &Gemini{
		baseURL:  "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent",
		apiKey:   apiKey,
		criteria: cfg.Criteria,
		client: client2.New(client2.Config{
			Timeout:       5 * time.Second,
			MaxRetries:    5,
			RetryDelay:    100 * time.Millisecond,
			MaxRetryDelay: 2 * time.Second,
		}),
	}
}

// Process sends tweets to Gemini and returns those flagged by the criteria.
// The caller's context controls cancellation — important for the worker,
// where a job can be cancelled mid-flight.
func (g *Gemini) Process(ctx context.Context, tweets []*parser.Tweet) ([]parser.Tweet, error) {
	if len(tweets) == 0 {
		return nil, nil
	}

	prompt := buildPrompt(g.criteria, tweets)

	reqBody := map[string]interface{}{
		"contents": []map[string]interface{}{
			{
				"parts": []map[string]string{
					{"text": prompt},
				},
			},
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := fmt.Sprintf("%s?key=%s", g.baseURL, g.apiKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := g.client.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gemini request: %w", err)
	}
	defer resp.Body.Close()

	var geminiResp geminiResponse
	if err := json.NewDecoder(resp.Body).Decode(&geminiResp); err != nil {
		return nil, fmt.Errorf("decode gemini response: %w", err)
	}

	if len(geminiResp.Candidates) == 0 ||
		len(geminiResp.Candidates[0].Content.Parts) == 0 {
		return nil, fmt.Errorf("empty response from gemini")
	}

	rawText := geminiResp.Candidates[0].Content.Parts[0].Text

	var result struct {
		IDs []int64 `json:"ids"`
	}

	if err := json.Unmarshal([]byte(rawText), &result); err != nil {
		return nil, fmt.Errorf("parse model output: %w\nraw: %s", err, rawText)
	}

	flagged := make([]parser.Tweet, 0, len(result.IDs))
	for _, id := range result.IDs {
		flagged = append(flagged, parser.Tweet{ID: id})
	}

	return flagged, nil
}

// geminiResponse mirrors the Gemini API response envelope.
// Unexported because callers only interact with []parser.Tweet.
type geminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
}

func buildPrompt(criteria Criteria, tweets []*parser.Tweet) string {
	return fmt.Sprintf(`
You are a strict tweet filtering system.

Your task:
Select ONLY tweets that satisfy ALL the given criteria.

CRITERIA:
%s

TWEETS:
%s

RULES:
- Return ONLY valid JSON
- Do NOT include explanations
- Do NOT include extra text
- Output format MUST be:

{
  "ids": [1, 2, 3]
}

- "ids" must contain ONLY tweet IDs that match the criteria
- If none match, return: { "ids": [] }
`,
		mustJSON(criteria),
		mustJSON(tweets),
	)
}

func mustJSON(v interface{}) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}
