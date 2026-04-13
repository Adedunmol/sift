package classifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Adedunmol/sift/parser"
	"net/http"
	"os"
	"time"
)

type Processor interface {
	Process(tweets []parser.Tweet) ([]parser.Tweet, error)
}

type Gemini struct {
	baseURL string
	apiKey  string
	client  *Client
}

type GeminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
}

type Criteria struct {
	ForbiddenWords    []string `json:"forbidden_words"`
	ProfessionalCheck bool     `json:"professional_check"`
	Tone              string   `json:"tone"`
	ExcludePolitics   bool     `json:"exclude_politics"`
}

func NewGemini() *Gemini {
	return &Gemini{
		baseURL: "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent",
		apiKey:  os.Getenv("GEMINI_API_KEY"),
		client: New(Config{
			Timeout:       5 * time.Second,
			MaxRetries:    5,
			RetryDelay:    100 * time.Millisecond,
			MaxRetryDelay: 2 * time.Second,
		}),
	}
}

func (g *Gemini) Process(tweets []parser.Tweet) ([]parser.Tweet, error) {
	if len(tweets) == 0 {
		return nil, nil
	}

	criteria := Criteria{
		ForbiddenWords:    []string{"crypto", "NFT", "hustlegrindset"},
		ProfessionalCheck: true,
		Tone:              "respectful and thoughtful",
		ExcludePolitics:   true,
	}

	prompt := buildPrompt(criteria, tweets)

	reqBody := map[string]interface{}{
		"contents": []map[string]interface{}{
			{
				"parts": []map[string]string{
					{
						"text": prompt,
					},
				},
			},
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s?key=%s", g.baseURL, g.apiKey)

	req, err := http.NewRequest("POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := g.client.Do(context.Background(), req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var geminiResp GeminiResponse
	if err := json.NewDecoder(resp.Body).Decode(&geminiResp); err != nil {
		return nil, err
	}

	if len(geminiResp.Candidates) == 0 ||
		len(geminiResp.Candidates[0].Content.Parts) == 0 {
		return nil, fmt.Errorf("empty response from Gemini")
	}

	rawText := geminiResp.Candidates[0].Content.Parts[0].Text

	// Parse model output
	var result struct {
		IDs []int64 `json:"ids"`
	}

	if err := json.Unmarshal([]byte(rawText), &result); err != nil {
		return nil, fmt.Errorf("failed to parse model output: %w\nraw: %s", err, rawText)
	}

	filteredTweets := make([]parser.Tweet, 0, len(tweets))
	for _, id := range result.IDs {
		filteredTweets = append(filteredTweets, tweets[id])
	}

	return filteredTweets, nil
}

func buildPrompt(criteria Criteria, tweets []parser.Tweet) string {
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
