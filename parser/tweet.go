package parser

import "strconv"

// Tweet is the normalised, in-memory representation of a single tweet.
// It is the only type shared across core packages — classifier, output,
// and processor all depend on it.
type Tweet struct {
	ID       int64
	Text     string
	Username string // populated from archive metadata; used to build URLs
}

// URL returns the canonical X/Twitter link for the tweet.
// The CLI and worker both write this to the output CSV rather than
// reconstructing it at read time.
func (t Tweet) URL() string {
	if t.Username == "" {
		// Fallback: ID-only path still resolves on Twitter.
		return "https://twitter.com/i/web/status/" + strconv.FormatInt(t.ID, 10)
	}
	return "https://twitter.com/" + t.Username + "/status/" + strconv.FormatInt(t.ID, 10)
}

// rawTweet mirrors the JSON structure inside a Twitter archive file.
// Unexported — callers only ever see Tweet.
type rawTweet struct {
	Tweet struct {
		IDStr    string `json:"id"`
		FullText string `json:"full_text"`
	} `json:"tweet"`
}

func mapTweet(raw *rawTweet, username string) (*Tweet, error) {
	id, err := strconv.ParseInt(raw.Tweet.IDStr, 10, 64)
	if err != nil {
		return nil, err
	}
	return &Tweet{
		ID:       id,
		Text:     raw.Tweet.FullText,
		Username: username,
	}, nil
}
