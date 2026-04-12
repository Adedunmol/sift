package parser

import "strconv"

type Tweet struct {
	ID   int64
	Text string
}

type rawTweet struct {
	Tweet struct {
		IDStr    string `json:"id"`
		FullText string `json:"full_text"`
	} `json:"tweet"`
}

func mapTweet(raw *rawTweet) (*Tweet, error) {
	id, err := strconv.ParseInt(raw.Tweet.IDStr, 10, 64)
	if err != nil {
		return nil, err
	}
	return &Tweet{
		ID:   id,
		Text: raw.Tweet.FullText,
	}, nil
}
