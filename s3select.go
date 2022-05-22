package s3select

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/goccy/go-json"
)

type SelectObjectContentInput s3.SelectObjectContentInput

type App struct {
	s3 *s3.Client
}

func NewApp(ctx context.Context) (*App, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	s3Client := s3.NewFromConfig(cfg)
	app := &App{
		s3: s3Client,
	}

	return app, nil
}

type RunOption struct {
	Prefixes string
	Query    string
}

func (a *App) Run(ctx context.Context, opt RunOption) error {
	// listを作るやつ
	keys := a.pagenate(bucket, prefix)
	params := &s3.SelectObjectContentInput{
		Bucket:          aws.String(opt.bucket.Name),
		Key:             aws.String(opt.Query),
		ExpressionType:  types.ExpressionTypeSql,
		Expression:      aws.String("SELECT * FROM S3Object LIMIT 2"),
		RequestProgress: &types.RequestProgress{},
		InputSerialization: &types.InputSerialization{
			CompressionType: types.CompressionTypeGzip,
			JSON: &types.JSONInput{
				Type: types.JSONTypeLines,
			},
		},
		OutputSerialization: &types.OutputSerialization{
			JSON: &types.JSONOutput{},
		},
	}

	resp, err := a.s3.SelectObjectContent(ctx, params)
	if err != nil {
		return err
	}
	stream := resp.GetStream()
	defer stream.Close()

	var arr [][]byte
	for event := range stream.Events() {
		v, ok := event.(*types.SelectObjectContentEventStreamMemberRecords)
		if ok {
			value := v.Value.Payload
			s := bytes.TrimRight(value, "\n")
			arr = bytes.Split(s, []byte("\n"))
		}
	}

	if err := stream.Err(); err != nil {
		return err
	}

	for _, v := range arr {
		var buf bytes.Buffer
		err := json.Indent(&buf, v, "", "  ")
		if err != nil {
			panic(err)
		}
		indentJson := buf.String()
		fmt.Println(indentJson)
	}

	return nil
}

type s3Prefix struct {
	backet string
	prefix string
}

func (a *App) pagenate(prefix s3Prefix) error {
	return nil
}

type s3Object struct {
	backet string
	key    string
}

func (a *App) select(object string, query string) error {
	return nil
}

// type JSONQueryInput struct {
// 	Select    string
// 	From      string
// 	Where     string
// 	Limit     int
// 	Count     bool
// 	MaxRetry  int
// 	Delimiter string
// }

// func NewDefaultJSONQueryInput() *JSONQueryInput {
// 	return &JSONQueryInput{
// 		Select:   "*",
// 		From:     "s3object s",
// 		Where:    "",
// 		Limit:    0,
// 		Count:    false,
// 		MaxRetry: 5,
// 	}
// }
