package main

import (
	"context"
	"flag"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/koluku/s3select"
)

var (
	// AWS
	region string

	// Bucket

	// S3 Select Query
	rawQuery string
)

func init() {
	// AWS
	flag.StringVar(&region, "region", "", "regeon")

	// S3 Select Config

	// S3 Select Query
	flag.StringVar(&rawQuery, "query", "", "query")
}

func cmd() int {
	flag.Parse()
	paths := flag.Args()

	if len(paths) == 0 {
		log.Println("[ERROR]", "no arguments")
		return 1
	}
	if rawQuery == "" {
		rawQuery = "SELECT * FROM S3Object s"
	}

	for _, path := range paths {
		u, err := url.Parse(path)
		if err != nil {
			log.Println("[ERROR]", err)
			return 1
		}
		var bucket, prefix string
		bucket = u.Hostname()
		prefix = strings.TrimPrefix(u.Path, "/")
		prefix = strings.TrimSuffix(prefix, "/")
		s3prefix := s3select.NewS3Prefix(bucket, prefix)
	}

	ctx := context.TODO()
	app, err := s3select.NewApp(ctx)
	if err != nil {
		log.Println("[ERROR]", err)
		return 1
	}

	opt := s3select.RunOption{
		Prefixes: string,
		Query:    rawQuery,
	}
	if err := app.Run(ctx, opt); err != nil {
		log.Println("[ERROR]", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(cmd())
}
