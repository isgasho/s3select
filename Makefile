run:
	AWS_PROFILE=kuwata go run cmd/s3s/main.go -bucket=logs-ce-kuwata-kayac-com
build:
	go build
