dev:
	go run main.go

build: build-binary
	doo b

build-binary:
	docker run --rm \
		--name sh-bot-build \
		-v $$(pwd)/..:/go/src/github.com/thbkrkr/qli \
		-e GOBIN=/go/bin/ -e CGO_ENABLED=0 -e GOPATH=/go \
		-w /go/src/github.com/thbkrkr/qli/sh-bot \
			golang:1.7.4 \
				go build

drun:
	doo d \
		-v /cmd:/cmd \
		-e B=${B} -e K=${K} -e T=${T} \
		krkr/sh-bot
