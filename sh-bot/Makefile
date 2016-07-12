dev:
	go run main.go -n devsh1337

build: build-binary
	doo b

build-binary:
	docker run --rm \
		--name sh-bot-build \
		-v $$(pwd)/../..:/go/src/github.com/thbkrkr/qli \
		-e GOBIN=/go/bin/ \
		-e CGO_ENABLED=0 \
		-e GOPATH=/go \
		-w /go/src/github.com/thbkrkr/qli/example/sh-bot \
			golang:1.6.2 go build

drun:
	doo b \
		-e B=${B} -e K=${K} -e ${T}
		-p 80:4242 \
		krkr/sh-bot
