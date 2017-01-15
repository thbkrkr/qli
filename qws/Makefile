
dev:
	go run api.go

build: build-binary
	doo b

build-binary:
	docker run --rm \
		-v $$(pwd)/..:/go/src/github.com/thbkrkr/qli \
		-e GOBIN=/go/bin/ \
		-e CGO_ENABLED=0 \
		-e GOPATH=/go \
		-w /go/src/github.com/thbkrkr/qli/qws \
			golang:1.7.4 \
				sh -c "go build -o qws"

run:
	docker run --rm -p 80:4242 krkr/miaou \
		-k ${KEY} -t ${TOPIC}

test-send:
	curl -XPOST localhost:4242/send -d '{"user": "matt-3", "message": "date"}'

test-curl-produce:
	curl -XPOST -H "X-Auth-Key: ${KEY}" \
		localhost:4242/produce/topic/${TOPIC} \
		-d '{"user": "matt-3", "message": "'$$(date +%s)'"}'

test-ab-produce:
	echo -n '{"user": "matt-3", "message": "'$$(date +%s)'"}' > data
	ab -c 500 -n 8000 -H "X-Auth-Key: ${KEY}" \
		-p data "http://localhost:4242/produce/topic/${TOPIC}"
	rm data