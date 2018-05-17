
dev:
	go run *.go

all: build push

build:
	docker build --rm \
		--label SHA1=$$(git rev-parse --short HEAD) \
		-t krkr/qws -f Dockerfile ..

push:
	docker push krkr/qws

#######

test-qws:
	docker run --rm -p 80:4242 \
		-e B=${B} -e U=${U} -e P=${P} -e T=${T} \
		krkr/qws

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

