FROM golang:1.10-alpine3.7 as gobuilder
COPY vendor /go/src/github.com/thbkrkr/qli/vendor
COPY client /go/src/github.com/thbkrkr/qli/client
COPY qws 	/go/src/github.com/thbkrkr/qli/qws
RUN  cd 	/go/src/github.com/thbkrkr/qli/qws && \
	GOBIN=/go/bin/ GOPATH=/go CGO_ENABLED=0 go build

FROM alpine:3.7
RUN apk --no-cache add ca-certificates
COPY --from=gobuilder /go/src/github.com/thbkrkr/qli/qws/qws /qws
COPY qws/_static /_static
ENTRYPOINT ["/qws"]