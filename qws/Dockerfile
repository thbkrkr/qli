FROM alpine:3.6

RUN apk --no-cache add ca-certificates

COPY qws /qws
COPY _static /_static

ENTRYPOINT ["/qws"]