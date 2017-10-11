FROM alpine:3.6

RUN apk --no-cache add bash curl jq

COPY sh-bot /sh-bot

ENTRYPOINT ["/sh-bot"]