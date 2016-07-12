FROM alpine:3.3

RUN apk --update add bash curl && \
    rm -f /var/cache/apk/*

COPY sh-bot /sh-bot

ENTRYPOINT ["/sh-bot"]