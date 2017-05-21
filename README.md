# Qli

[![Build Status](https://travis-ci.org/thbkrkr/qli.svg)](https://travis-ci.org/thbkrkr/qli)

Some Kafka experiments.

Requires configuring the env vars:

```sh
export B=k4fk4.io:9093
export T=topic
export U=user
export P=password
```

### qli/oq

Simple CLI to produce and consume data.

Produce:
```sh
> fortune -s | oq
```

Consume:
```
> oq
Today is what happened to yesterday.
```

### qli/client

A Go client to simply produce and consume.

```go
// Create client
q, err := client.NewClientFromEnv(client)

// Produce one message synchronously
partition, offset, err := q.Send(data)

// Produce messages asynchronously
pub, err := q.Pub()
pub, err := q.PubOn(topic)

for stdin.Scan() {
  pub <- stdin.Bytes()
}

// Consume messages asynchronously
sub, err := q.Sub()
sub, err := q.SubOn(topic)

for msg := range sub {
  fmt.Println(string(msg))
}
```

### qli/qws

Produce or consume in Kafka over WebSockets.

### qli/bot

Register scripts or go funcs to be executed when a message contains a string.

```go
bot.NewBot(fmt.Sprintf("koko-%s-bot", hostname)).
  RegisterScript("bam", "scripts/bam.sh").
  RegisterCmdFunc("ping", func(args ...string) string {
    return "pong"
  }).
  Start()
```

