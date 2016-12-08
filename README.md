# Qli

Some Kafka experiments.

API, bot and oq use env vars:

```
export B=broker.com:9092
export K=specialclientid
export T=topic
```

### qli/client

Go client to produce and consume.

Create a new client.

```
qli, err := client.NewClient(brokers, key, topic, name)
```

Create a new client using the env vars B, K and T.

```
qli, err := client.NewClientFromEnv()
```

Produce:

```
qli.Send(msg)
```

Consume:
```
for msg range qli.Sub() {
  // Do something with msg
}

```

### qli/api

HTTP API to produce and consume data.

```
curl -XPOST -H "X-Auth-Key: ${KEY}" \
  localhost:4242/produce/topic/${TOPIC} \
  -d '{"message": "'$(date +%s)'"}'

curl -H "X-Auth-Key: ${KEY}" \
  localhost:4242/consume/topic/${TOPIC}
```

### qli/bot

Bot to register command, scripts ou go funcs to be executed when
a message contains a keyword.

```
bot.NewBot(fmt.Sprintf("koko-%s-bot", hostname)).
  RegisterScript("bam", "scripts/bam.sh").
  RegisterCmdFunc("ping", func(args ...string) string {
    return "pong"
  }).
  Start()
```

### qli/oq

Simple cli to produce and consume data.

Consume:
```
oq
```

Produce:
```
echo '{"date": "$(date %s)", "message": "'$RANDOM'"}' | \
  oq
```
