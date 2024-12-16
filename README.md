# NATS App

[![Build](https://github.com/centum/nats_app/actions/workflows/build.yml/badge.svg)](https://github.com/centum/nats_app/actions/workflows/build.yml)
[![Apache 2 licensed](https://img.shields.io/badge/license-Apache2-blue.svg)](https://raw.githubusercontent.com/centum/nats_app/refs/heads/master/LICENSE)

NATS App is wrapper application on NATS Connection

### Create NATS Application

```python
NATS_URL = "nats://localhost:4222"

nc = NATSApp(NATS_URL)
await nc.connect()
```

### Add RPC Handler

```python
@nc.push_subscribe("app.test.echo", queue="worker")
async def rpc_func_echo(msg: Any) -> str:
    return f"Response with Msg.data: {msg.data}"
```

### Call RPC
```python
res = await nc.request("app.test.echo", {"args": ["test"]})
print(res)
```

### JetStream push subscription

```python
@nc.js_push_subscribe("app.test.js.subs", queue="worker")
async def handler(msg: Msg):
    print(msg.data)
    await msg.ack()
```

### JetStream pull subscription

```python
@nc.js_pull_subscribe("app.subject.js.subs", batch=1)
async def handler(msgs: list[Msg]):
    for m in msgs:
        print(m)
        await m.ack()
```

### JetStream publish

```python
await nc.js.publish("app.subject.js.subs", b"TEST123")
```

