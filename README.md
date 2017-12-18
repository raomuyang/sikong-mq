# SIKONG-MQ

> A message queue through the pipe connected sender and receiver

This message queue designed based on the advantage of golang channel and goroutine 

While a connection accepted, the byte stream will decode to message and transfer 
from channel to channel, a process by a handler and send to next handler (via channel). 
The whole experience of a message includes encoding, decode, store, enqueue and wait to 
send, etc.

![message process](https://raw.githubusercontent.com/raomuyang/sikong-mq/master/message-process.png)


## Availability

* message queuing and delivery
* consumer register and load balance
* rate limiter (token bucket)

## Requirements
* Redis   The message cache dependence a redis server


## Launch

> launcher: mqlauncher.go

The launcher must load mq config and db config while message queue launch

* mq config 

```json
{
  "retry_times": 5,
  "ack_timeout": 60000,
  "rate": 1000,
  "listener_host": "127.0.0.1",
  "listener_port": "1734"
}
```

* redis config

```json
{
  "address": "127.0.0.1:6379",
  "password": "",
  "max_idle": 0,
  "max_active": 0,
  "idle_timeout": 0,
  "wait": false,
  "db": 1,
  "read_timeout": 0,
  "write_timeout": 0,
  "dial_connect_timeout": 0
}
```
