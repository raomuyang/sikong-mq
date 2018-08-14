# SIKONG-MQ

[![Build Status](https://travis-ci.org/raomuyang/sikong-mq.svg?branch=master)](https://travis-ci.org/raomuyang/sikong-mq) 
[![Go Report Card](https://goreportcard.com/badge/github.com/raomuyang/sikong-mq)](https://goreportcard.com/report/github.com/raomuyang/sikong-mq)
[![GoDoc](https://godoc.org/github.com/raomuyang/sikong-mq?status.svg)](https://godoc.org/github.com/raomuyang/sikong-mq)

> A message queue through the channel connected sender and receiver

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


## Transfer Protocol

The structure of message:
* Message
	* MsgId   string
	* AppID   string
	* Type    string
	* Content []byte

SKMQ's service is based on the TCP protocol, which uses structured data to communication. 
The rules for message encoding are similar to those of the HTTP service - Use  `\r\n`  to 
separate a parameter, the  key and value of the parameter are separated by `=`,  
use  `\r\n\r\n`  to separate a message, in this way to solve the TCP sticky problem.

The list of request parameters for SKMQ is as follows:

| Parameter | Type | Description |
| ------ | ------ | ------------- |
| appid | string | The ID of the application, SKMQ will deliver the message to different consumers based on the application ID |
| msgid | string | Message ID, must be uniqueness, SKMQ will filter duplicate messages |
| type | string | The message type - send by producer, delivers by MQ, feedback of consumer |
| content | byte | message content, SKMQ will forward it to the appropriate consumer |


### msg `type`

* `topic` -  The type of message the producer wants to send - broadcast by applicationID
* `queue` -  The type of message to send in production - p2p by applicationID

* `push`  -  The type of message pushed to consumer

* `resp` - All return message types of SKMQ are `resp`

the `content` of message is json text, 
the `status` is included in the return content
```json
      {
        "status": "ack | reject | error",
        "content": "response msg"
      }
```

* `arrived` - The consumer must first send an `arrived` to the MQ when it receives a message, 
and then sends an ack message while ensuring that the message have landed

* `ack` - Acknowledge
* `reject` - Response `reject` when the consumer can not process
* `error` - Message processing failed sign

* `register` - Register recipient (consumer)

At this time the content of the message should be legal json text, 
otherwise can not complete the registration.
```json
    {
        "id": "recipient id",
        "app_id": "application id",
        "host": "",
        "port":"",
        "weight": 0
    }
```

### Message delivery and receiving

* Message delivery
The `type` of message is `push` while message delivery, consumer nodes need to maintain an 
open listening port, ready to wait for message from SKMQ.
Consumer registration, producer push messages, message delivery, consumer response and other message content, 
are all based on the same encoding rules described above.

* ack
When a message arrives at a consumer node, the consumer sends a `arrived` type message 
telling MQ that the message has arrived and MQ will wait for a while, during this time, 
if the consumer completes the task processing soon, it can immediately send an ack response 
to inform  the MQ that message has been processed. If it is not completed,
MQ will close the connection, consumers can then reconnect and send ack message to inform MQ 
message has been completed


```
delivery: MQ -> msgid=id_xxx\r\ntype=push\r\ncontent=bytes_xxx\r\n\r\n -> Consumer

process1:
    MQ -> wait -> Consumer -> process -> Consumer -> msgid=id_xxx\r\ntype=ack\r\n\r\n -> MQ
process2: 
    1) Consumer -> process;  MQ -> connection closed; 
    2) Consumer ->  msgid=id_xxx\r\ntype=ack\r\n\r\n -> MQ

```


### PING/PONG

In order to ensure the stability of message delivery, MQ periodically monitors the 
consumer nodes and checks whether the nodes are out of association with the heartbeat. 
Therefore, the message node needs to open the listening port for receiving and feedback 
Heartbeat package: just return a `pong\r\n\r\n` package when it receives data that is`ping\r\n\r\n`
```go
    connect.SetWriteDeadline(time.Now().Add(ConnectTimeOut))
	err := SendMessage(connect, []byte(PING))
	if err != nil {
		return false
	}

	buf := make([]byte, 10)
	connect.SetReadDeadline(time.Now().Add(ConnectTimeOut))
	read, err := connect.Read(buf)
```


## Requirements
* Redis   The message cache dependence a redis server


## Launch

> launch service by sqmq launcher: sikong `--conf` /path/to/conf.json `--dbconf` path/to/dbconf.json

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

---

---
# SKMQ

SKMQ的设计基于构建在golang的channel和goroutine的优势上

当连接建立时，客户端发送过来的字节流被解码成消息实体，通过通道传递给正在等待的其它handler处理，
其它的handler处理完成之后，又通过通道传递给下一个handler，这个过程中消息会经历编码、缓存、进入队列、
投递给消费者等一系列过程。


## 实现

* 消息排队、消息投递
* 消费者的负载均衡
* 流量控制


## 传输协议

SKMQ的服务基于TCP协议，它使用结构化的数据进行通讯。消息编码的规则类似HTTP服务的响应头，使用两个换行符
`\r\n`间隔一条参数，参数的key和value之间使用`=`间隔，使用四个换行`\r\n\r\n`符间隔一段消息，通过这种
方式解决TCP的粘包问题。

SKMQ的请求参数列表如下：

| 参数    |  类型   | 描述 |
| ------ | ------ | ------------- |
| appid  | string  | 应用的ID，SKMQ会根据应用ID将消息投递给不同的消费者 |
| msgid  |  string | 消息ID，必须保证ID的唯一性，SKMQ会过滤重复的消息 |
| type   |  string | 消息类型，生产者发送消息、消息队列投递消息、消费者反馈都会附带相应的消息类型 |
|content |  byte    | 消息内容，SKMQ会将它转发给相应的消费者 |


### msg `type`

* topic  生产者要发送的消息类型 - 应用内广播
* queue  生产中要发送的消息类型 - 点对点单播

* push SKMQ推送消息时的默认类型

* resp SKMQ所有的返回信息类型均为`resp`，此时的返回内容为json text，状态包含在返回内容中
```json
      {
        "Status": "ack | reject | error",
        "Content": "response msg"
      }
```

* arrived  MQ投递消息时，消费者在接收到消息时需先发送arrived类型的消息，在确保消息落地时才发送ack消息
* ack    对消息队列的正常响应信号
* reject 当消费者无法消费时，发送一个reject消息给消息队列
* error  消息处理失败的标志

* register 注册收件人（消费者），此时消息的content应该为合法的json text，否则无法完成注册。
```json
    {
        "id": "recipient id",
        "app_id": "application id",
        "host": "",
        "port":"",
        "weight": 0
    }
```

### Message delivery and receiving

* Message delivery
消息投递时，消息类型`type`为push，消费者节点需要维持一个开放的侦听端口，随时等候MQ的消息投递。
消费者注册、生产者推送消息、消息投递、消费者响应等消息内容，全都是以上述相同的内容编码规则进行传递。

* ack
消息到达消费者节点时，消费者先发送一个`arrived`类型的消息告知MQ信件已经到达，MQ会等待一段时间，
在这段时间内，如果消费者很快完成任务处理，可以立即发送ack响应告知MQ消息已经处理完成；若未完成，
MQ会关闭连接，消费者可以随后主动发送ack消息告知MQ消息已处理完成

```
delivery: MQ -> msgid=id_xxx\r\ntype=push\r\ncontent=bytes_xxx\r\n\r\n -> Consumer

process1:
    MQ -> wait -> Consumer -> process -> Consumer -> msgid=id_xxx\r\ntype=ack\r\n\r\n -> MQ
process2:
    1) Consumer -> process;  MQ -> connection closed;
    2) Consumer ->  msgid=id_xxx\r\ntype=ack\r\n\r\n -> MQ

```


### PING/PONG

为了消息投递的稳定性，MQ会定期监测消费者节点，通过心跳包检查节点是否失联并及时将其标记，不参与下次
消息接收，所以消息节点需要开放侦听端口，用于接收和反馈心跳包: 只需在收到内容为 `ping\r\n\r\n`的数据时，
返回一个`pong\r\n\r\n`的数据包
```go
    connect.SetWriteDeadline(time.Now().Add(ConnectTimeOut))
	err := SendMessage(connect, []byte(PING))
	if err != nil {
		return false
	}

	buf := make([]byte, 10)
	connect.SetReadDeadline(time.Now().Add(ConnectTimeOut))
	read, err := connect.Read(buf)
```

## 服务部署与使用

* redis

消息的缓存使用了redis，在服务启动前，需配置好可用的redis服务

* 配置

消息队列默认配置如下

conf.json

```json
{
  "retry_times": 5,
  "ack_timeout": 60000,
  "rate": 1000,
  "listener_host": "127.0.0.1",
  "listener_port": "1734"
}
```

Redis的默认配置如下

dbconf.json

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

sikong启动时会默认读取同级目录下的conf.json和dbconf.json，可以通过`--conf`和`--dbconf`分别指定
消息队列配置和redis配置


