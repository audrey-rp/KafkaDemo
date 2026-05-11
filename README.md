# Kafka Demo

A hands-on set of demos for learning Apache Kafka with .NET.

## Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

---

## Project Structure

Both demos use a shared Producer and Consumer located in `src/`:

- `KafkaDemo.Producer` -- sends messages to a Kafka topic. Supports interactive mode (press Enter to send) or batch mode (specify `--count=N`).
- `KafkaDemo.Consumer` -- reads messages from a Kafka topic and prints them.

Both accept command-line arguments:

| Argument | Default | Description |
|----------|---------|-------------|
| `--topic=<name>` | `rpu-topic` | The Kafka topic to produce to or consume from |
| `--group=<name>` | `demo-consumer-group` | (Consumer only) The consumer group ID |
| `--delay-ms=<n>` | `0` | (Consumer only) Simulated processing delay per message |
| `--count=<n>` | _(interactive)_ | (Producer only) Send N messages in batch mode |
| `--key=<value>` | _(none)_ | (Producer only) Optional fixed key for all produced messages |


## Setup: Create Topics

All demos use the same Kafka topic: `rpu-topic` (with 3 partitions to support scaling and parallelism demos).

Create it using the provided setup script:

```powershell
.\setup-topics.ps1
```

This will create the `rpu-topic` with 3 partitions and 1 replication factor.

To clear all messages between demos, reset the topic (delete + recreate):

```powershell
.\setup-topics.ps1 -Reset
```

---

## Demo 1: Producer to Consumer

#### 1. Start Kafka

From the repository root in either the VS Code terminal or a PowerShell session:

```powershell
docker compose up -d
```

This starts a single-node Kafka broker on `localhost:9092`.

#### 2. Start the Consumer

In a terminal:

```powershell
cd src/KafkaDemo.Consumer
dotnet run
```

You should see:

```
Kafka Consumer started. Topic: 'rpu-topic', Group: 'demo-consumer-group'. Listening for messages... (Ctrl+C to stop)
```
(or whatever your chosen topic name and consumer group name are, these are just the defaults)

#### 3. Start the Producer

In a separate terminal:

```powershell
cd src/KafkaDemo.Producer
dotnet run
```

Press Enter to send a message. The producer sends events like:

```json
{
  "eventType": "ContentDiscovered",
  "contentId": "1",
  "timestamp": "2025-01-01T00:00:00Z"
}
```

The consumer prints the message immediately.

Press `q` to quit the producer. Shut down the consumer with the command `Ctrl+C`

---

## Demo 2: Persistence and Replay among Consumer Groups

### Running the demo

#### 1. Start Kafka,

If not already running:

```powershell
docker compose up -d
```

#### 2. Produce 10 messages

```powershell
cd src/KafkaDemo.Producer
dotnet run -- --count=10
```

You should see 10 messages delivered to the `rpu-topic` topic.

#### 3. Start the Consumer

```powershell
dotnet run -- --group=persistence-demo-group
```

The consumer reads all 10 messages and prints them with their offsets. Press Ctrl+C to stop.

#### 4. Restart the Consumer

Run the consumer again:

```powershell
dotnet run -- --group=persistence-demo-group
```

What happens when this consumer runs?

#### 5. Reset offsets and replay everything

Stop the consumer, then reset the consumer group offsets to the beginning for the consumer group:

```powershell
docker exec kafka kafka-consumer-groups `
  --bootstrap-server localhost:9092 `
  --group persistence-demo-group `
  --topic rpu-topic `
  --reset-offsets `
  --to-earliest `
  --execute
```

Now start the consumer again with your consumer group:

```powershell
cd src/KafkaDemo.Consumer
dotnet run -- --group=persistence-demo-group
```

What happens now with the consumer group that you had previously consumed all of the messages with?

---

## Demo 3: More Consumer Groups (Parallelism and Horizontal Scaling)

### Running the demo

#### 1. Start Kafka

If not already running:

```powershell
docker compose up -d
```

#### 2. Start Consumer 1 in Group A

In Terminal 1:

```powershell
cd src/KafkaDemo.Consumer
dotnet run -- --group=scaling-demo-group-a
```

Watch the output. Notice which partition(s) this consumer is reading from.

#### 3. Start Consumer 2 in the Same Group

In Terminal 2, while Consumer 1 is still running:

```powershell
cd src/KafkaDemo.Consumer
dotnet run -- --group=scaling-demo-group-a
```

What happens? What can you see in the Partition field in the output?

#### 4. Start Consumer 3 in the Same Group

In Terminal 3, while Consumers 1 and 2 are still running:

```powershell
cd src/KafkaDemo.Consumer
dotnet run -- --group=scaling-demo-group-a
```

What happens now? Why would you possibly want this?

#### 5. Produce 30 messages to the topic

```powershell
cd src/KafkaDemo.Producer
dotnet run -- --count=30
```

This sends 30 messages to the `rpu-topic` topic with 3 partitions.

#### 6. Add a Consumer from a Different Group

In Terminal 4:

```powershell
cd src/KafkaDemo.Consumer
dotnet run -- --group=scaling-demo-group-b
```

What happens to this consumer? Which messages does it receive? What is a scenario in which you would want this behaviour?

#### 7. Produce more messages and observe

In a 5th terminal, produce 10 more messages:

```powershell
cd src/KafkaDemo.Producer
dotnet run -- --count=10
```

What happens to each of the consumers? 

---

## Demo 4: Partitioning and Ordering

#### 1. Start Kafka

If not already running:

```powershell
docker compose up -d
```

#### 2. Start one consumer to observe order and partitions

In Terminal 1:

If you do not currently have a consumer running, start up a new one. You can also re-use one of the consumers from the last demo. 

```powershell
cd src/KafkaDemo.Consumer
dotnet run -- --group=ordering-demo-group
```

Watch the output fields:

- `Partition`
- `Offset`
- `Key`

#### 3. Case A: Same key

Produce 10 events using the same key:

```powershell
cd src/KafkaDemo.Producer
dotnet run -- --count=10 --key=content-123
```

Where do these messages go? Why do you think that is?

#### 4. Case B: Different keys

Produce 10 more events with default behavior (these will then have different keys via different `contentId` values):

```powershell
cd src/KafkaDemo.Producer
dotnet run -- --count=10
```

What happens to these messages? What do you see in each partition? Why do you think that is?

---

## Demo 5: Consumer Lag

#### 1. Start Kafka

If not already running:

```powershell
docker compose up -d
```

#### 2. Start a slow consumer

In Terminal 1:

```powershell
cd src/KafkaDemo.Consumer
dotnet run -- --group=lag-demo-group --delay-ms=300
```

This consumer intentionally sleeps 300ms after each message to simulate a slow downstream dependency.

#### 3. Produce quickly to create pressure

In Terminal 2:

```powershell
cd src/KafkaDemo.Producer
dotnet run -- --count=500
```

The producer sends quickly. The consumer cannot keep up, so backlog accumulates.
Why is the producer so fast? Besides the fact we put in a sleep time for the consumer

#### 4. Observe consumer lag increasing

In Terminal 3, run this repeatedly while producer/consumer are active:

```powershell
docker exec kafka kafka-consumer-groups `
  --bootstrap-server localhost:9092 `
  --describe `
  --group lag-demo-group
```

Look at the `LAG` column per partition. What is happening inside the consumer group?

#### 5. Stop producing and watch recovery

Once the producer finishes, keep the slow consumer running and continue checking lag.

What happens to lag over time? Why might we desire this behaviour?

---

## Shutting down

```powershell
docker compose down
```