# Kafka Demo

A hands-on set of demos for learning Apache Kafka with .NET.

## Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

---

## Project Structure

Both demos use a shared Producer and Consumer located in `src/Common/`:

- `KafkaDemo.Producer` -- sends messages to a Kafka topic. Supports interactive mode (press Enter to send) or batch mode (specify `--count=N`).
- `KafkaDemo.Consumer` -- reads messages from a Kafka topic and prints them.

Both accept command-line arguments:

| Argument | Default | Description |
|----------|---------|-------------|
| `--topic=<name>` | `content-events` | The Kafka topic to produce to or consume from |
| `--group=<name>` | `demo-consumer-group` | (Consumer only) The consumer group ID |
| `--count=<n>` | _(interactive)_ | (Producer only) Send N messages in batch mode |

---

## Demo 1: "Hello Kafka" -- Producer to Consumer

### What it teaches

- Kafka is not magic -- it is just writing to a topic and reading from it
- Real-time streaming feel
- Messages are stored, not just delivered

### Running the demo

#### 1. Start Kafka

From the repository root:

```bash
docker compose up -d
```

This starts a single-node Kafka broker on `localhost:9092`.

#### 2. Start the Consumer

In a terminal:

```bash
cd src/Common/KafkaDemo.Consumer
dotnet run
```

You should see:

```
Kafka Consumer started. Topic: 'content-events', Group: 'demo-consumer-group'. Listening for messages... (Ctrl+C to stop)
```

#### 3. Start the Producer

In a separate terminal:

```bash
cd src/Common/KafkaDemo.Producer
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

Press `q` to quit the producer.

### Interactive exercise

Ask yourself: "If I stop the consumer, what happens to messages?"

1. Stop the consumer (Ctrl+C)
2. Send several messages from the producer
3. Restart the consumer (`dotnet run`)

**Key insight:** The messages appear -- Kafka stored them on the broker. Messages are persisted to the topic and replayed from the last committed offset when the consumer reconnects.

---

## Demo 2: Persistence + Replay

### What it teaches

- Kafka is a log, not a queue -- messages are not deleted after consumption
- Consumers track their position (offset), not the broker deleting data
- You can reprocess the entire history at any time

### Running the demo

#### 1. Start Kafka

If not already running:

```bash
docker compose up -d
```

#### 2. Produce 10 messages

```bash
cd src/Common/KafkaDemo.Producer
dotnet run -- --topic=persistence-demo --count=10
```

You should see 10 messages delivered to the `persistence-demo` topic.

#### 3. Start the Consumer

```bash
cd src/Common/KafkaDemo.Consumer
dotnet run -- --topic=persistence-demo --group=persistence-demo-group
```

The consumer reads all 10 messages and prints them with their offsets. Press Ctrl+C to stop.

#### 4. Restart the Consumer

Run the consumer again:

```bash
dotnet run -- --topic=persistence-demo --group=persistence-demo-group
```

Notice that no messages appear -- the consumer group has already committed its offsets past the end of the topic.

#### 5. Reset offsets and replay everything

Stop the consumer, then reset the consumer group offsets to the beginning:

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group persistence-demo-group \
  --topic persistence-demo \
  --reset-offsets \
  --to-earliest \
  --execute
```

Now start the consumer again:

```bash
cd src/Common/KafkaDemo.Consumer
dotnet run -- --topic=persistence-demo --group=persistence-demo-group
```

All 10 messages are replayed from the beginning.

### Interactive exercise

Ask yourself: "Why is this useful?"

Expected answers:

- **Debugging** -- replay events to reproduce issues
- **Rebuilding state** -- reconstruct a materialized view or database from the event log
- **New services** -- a service deployed after the fact can catch up on historical events

### Key insight

Kafka lets you reprocess history. Unlike traditional message queues where messages are deleted after delivery, Kafka retains messages for a configurable period. Consumers simply move a pointer (offset) forward through the log, and that pointer can be reset at any time.

---

## Shutting down

```bash
docker compose down
```