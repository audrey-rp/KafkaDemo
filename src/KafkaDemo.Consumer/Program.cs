using Confluent.Kafka;

var topic = args.FirstOrDefault(a => a.StartsWith("--topic="))?.Split('=', 2)[1] ?? "content-events";
var groupId = args.FirstOrDefault(a => a.StartsWith("--group="))?.Split('=', 2)[1] ?? "demo-consumer-group";

const string bootstrapServers = "localhost:9092";

var config = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = groupId,
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();

consumer.Subscribe(topic);

Console.WriteLine($"Kafka Consumer started. Topic: '{topic}', Group: '{groupId}'. Listening for messages... (Ctrl+C to stop)\n");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var count = 0;

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        var result = consumer.Consume(cts.Token);
        count++;

        Console.WriteLine($"[{count}] Offset {result.Offset.Value}, Partition {result.Partition.Value}");
        Console.WriteLine($"      Key:   {result.Message.Key}");
        Console.WriteLine($"      Value: {result.Message.Value}");
        Console.WriteLine();
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine($"\nConsumer shutting down. Total messages read: {count}");
}
finally
{
    consumer.Close();
}
