using Confluent.Kafka;

var topic = args.FirstOrDefault(a => a.StartsWith("--topic="))?.Split('=', 2)[1] ?? "demo-topic";
var groupId = args.FirstOrDefault(a => a.StartsWith("--group="))?.Split('=', 2)[1] ?? "demo-consumer-group";
var delayArg = args.FirstOrDefault(a => a.StartsWith("--delay-ms="))?.Split('=', 2)[1];
var processingDelayMs = int.TryParse(delayArg, out var parsedDelay) && parsedDelay > 0 ? parsedDelay : 0;
var crashAfterArg = args.FirstOrDefault(a => a.StartsWith("--crash-after="))?.Split('=', 2)[1];
var crashAfterMessages = int.TryParse(crashAfterArg, out var parsedCrashAfter) && parsedCrashAfter > 0 ? parsedCrashAfter : 0;

const string bootstrapServers = "localhost:9092";

var config = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = groupId,
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();

consumer.Subscribe(topic);

Console.WriteLine(
    $"Kafka Consumer started. Topic: '{topic}', Group: '{groupId}'{(processingDelayMs > 0 ? $", Processing delay: {processingDelayMs}ms" : "")}{(crashAfterMessages > 0 ? $", Crash after: {crashAfterMessages} message(s)" : "")}. Listening for messages... (Ctrl+C to stop)\n");

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

        if (processingDelayMs > 0)
        {
            // Simulate slow downstream processing for lag/backpressure demos.
            Thread.Sleep(processingDelayMs);
        }

        if (crashAfterMessages > 0 && count >= crashAfterMessages)
        {
            Environment.FailFast($"Intentional crash for demo after {count} message(s).");
        }
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
