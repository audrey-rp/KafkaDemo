using System.Text.Json;
using Confluent.Kafka;

var topic = args.FirstOrDefault(a => a.StartsWith("--topic="))?.Split('=', 2)[1] ?? "demo-topic";
var countArg = args.FirstOrDefault(a => a.StartsWith("--count="))?.Split('=', 2)[1];
var keyArg = args.FirstOrDefault(a => a.StartsWith("--key="))?.Split('=', 2)[1];

const string bootstrapServers = "localhost:9092";

var config = new ProducerConfig
{
    BootstrapServers = bootstrapServers
};

using var producer = new ProducerBuilder<string, string>(config).Build();

if (int.TryParse(countArg, out var count))
{
    Console.WriteLine($"Producing {count} messages to topic '{topic}'{(string.IsNullOrWhiteSpace(keyArg) ? "" : $" with key '{keyArg}'")}...\n");

    for (var i = 1; i <= count; i++)
    {
        var message = new
        {
            eventType = "ContentDiscovered",
            contentId = i.ToString(),
            timestamp = DateTime.UtcNow
        };

        var json = JsonSerializer.Serialize(message);

        var result = await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = string.IsNullOrWhiteSpace(keyArg) ? message.contentId : keyArg,
            Value = json
        });

        Console.WriteLine($"[{i}/{count}] Delivered to {result.TopicPartitionOffset}");
    }

    Console.WriteLine($"\nAll {count} messages produced successfully.");
}
else
{
    Console.WriteLine(
        $"Kafka Producer started. Topic: '{topic}'{(string.IsNullOrWhiteSpace(keyArg) ? "" : $", Key override: '{keyArg}'")}. Press Enter to send a message, or 'q' to quit.");

    var messageCount = 0;

    while (true)
    {
        Console.Write("\n> ");
        var input = Console.ReadLine();

        if (string.Equals(input, "q", StringComparison.OrdinalIgnoreCase))
            break;

        messageCount++;

        var message = new
        {
            eventType = "ContentDiscovered",
            contentId = messageCount.ToString(),
            timestamp = DateTime.UtcNow
        };

        var json = JsonSerializer.Serialize(message);

        var result = await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = string.IsNullOrWhiteSpace(keyArg) ? message.contentId : keyArg,
            Value = json
        });

        Console.WriteLine($"Delivered to {result.TopicPartitionOffset}");
        Console.WriteLine($"   Payload: {json}");
    }

    Console.WriteLine("Producer shutting down...");
}
