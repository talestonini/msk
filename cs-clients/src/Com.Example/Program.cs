namespace Com.Example;

/// <summary>
/// Entry point: <c>dotnet run -- produce [config-file] [event-count]</c>
/// or <c>dotnet run -- consume [config-file]</c>.
/// </summary>
public static class Program
{
    public static int Main(string[] args)
    {
        var rest = args.Skip(1).ToArray();
        switch (args.FirstOrDefault())
        {
            case "produce":
                return TemperatureProducer.Run(rest);
            case "consume":
                return TemperatureConsumer.Run(rest);
            default:
                Console.Error.WriteLine("Usage: Com.Example produce [config-file] [event-count]");
                Console.Error.WriteLine("       Com.Example consume [config-file]");
                return 2;
        }
    }
}
