using System.Text;
using System.Text.Json;
using Rebus.Messages;
using Rebus.Serialization;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// The default serializer used by the round-trip check: UTF-8 <c>System.Text.Json</c>, with the message type
/// carried in the <c>rbs2-msg-type</c> header.
/// <para>
/// This exists rather than reusing Rebus's own <c>SystemTextJsonSerializer</c> because that type is internal
/// to the Rebus assembly. Owning it here is also the better default: the byte output of this serializer is
/// what the check compares, so it should be pinned and explicit.
/// </para>
/// </summary>
public sealed class SystemTextJsonContractSerializer : ISerializer
{
    static readonly JsonSerializerOptions DefaultOptions = new()
    {
        AllowTrailingCommas = true,
        ReadCommentHandling = JsonCommentHandling.Skip
    };

    readonly IMessageTypeNameConvention _convention;
    readonly JsonSerializerOptions _options;

    /// <summary>
    /// Creates the serializer.
    /// </summary>
    /// <param name="convention">
    /// How type names are written to and read from the type header. Defaults to the assembly-qualified name.
    /// </param>
    /// <param name="options">JSON options. Defaults to trailing commas and comments allowed.</param>
    public SystemTextJsonContractSerializer(
        IMessageTypeNameConvention? convention = null, JsonSerializerOptions? options = null)
    {
        _convention = convention ?? new AssemblyQualifiedTypeNameConvention();
        _options = options ?? DefaultOptions;
    }

    /// <inheritdoc />
    public Task<TransportMessage> Serialize(Message message)
    {
        if (message == null) throw new ArgumentNullException(nameof(message));

        var headers = new Dictionary<string, string>(message.Headers);
        var body = message.Body;

        headers[Headers.ContentType] = "application/json;charset=utf-8";

        if (!headers.ContainsKey(Headers.Type))
        {
            headers[Headers.Type] = _convention.GetTypeName(body.GetType());
        }

        var json = JsonSerializer.Serialize(body, body.GetType(), _options);

        return Task.FromResult(new TransportMessage(headers, Encoding.UTF8.GetBytes(json)));
    }

    /// <inheritdoc />
    public Task<Message> Deserialize(TransportMessage transportMessage)
    {
        if (transportMessage == null) throw new ArgumentNullException(nameof(transportMessage));

        if (!transportMessage.Headers.TryGetValue(Headers.Type, out var typeName))
        {
            throw new InvalidOperationException(
                $"The transport message carries no '{Headers.Type}' header, so its type is unknown.");
        }

        var type = _convention.GetType(typeName)
                   ?? throw new InvalidOperationException($"Could not resolve the message type '{typeName}'.");

        var json = Encoding.UTF8.GetString(transportMessage.Body);
        var body = JsonSerializer.Deserialize(json, type, _options)
                   ?? throw new InvalidOperationException($"Deserializing a '{typeName}' produced null.");

        return Task.FromResult(new Message(new Dictionary<string, string>(transportMessage.Headers), body));
    }

    sealed class AssemblyQualifiedTypeNameConvention : IMessageTypeNameConvention
    {
        public string GetTypeName(Type type) => type.AssemblyQualifiedName ?? type.FullName ?? type.Name;

        public Type GetType(string name) => Type.GetType(name, throwOnError: true)!;
    }
}
