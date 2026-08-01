using System.Collections.Immutable;
using System.Reflection;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Builds a contract instance from its greediest public constructor, filled with deterministic,
/// type-derived, non-default values, recursively. No AutoFixture: determinism matters more than variety,
/// because a flaky serialization test gets deleted (design §10).
/// </summary>
public sealed class DefaultMessageInstanceFactory : IMessageInstanceSource
{
    const int MaxDepth = 5;

    /// <inheritdoc />
    public bool TryCreate(Type messageType, out object? instance)
    {
        if (messageType == null) throw new ArgumentNullException(nameof(messageType));

        try
        {
            instance = Create(messageType, Seed(messageType.FullName ?? messageType.Name), depth: 0);
            return instance != null;
        }
        catch (Exception)
        {
            instance = null;
            return false;
        }
    }

    static object? Create(Type type, uint seed, int depth)
    {
        var underlying = Nullable.GetUnderlyingType(type);
        if (underlying != null) return Create(underlying, seed, depth);

        if (type == typeof(string)) return $"value-{seed % 100000}";
        if (type == typeof(bool)) return true;
        if (type == typeof(byte)) return (byte)(seed % 200 + 1);
        if (type == typeof(sbyte)) return (sbyte)(seed % 100 + 1);
        if (type == typeof(short)) return (short)(seed % 30000 + 1);
        if (type == typeof(ushort)) return (ushort)(seed % 60000 + 1);
        if (type == typeof(int)) return (int)(seed % 1000000 + 1);
        if (type == typeof(uint)) return seed % 1000000 + 1;
        if (type == typeof(long)) return (long)(seed % 1000000 + 1);
        if (type == typeof(ulong)) return (ulong)(seed % 1000000 + 1);
        if (type == typeof(float)) return seed % 1000 + 1.5f;
        if (type == typeof(double)) return seed % 1000 + 1.5d;
        if (type == typeof(decimal)) return seed % 1000 + 1.5m;
        if (type == typeof(char)) return (char)('a' + seed % 26);
        if (type == typeof(Guid)) return DeterministicGuid(seed);
        if (type == typeof(DateTime)) return new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(seed % 100000);
        if (type == typeof(DateTimeOffset)) return new DateTimeOffset(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(seed % 100000), TimeSpan.Zero);
        if (type == typeof(TimeSpan)) return TimeSpan.FromSeconds(seed % 10000 + 1);
        if (type == typeof(Uri)) return new Uri($"https://example.test/{seed % 100000}");

        if (type.IsEnum)
        {
            var values = Enum.GetValues(type);
            // Prefer a non-zero member, so a dropped enum property is visible.
            foreach (var value in values)
            {
                if (Convert.ToInt64(value) != 0) return value;
            }

            return values.Length > 0 ? values.GetValue(0) : Activator.CreateInstance(type);
        }

        if (depth >= MaxDepth) return null;

        if (TryCreateCollection(type, seed, depth, out var collection)) return collection;

        return CreateComplex(type, seed, depth);
    }

    static bool TryCreateCollection(Type type, uint seed, int depth, out object? collection)
    {
        collection = null;

        if (type.IsArray)
        {
            var elementType = type.GetElementType()!;
            var element = Create(elementType, Mix(seed, "element"), depth + 1);
            if (element == null) return false;

            var array = Array.CreateInstance(elementType, 1);
            array.SetValue(element, 0);
            collection = array;
            return true;
        }

        if (!type.IsConstructedGenericType) return false;

        var definition = type.GetGenericTypeDefinition();
        var arguments = type.GetGenericArguments();

        if (definition == typeof(IReadOnlyDictionary<,>) || definition == typeof(IDictionary<,>)
            || definition == typeof(Dictionary<,>) || definition == typeof(ImmutableDictionary<,>))
        {
            var key = Create(arguments[0], Mix(seed, "key"), depth + 1);
            var value = Create(arguments[1], Mix(seed, "value"), depth + 1);
            if (key == null || value == null) return false;

            var dictionary = (System.Collections.IDictionary)Activator.CreateInstance(
                typeof(Dictionary<,>).MakeGenericType(arguments))!;
            dictionary[key] = value;

            collection = definition == typeof(ImmutableDictionary<,>)
                ? typeof(ImmutableDictionary).GetMethods()
                    .First(m => m.Name == nameof(ImmutableDictionary.ToImmutableDictionary) && m.GetParameters().Length == 1)
                    .MakeGenericMethod(arguments).Invoke(null, [dictionary])
                : dictionary;

            return collection != null;
        }

        if (arguments.Length != 1) return false;

        var itemType = arguments[0];
        var item = Create(itemType, Mix(seed, "item"), depth + 1);
        if (item == null) return false;

        var list = (System.Collections.IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(itemType))!;
        list.Add(item);

        if (definition == typeof(ImmutableArray<>))
        {
            collection = typeof(ImmutableArray).GetMethods()
                .First(m => m.Name == nameof(ImmutableArray.ToImmutableArray) && m.GetParameters().Length == 1)
                .MakeGenericMethod(itemType).Invoke(null, [list]);
            return collection != null;
        }

        if (definition == typeof(ImmutableList<>))
        {
            collection = typeof(ImmutableList).GetMethods()
                .First(m => m.Name == nameof(ImmutableList.ToImmutableList) && m.GetParameters().Length == 1)
                .MakeGenericMethod(itemType).Invoke(null, [list]);
            return collection != null;
        }

        if (definition == typeof(List<>) || definition == typeof(IReadOnlyList<>)
            || definition == typeof(IReadOnlyCollection<>) || definition == typeof(IList<>)
            || definition == typeof(ICollection<>) || definition == typeof(IEnumerable<>))
        {
            collection = list;
            return true;
        }

        return false;
    }

    static object? CreateComplex(Type type, uint seed, int depth)
    {
        var constructor = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .OrderByDescending(c => c.GetParameters().Length)
            .FirstOrDefault();

        if (constructor == null) return null;

        var parameters = constructor.GetParameters();
        var arguments = new object?[parameters.Length];

        for (var i = 0; i < parameters.Length; i++)
        {
            var parameter = parameters[i];
            var value = Create(parameter.ParameterType, Mix(seed, parameter.Name ?? i.ToString()), depth + 1);

            if (value == null)
            {
                // Depth limit or an unconstructable member. Null is legal only for a reference or nullable
                // type - this is how a cycle terminates.
                if (parameter.ParameterType.IsValueType && Nullable.GetUnderlyingType(parameter.ParameterType) == null)
                {
                    return null;
                }
            }

            arguments[i] = value;
        }

        var instance = constructor.Invoke(arguments);

        FillWritableProperties(type, instance, parameters, seed, depth);

        return instance;
    }

    /// <summary>
    /// Fills every writable property the constructor did not already cover - including init-only and
    /// private setters, which application code inside the owning assembly can set.
    /// <para>
    /// This is what makes the round-trip check able to observe anything at all. A deserializer runs the
    /// constructor, so every value the constructor establishes is re-established on the way back; only state
    /// set outside the constructor can actually be lost. Filled from the constructor alone, the check is
    /// vacuously green.
    /// </para>
    /// </summary>
    static void FillWritableProperties(
        Type type, object instance, ParameterInfo[] constructorParameters, uint seed, int depth)
    {
        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetIndexParameters().Length > 0) continue;
            if (property.SetMethod is not { } setter) continue;

            // Anything the constructor already supplied is left alone: overwriting it would only make the
            // generated instance disagree with itself.
            if (constructorParameters.Any(p =>
                    string.Equals(p.Name, property.Name, StringComparison.OrdinalIgnoreCase))) continue;

            var value = Create(property.PropertyType, Mix(seed, property.Name), depth + 1);
            if (value == null) continue;

            try
            {
                setter.Invoke(instance, [value]);
            }
            catch (Exception)
            {
                // A property that refuses the value it was offered is not worth failing construction over.
            }
        }
    }

    static Guid DeterministicGuid(uint seed)
    {
        var bytes = new byte[16];
        BitConverter.GetBytes(seed).CopyTo(bytes, 0);
        BitConverter.GetBytes(seed * 2654435761u).CopyTo(bytes, 4);
        BitConverter.GetBytes(seed * 40503u).CopyTo(bytes, 8);
        BitConverter.GetBytes(seed ^ 0x5bf03635u).CopyTo(bytes, 12);
        return new Guid(bytes);
    }

    static uint Mix(uint seed, string name) => Seed(name) ^ (seed * 16777619u);

    /// <summary>
    /// FNV-1a. Deliberately not <see cref="string.GetHashCode()"/>, which is randomized per process and
    /// would make the generated values differ between runs.
    /// </summary>
    static uint Seed(string text)
    {
        var hash = 2166136261u;

        foreach (var c in text)
        {
            hash ^= c;
            hash *= 16777619u;
        }

        return hash;
    }
}
