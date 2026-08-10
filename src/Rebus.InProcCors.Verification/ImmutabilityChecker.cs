using System.Collections;
using System.Collections.Immutable;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Walks a contract's reachable object graph and reports everything that is publicly mutable.
/// Design §10 "Check 2".
/// </summary>
public sealed class ImmutabilityChecker
{
    static readonly HashSet<Type> TerminatingTypes =
    [
        typeof(string), typeof(decimal), typeof(Guid), typeof(DateTime), typeof(DateTimeOffset),
        typeof(TimeSpan), typeof(Uri), typeof(object), typeof(Type)
    ];

    static readonly HashSet<Type> MutableCollectionDefinitions =
    [
        typeof(List<>), typeof(Dictionary<,>), typeof(HashSet<>), typeof(Queue<>), typeof(Stack<>),
        typeof(SortedList<,>), typeof(SortedDictionary<,>),
        typeof(ICollection<>), typeof(IList<>), typeof(IDictionary<,>), typeof(ISet<>)
    ];

    static readonly HashSet<Type> AcceptedCollectionDefinitions =
    [
        typeof(IReadOnlyList<>), typeof(IReadOnlyCollection<>), typeof(IReadOnlyDictionary<,>),
        typeof(IEnumerable<>), typeof(ImmutableArray<>), typeof(ImmutableList<>), typeof(ImmutableDictionary<,>),
        typeof(ImmutableHashSet<>), typeof(ImmutableSortedSet<>), typeof(ImmutableSortedDictionary<,>),
        typeof(KeyValuePair<,>)
    ];

    /// <summary>
    /// Checks <paramref name="messageType"/>, appending anything it finds to <paramref name="violations"/>.
    /// There is no per-type or per-member exemption: the only escape hatch is
    /// <see cref="MessageContractVerificationOptions.VerifyImmutability"/>, which switches the whole check
    /// off for a module (design §10).
    /// </summary>
    /// <param name="messageType">The contract to check.</param>
    /// <param name="violations">Receives everything publicly mutable that was found.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="messageType"/> is null.</exception>
    public void Check(Type messageType, ICollection<VerificationViolation> violations)
    {
        if (messageType == null) throw new ArgumentNullException(nameof(messageType));

        Walk(messageType, messageType, memberPath: "", new HashSet<Type>(), violations);
    }

    void Walk(
        Type rootType,
        Type currentType,
        string memberPath,
        HashSet<Type> visited,
        ICollection<VerificationViolation> violations)
    {
        currentType = Nullable.GetUnderlyingType(currentType) ?? currentType;

        if (IsTerminating(currentType)) return;

        if (TryGetElementType(currentType, out var elementType))
        {
            Walk(rootType, elementType!, memberPath, visited, violations);
            return;
        }

        if (!visited.Add(currentType)) return;   // cycle

        foreach (var property in currentType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetIndexParameters().Length > 0) continue;

            var path = Combine(memberPath, currentType, property.Name, rootType);

            if (IsPubliclyWritable(property))
            {
                violations.Add(new VerificationViolation(rootType, VerificationCheck.Immutability, path,
                    "the property has a public setter; make it get-only or init-only"));
                continue;
            }

            if (CheckMemberType(rootType, property.PropertyType, path, violations)) continue;

            Walk(rootType, property.PropertyType, path, visited, violations);
        }

        foreach (var field in currentType.GetFields(BindingFlags.Public | BindingFlags.Instance))
        {
            var path = Combine(memberPath, currentType, field.Name, rootType);

            if (!field.IsInitOnly)
            {
                violations.Add(new VerificationViolation(rootType, VerificationCheck.Immutability, path,
                    "the field is publicly writable; make it readonly, or expose it as a get-only property"));
                continue;
            }

            if (CheckMemberType(rootType, field.FieldType, path, violations)) continue;

            Walk(rootType, field.FieldType, path, visited, violations);
        }

        visited.Remove(currentType);
    }

    static string Combine(string memberPath, Type declaringType, string memberName, Type rootType)
    {
        var name = declaringType == rootType ? memberName : $"{declaringType.Name}.{memberName}";

        return string.IsNullOrEmpty(memberPath) ? name : $"{memberPath} -> {name}";
    }

    /// <summary>
    /// Returns true when the member's type is itself the problem, so the caller should not recurse into it.
    /// </summary>
    static bool CheckMemberType(
        Type rootType, Type memberType, string path, ICollection<VerificationViolation> violations)
    {
        memberType = Nullable.GetUnderlyingType(memberType) ?? memberType;

        if (!IsMutableCollection(memberType)) return false;

        violations.Add(new VerificationViolation(rootType, VerificationCheck.Immutability, path,
            $"the member is typed as the mutable collection '{Describe(memberType)}'; use IReadOnlyList<>, " +
            "IReadOnlyDictionary<> or ImmutableArray<> instead"));

        return true;
    }

    static bool IsTerminating(Type type) =>
        type.IsPrimitive || type.IsEnum || TerminatingTypes.Contains(type);

    static bool IsMutableCollection(Type type)
    {
        if (type == typeof(string)) return false;
        if (type.IsArray) return true;

        if (type.IsConstructedGenericType)
        {
            var definition = type.GetGenericTypeDefinition();

            // Accepted definitions win outright: ImmutableArray<T> implements IList<T> explicitly, so the
            // interface test below would otherwise reject the very type the design recommends.
            if (AcceptedCollectionDefinitions.Contains(definition)) return false;
            if (MutableCollectionDefinitions.Contains(definition)) return true;
        }

        return type.GetInterfaces().Any(i => i.IsConstructedGenericType
                                             && MutableCollectionDefinitions.Contains(i.GetGenericTypeDefinition()))
               || typeof(IList).IsAssignableFrom(type)
               || typeof(IDictionary).IsAssignableFrom(type);
    }

    /// <summary>
    /// Gets the element type to recurse into for an accepted read-only collection, so that a
    /// <c>IReadOnlyList&lt;Mutable&gt;</c> is still caught.
    /// </summary>
    static bool TryGetElementType(Type type, out Type? elementType)
    {
        elementType = null;

        if (type == typeof(string)) return false;

        if (type.IsConstructedGenericType && AcceptedCollectionDefinitions.Contains(type.GetGenericTypeDefinition()))
        {
            var arguments = type.GetGenericArguments();
            elementType = arguments[arguments.Length - 1];   // value type for dictionaries, element otherwise
            return true;
        }

        return false;
    }

    static bool IsPubliclyWritable(PropertyInfo property)
    {
        var setter = property.SetMethod;

        if (setter == null || !setter.IsPublic) return false;

        // An init accessor is a setter whose return parameter carries a required custom modifier for
        // IsExternalInit. Without this every record reads as mutable and the check is worthless (design §10).
        return !setter.ReturnParameter.GetRequiredCustomModifiers().Contains(typeof(IsExternalInit));
    }

    static string Describe(Type type) =>
        type.IsConstructedGenericType
            ? $"{type.Name.Split('`')[0]}<{string.Join(", ", type.GetGenericArguments().Select(a => a.Name))}>"
            : type.Name;
}
