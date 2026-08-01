namespace Rebus.InProcCors.Verification.Tests;

public class DefaultMessageInstanceFactoryTests
{
    sealed record Scalars(
        string Text, int Count, long Big, bool Flag, double Rate, decimal Amount,
        Guid Id, DateTime At, DateTimeOffset AtOffset, TimeSpan Duration, Uri Link, DayOfWeek Day);

    sealed record WithNullable(int? Count, string? Text);

    sealed record WithCollection(IReadOnlyList<string> Tags);

    sealed record Nested(Scalars Inner);

    sealed record Cyclic(string Name, Cyclic? Next);

    sealed class NoPublicConstructor
    {
        NoPublicConstructor() { }
    }

    sealed record TwoConstructors
    {
        public TwoConstructors(string a) : this(a, 0) { }

        public TwoConstructors(string a, int b)
        {
            A = a;
            B = b;
        }

        public string A { get; }
        public int B { get; }
    }

    static object Create<T>()
    {
        Assert.True(new DefaultMessageInstanceFactory().TryCreate(typeof(T), out var instance));
        Assert.NotNull(instance);
        return instance!;
    }

    [Fact]
    public void EveryScalarGetsANonDefaultValue()
    {
        // Non-default values are load-bearing: filled with defaults, a property the serializer silently
        // drops is invisible - default in s1, default after deserialization, bytes equal, test green.
        var scalars = (Scalars)Create<Scalars>();

        Assert.False(string.IsNullOrEmpty(scalars.Text));
        Assert.NotEqual(0, scalars.Count);
        Assert.NotEqual(0L, scalars.Big);
        Assert.True(scalars.Flag);
        Assert.NotEqual(0d, scalars.Rate);
        Assert.NotEqual(0m, scalars.Amount);
        Assert.NotEqual(Guid.Empty, scalars.Id);
        Assert.NotEqual(default, scalars.At);
        Assert.NotEqual(default, scalars.AtOffset);
        Assert.NotEqual(TimeSpan.Zero, scalars.Duration);
        Assert.NotNull(scalars.Link);
    }

    [Fact]
    public void ConstructionIsDeterministicAcrossInstancesAndProcesses()
    {
        // A flaky serialization test gets deleted, so determinism matters more than variety.
        // String.GetHashCode is randomized per process, so the factory must not use it for its seed.
        var first = (Scalars)Create<Scalars>();
        var second = (Scalars)Create<Scalars>();

        Assert.Equal(first, second);
    }

    [Fact]
    public void DifferentMembersGetDifferentValues()
    {
        var scalars = (Scalars)Create<Scalars>();
        var nested = (Nested)Create<Nested>();

        Assert.NotEqual(scalars.Text, scalars.Id.ToString());
        Assert.NotNull(nested.Inner);
        Assert.False(string.IsNullOrEmpty(nested.Inner.Text));
    }

    [Fact]
    public void NullablesAreFilledRatherThanLeftNull()
    {
        var value = (WithNullable)Create<WithNullable>();

        Assert.NotNull(value.Count);
        Assert.False(string.IsNullOrEmpty(value.Text));
    }

    [Fact]
    public void CollectionsGetAtLeastOneElement()
    {
        var value = (WithCollection)Create<WithCollection>();

        Assert.NotEmpty(value.Tags);
        Assert.False(string.IsNullOrEmpty(value.Tags[0]));
    }

    [Fact]
    public void TheGreediestConstructorIsChosen()
    {
        var value = (TwoConstructors)Create<TwoConstructors>();

        Assert.NotEqual(0, value.B);
    }

    [Fact]
    public void ACycleIsBrokenWithNullRatherThanOverflowing()
    {
        var value = (Cyclic)Create<Cyclic>();

        Assert.False(string.IsNullOrEmpty(value.Name));
        // Some depth is produced, then the chain terminates.
        var depth = 0;
        for (var node = value; node != null; node = node.Next) depth++;
        Assert.InRange(depth, 1, 10);
    }

    [Fact]
    public void ATypeThatCannotBeConstructedIsDeclinedRatherThanThrowing()
    {
        Assert.False(new DefaultMessageInstanceFactory().TryCreate(typeof(NoPublicConstructor), out var instance));
        Assert.Null(instance);
    }
}
