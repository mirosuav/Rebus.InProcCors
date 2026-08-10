using System.Collections.Immutable;

namespace Rebus.InProcCors.Verification.Tests;

public class ImmutabilityCheckerTests
{
    sealed record GoodOrder(string Sku, int Quantity, IReadOnlyList<string> Tags);

    sealed record OrderWithImmutableArray(ImmutableArray<string> Tags);

    sealed class MutableProperty
    {
        public string Sku { get; set; } = "";
    }

    sealed class PrivateSetter
    {
        public string Sku { get; private set; } = "";
    }

    sealed class PublicWritableField
    {
        public string Sku = "";
    }

    sealed class ReadonlyField
    {
        public readonly string Sku = "";
    }

    sealed record MutableCollectionMember(List<string> Tags);

    sealed record ArrayMember(string[] Tags);

    sealed record DictionaryMember(Dictionary<string, string> Attributes);

    sealed record ReadOnlyDictionaryMember(IReadOnlyDictionary<string, string> Attributes);

    sealed record Outer(Inner Inner);

    sealed class Inner
    {
        public string Value { get; set; } = "";
    }

    sealed record Node(string Name, Node? Next);

    sealed record ScalarTerminators(
        Guid Id, DateTime At, DateTimeOffset AtOffset, decimal Amount, TimeSpan Duration, Uri Link, DayOfWeek Day,
        int? MaybeCount);

    static List<VerificationViolation> Check<T>()
    {
        var violations = new List<VerificationViolation>();
        new ImmutabilityChecker().Check(typeof(T), violations);
        return violations;
    }

    [Fact]
    public void AGetOnlyRecordWithReadOnlyCollectionsPasses()
    {
        Assert.Empty(Check<GoodOrder>());
    }

    [Fact]
    public void ImmutableArrayIsAcceptedDespiteImplementingIList()
    {
        Assert.Empty(Check<OrderWithImmutableArray>());
    }

    [Fact]
    public void ScalarTypesTerminateTheWalk()
    {
        Assert.Empty(Check<ScalarTerminators>());
    }

    [Fact]
    public void APublicSetterIsAViolation()
    {
        var violation = Assert.Single(Check<MutableProperty>());

        Assert.Equal(VerificationCheck.Immutability, violation.Check);
        Assert.Equal(nameof(MutableProperty.Sku), violation.Member);
    }

    [Fact]
    public void APrivateSetterIsNotAViolationBecauseItIsNotPubliclyWritable()
    {
        Assert.Empty(Check<PrivateSetter>());
    }

    [Fact]
    public void APublicWritableFieldIsAViolation()
    {
        var violation = Assert.Single(Check<PublicWritableField>());
        Assert.Equal(nameof(PublicWritableField.Sku), violation.Member);
    }

    [Fact]
    public void APublicReadonlyFieldIsNotAViolation()
    {
        Assert.Empty(Check<ReadonlyField>());
    }

    [Theory]
    [InlineData(typeof(MutableCollectionMember))]
    [InlineData(typeof(ArrayMember))]
    [InlineData(typeof(DictionaryMember))]
    public void MutableCollectionMembersAreViolations(Type messageType)
    {
        var violations = new List<VerificationViolation>();
        new ImmutabilityChecker().Check(messageType, violations);

        Assert.Single(violations);
    }

    [Fact]
    public void ReadOnlyDictionaryMembersAreAccepted()
    {
        Assert.Empty(Check<ReadOnlyDictionaryMember>());
    }

    [Fact]
    public void TheWalkRecursesTransitively()
    {
        var violation = Assert.Single(Check<Outer>());

        Assert.Contains(nameof(Inner.Value), violation.Member);
        Assert.Contains(nameof(Inner), violation.Member);
    }

    [Fact]
    public void ACycleTerminatesInsteadOfOverflowing()
    {
        Assert.Empty(Check<Node>());
    }
}
