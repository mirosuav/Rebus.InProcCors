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

    [ImmutabilityExempt("Legacy contract owned by the billing team, tracked in TICKET-42.")]
    sealed class ExemptedType
    {
        public string Sku { get; set; } = "";
    }

    sealed class TypeWithExemptedMember
    {
        public string Sku { get; init; } = "";

        [ImmutabilityExempt("Interop buffer handed straight to the native layer, tracked in TICKET-43.")]
        public List<string> Buffer { get; set; } = new();
    }

    static (List<VerificationViolation> Violations, List<VerificationExemption> Exemptions) Check<T>()
    {
        var violations = new List<VerificationViolation>();
        var exemptions = new List<VerificationExemption>();
        new ImmutabilityChecker().Check(typeof(T), violations, exemptions);
        return (violations, exemptions);
    }

    [Fact]
    public void AGetOnlyRecordWithReadOnlyCollectionsPasses()
    {
        var (violations, exemptions) = Check<GoodOrder>();

        Assert.Empty(violations);
        Assert.Empty(exemptions);
    }

    [Fact]
    public void ImmutableArrayIsAcceptedDespiteImplementingIList()
    {
        Assert.Empty(Check<OrderWithImmutableArray>().Violations);
    }

    [Fact]
    public void ScalarTypesTerminateTheWalk()
    {
        Assert.Empty(Check<ScalarTerminators>().Violations);
    }

    [Fact]
    public void APublicSetterIsAViolation()
    {
        var violation = Assert.Single(Check<MutableProperty>().Violations);

        Assert.Equal(VerificationCheck.Immutability, violation.Check);
        Assert.Equal(nameof(MutableProperty.Sku), violation.Member);
    }

    [Fact]
    public void APrivateSetterIsNotAViolationBecauseItIsNotPubliclyWritable()
    {
        Assert.Empty(Check<PrivateSetter>().Violations);
    }

    [Fact]
    public void APublicWritableFieldIsAViolation()
    {
        var violation = Assert.Single(Check<PublicWritableField>().Violations);
        Assert.Equal(nameof(PublicWritableField.Sku), violation.Member);
    }

    [Fact]
    public void APublicReadonlyFieldIsNotAViolation()
    {
        Assert.Empty(Check<ReadonlyField>().Violations);
    }

    [Theory]
    [InlineData(typeof(MutableCollectionMember))]
    [InlineData(typeof(ArrayMember))]
    [InlineData(typeof(DictionaryMember))]
    public void MutableCollectionMembersAreViolations(Type messageType)
    {
        var violations = new List<VerificationViolation>();
        new ImmutabilityChecker().Check(messageType, violations, new List<VerificationExemption>());

        Assert.Single(violations);
    }

    [Fact]
    public void ReadOnlyDictionaryMembersAreAccepted()
    {
        Assert.Empty(Check<ReadOnlyDictionaryMember>().Violations);
    }

    [Fact]
    public void TheWalkRecursesTransitively()
    {
        var violation = Assert.Single(Check<Outer>().Violations);

        Assert.Contains(nameof(Inner.Value), violation.Member);
        Assert.Contains(nameof(Inner), violation.Member);
    }

    [Fact]
    public void ACycleTerminatesInsteadOfOverflowing()
    {
        Assert.Empty(Check<Node>().Violations);
    }

    [Fact]
    public void AnExemptedTypeIsListedRatherThanFailed()
    {
        var (violations, exemptions) = Check<ExemptedType>();

        Assert.Empty(violations);
        var exemption = Assert.Single(exemptions);
        Assert.Contains("TICKET-42", exemption.Reason);
    }

    [Fact]
    public void AnExemptedMemberIsListedRatherThanFailed()
    {
        var (violations, exemptions) = Check<TypeWithExemptedMember>();

        Assert.Empty(violations);
        var exemption = Assert.Single(exemptions);
        Assert.Equal(nameof(TypeWithExemptedMember.Buffer), exemption.Member);
        Assert.Contains("TICKET-43", exemption.Reason);
    }

    [Fact]
    public void AnEmptyExemptionReasonIsRejectedAtConstruction()
    {
        Assert.Throws<ArgumentException>(() => new ImmutabilityExemptAttribute("  "));
    }
}
