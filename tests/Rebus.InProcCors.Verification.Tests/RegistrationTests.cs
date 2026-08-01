using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Rebus.Handlers;

namespace Rebus.InProcCors.Verification.Tests;

public class RegistrationTests
{
    public sealed record GoodOrder(string Sku, int Quantity);

    public sealed class BadOrder
    {
        public string Sku { get; set; } = "";
    }

    sealed class GoodOrderHandler : IHandleMessages<GoodOrder>
    {
        public Task Handle(GoodOrder message) => Task.CompletedTask;
    }

    sealed class BadOrderHandler : IHandleMessages<BadOrder>
    {
        public Task Handle(BadOrder message) => Task.CompletedTask;
    }

    sealed class FakeEnvironment : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = Environments.Development;
        public string ApplicationName { get; set; } = "tests";
        public string ContentRootPath { get; set; } = AppContext.BaseDirectory;
        public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
    }

    sealed class RecordingLoggerProvider : ILoggerProvider
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public ILogger CreateLogger(string categoryName) => new RecordingLogger(Entries);

        public void Dispose() { }

        sealed class RecordingLogger(List<(LogLevel, string)> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter) =>
                entries.Add((logLevel, formatter(state, exception)));
        }
    }

    static ServiceProvider BuildProvider(
        Action<IServiceCollection> configure, string environmentName, RecordingLoggerProvider? loggerProvider = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IHostEnvironment>(new FakeEnvironment { EnvironmentName = environmentName });
        services.AddLogging(b =>
        {
            if (loggerProvider != null) b.AddProvider(loggerProvider);
        });
        configure(services);
        return services.BuildServiceProvider();
    }

    [Fact]
    public void TheVerifierIsResolvableAsASingletonSoATestUsesTheSameWiringAsTheApp()
    {
        using var provider = BuildProvider(services =>
        {
            services.AddRebusInProcContractVerification();
            services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();
        }, Environments.Development);

        var verifier = provider.GetRequiredService<MessageContractVerifier>();

        Assert.Same(verifier, provider.GetRequiredService<MessageContractVerifier>());
        Assert.True(verifier.Verify().IsSuccess);
    }

    [Fact]
    public void HandlersRegisteredAfterTheCallAreStillDiscovered()
    {
        using var provider = BuildProvider(services =>
        {
            services.AddRebusInProcContractVerification();
            services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();
        }, Environments.Development);

        Assert.Equal([typeof(GoodOrder)], provider.GetRequiredService<MessageContractVerifier>().Verify().VerifiedTypes);
    }

    [Fact]
    public async Task TheStartupCheckThrowsOutsideProduction()
    {
        using var provider = BuildProvider(services =>
        {
            services.AddRebusInProcContractVerification();
            services.AddTransient<IHandleMessages<BadOrder>, BadOrderHandler>();
        }, Environments.Development);

        var hostedService = provider.GetServices<IHostedService>().Single();

        await Assert.ThrowsAsync<MessageContractVerificationException>(
            () => hostedService.StartAsync(CancellationToken.None));
    }

    [Fact]
    public async Task TheStartupCheckLogsAndContinuesInProduction()
    {
        // A violation reaching production was already in a build that passed CI, and a reflection check
        // must never be the cause of an outage (design §10).
        var loggerProvider = new RecordingLoggerProvider();

        using var provider = BuildProvider(services =>
        {
            services.AddRebusInProcContractVerification();
            services.AddTransient<IHandleMessages<BadOrder>, BadOrderHandler>();
        }, Environments.Production, loggerProvider);

        var hostedService = provider.GetServices<IHostedService>().Single();
        await hostedService.StartAsync(CancellationToken.None);

        Assert.Contains(loggerProvider.Entries, e => e.Level == LogLevel.Error && e.Message.Contains(nameof(BadOrder)));
    }

    [Fact]
    public async Task TheStartupCheckIsSilentWhenEverythingPasses()
    {
        var loggerProvider = new RecordingLoggerProvider();

        using var provider = BuildProvider(services =>
        {
            services.AddRebusInProcContractVerification();
            services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();
        }, Environments.Development, loggerProvider);

        await provider.GetServices<IHostedService>().Single().StartAsync(CancellationToken.None);

        Assert.DoesNotContain(loggerProvider.Entries, e => e.Level == LogLevel.Error);
    }

    [Fact]
    public void VerifyOnStartupFalseRegistersNoHostedService()
    {
        using var provider = BuildProvider(services =>
        {
            services.AddRebusInProcContractVerification(o => o.VerifyOnStartup = false);
            services.AddTransient<IHandleMessages<BadOrder>, BadOrderHandler>();
        }, Environments.Development);

        Assert.Empty(provider.GetServices<IHostedService>());
        Assert.NotNull(provider.GetRequiredService<MessageContractVerifier>());
    }
}
