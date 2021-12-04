using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Core;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Streams;

namespace Orlens.EventuousGrains
{
    /// <summary>
    /// In order to make grain's publish events timer keep 'grain call semantic'
    /// </summary>
    internal interface IEventuousTiming : IGrain
    {
        Task PublishEventsByTimer();
    }

    public abstract class EventuousGrain<TState> : Grain, IRemindable, IEventuousTiming where TState : new()
    {
        private readonly static EventId _publishErrorEventId = new EventId(9000001, "PublishingError");

        private readonly PublishOptions _publishOptions;
        private readonly StreamOptions _streamOptions;
        private readonly ILogger _logger;

        private IAsyncStream<object> _stream;
        private IStorage<EventuousState<TState>> _storage;
        private IGrainReminder _publishingReminder;
        private IDisposable _publishingTimer;

        protected EventuousGrain(PublishOptions publishOptions, StreamOptions streamOptions, ILogger logger)
        {
            _publishOptions = publishOptions;
            _streamOptions = streamOptions;
            _logger = logger;
        }

        protected readonly static string PublishingReminderName = "Eventuous-PublishingReminder";

        protected IReadOnlyCollection<object> UnpublishedEvents => _storage.State.UnpublishedEvents;

        protected bool HasUnpublishedEvents => _storage.State.HasUnpublishedEvents;

        protected DateTimeOffset? LastAddEventTime => _storage.State.LastAddEventTime;

        protected TState State
        {
            get => _storage.State.State;
            set => _storage.State.State = value;
        }

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();

            var streamProvider = GetStreamProvider(_streamOptions.Provider);
            _stream = streamProvider.GetStream<object>(this.GetPrimaryKey(), _streamOptions.Namespace);
        }

        public override async Task OnDeactivateAsync()
        {
            await base.OnDeactivateAsync();

            if (!HasUnpublishedEvents)
            {
                try { await UnregisterPublishingReminder(); } catch { /* Ignore.. */ }
            }
        }

        public virtual async Task ReceiveReminder(string reminderName, TickStatus status)
        {
            if (reminderName == PublishingReminderName)
            {
                _publishingReminder ??= await GetReminder(reminderName);

                // If doesn't has unpublished events
                // And hasn't added events in (PublishReminderIntervalMinutes * 2)
                // Then unregister the publish events reminder
                if (!HasUnpublishedEvents
                    && (DateTimeOffset.Now - (LastAddEventTime ?? status.FirstTickTime))
                        >= TimeSpan.FromMinutes(_publishOptions.PublishReminderIntervalMinutes * 2))
                {
                    await UnregisterPublishingReminder();
                    UnregisterPublishingTimer();
                }
                else if (HasUnpublishedEvents)
                {
                    RegisterPublishingTimer();
                }
            }
        }

        public override void Participate(IGrainLifecycle lifecycle)
        {
            base.Participate(lifecycle);
            lifecycle.Subscribe(GetType().FullName, GrainLifecycleStage.SetupState, OnSetupState);

            Task OnSetupState(CancellationToken ct)
            {
                if (ct.IsCancellationRequested)
                    return Task.CompletedTask;

                var grainStorage = this.GetGrainStorage(ServiceProvider);
                var grainTypeName = GetType().FullName;
                var loggerFactory = ServiceProvider.GetService<ILoggerFactory>();

                _storage = new StateStorageBridge<EventuousState<TState>>(
                    grainTypeName, GrainReference, grainStorage, loggerFactory);

                return _storage.ReadStateAsync();
            }
        }

        Task IEventuousTiming.PublishEventsByTimer()
        {
            return PublishEvents(true, false);
        }

        protected async Task SafeClear()
        {
            await PublishEvents(false, true);
            await _storage.ClearStateAsync();

            _storage.State = new EventuousState<TState>();
        }

        protected Task Commit()
        {
            return _storage.WriteStateAsync();
        }

        protected Task CommitWithEvent(object @event, bool publishEventsImmediately = true)
            => CommitWithEvents(new[] { @event }, publishEventsImmediately);

        protected async Task CommitWithEvents(
            IEnumerable<object> events,
            bool publishEventsImmediately = true)
        {
            events = events?.Where(e => e != null) ?? Enumerable.Empty<object>();

            if (events.Count() > 0)
            {
                await DeactivateOnError(async () =>
                {
                    await RegisterPublishingReminder();
                    RegisterPublishingTimer();
                });
            }

            foreach (var @event in events ?? Enumerable.Empty<object>())
            {
                _storage.State.AddEvent(@event);
            }

            await DeactivateOnError(_storage.WriteStateAsync);

            if (publishEventsImmediately && HasUnpublishedEvents)
            {
                await PublishEvents(true, false);
            }

            async Task DeactivateOnError(Func<Task> func)
            {
                try
                {
                    await func.Invoke();
                }
                catch
                {
                    DeactivateOnIdle();
                    throw;
                }
            }
        }

        protected Task Reload()
        {
            return _storage.ReadStateAsync();
        }

        private async Task PublishEvents(bool saveAfterPublished, bool throwOnPublishError)
        {
            var eventQueue = _storage.State.UnpublishedEvents;
            var anyEventPublished = false;

            while (eventQueue.TryPeek(out object @event))
            {
                try
                {
                    await _stream.OnNextAsync(@event);
                    eventQueue.Dequeue();
                    anyEventPublished = true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        _publishErrorEventId, ex,
                        "Occur error when publish event {@event}, Grain id {@grainId}, Silo Id {@siloId}",
                        @event, this.GetPrimaryKey(), RuntimeIdentity);

                    if (throwOnPublishError) throw;

                    break;
                }
            }

            if (saveAfterPublished && anyEventPublished)
            {
                await _storage.WriteStateAsync();
            }
        }

        private async Task RegisterPublishingReminder()
        {
            var interval = TimeSpan.FromMinutes(_publishOptions.PublishReminderIntervalMinutes);

            _publishingReminder ??= await RegisterOrUpdateReminder(
                PublishingReminderName, dueTime: interval, period: interval);
        }

        private async Task UnregisterPublishingReminder()
        {
            if (_publishingReminder != null)
            {
                await UnregisterReminder(_publishingReminder);
                _publishingReminder = null;
            }
        }

        private void RegisterPublishingTimer()
        {
            var interval = TimeSpan.FromSeconds(_publishOptions.PublishTimerIntervalSeconds);

            _publishingTimer ??= RegisterTimer(
                state => this.AsReference<IEventuousTiming>().PublishEventsByTimer(),
                state: null,
                dueTime: interval,
                period: interval);
        }

        private void UnregisterPublishingTimer()
        {
            _publishingTimer?.Dispose();
            _publishingTimer = null;
        }
    }
}
