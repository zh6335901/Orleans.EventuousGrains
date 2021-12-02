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
    internal interface IPublishable : IGrain
    {
        Task PublishEvents(bool saveAfterPublished);
    }

    public abstract class EventuousGrain<TState> : Grain, IRemindable, IPublishable where TState : new()
    {
        private readonly static EventId _publishErrorEventId = new EventId(9000001, "PublishError");

        private IAsyncStream<object> _stream;
        private IStorage<EventuousState<TState>> _storage;
        private IGrainReminder _publishEventsReminder;
        private IDisposable _publishEventsTimer;

        private readonly PublishOptions _publishOptions;
        private readonly StreamOptions _streamOptions;
        private readonly ILogger _logger;

        protected EventuousGrain(PublishOptions publishOptions, StreamOptions streamOptions, ILogger logger) 
        {
            _publishOptions = publishOptions;
            _streamOptions = streamOptions;
            _logger = logger;
        }

        protected readonly static string PublishReminderName = "Eventuous-PublishEvents";

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

            _publishEventsTimer = RegisterTimer(
                state => this.AsReference<IPublishable>().PublishEvents(true),
                state: null,
                dueTime: TimeSpan.FromSeconds(_publishOptions.PublishTimerIntervalSeconds),
                period: TimeSpan.FromSeconds(_publishOptions.PublishTimerIntervalSeconds));
        }

        public override async Task OnDeactivateAsync()
        {
            await base.OnDeactivateAsync();

            if (!HasUnpublishedEvents)
            {
                try { await UnregisterPublishEventsReminder(); } catch { /* Ignore.. */ }
            }

            _publishEventsTimer?.Dispose();
        }

        public virtual async Task ReceiveReminder(string reminderName, TickStatus status)
        {
            // If doesn't have unpublished event
            // And hasn't added events in (PublishReminderIntervalMinutes * 2)
            // Then unregister the publish events reminder
            if (reminderName == PublishReminderName 
                && !HasUnpublishedEvents 
                && (DateTimeOffset.Now - LastAddEventTime) 
                    > TimeSpan.FromMinutes(_publishOptions.PublishReminderIntervalMinutes * 2))
            {
                if (_publishEventsReminder == null)
                {
                    _publishEventsReminder = await GetReminder(reminderName);
                }

                await UnregisterPublishEventsReminder();
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

        public async Task PublishEvents(bool saveAfterPublished)
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

                    break;
                }
            }

            if (saveAfterPublished && anyEventPublished)
            {
                await _storage.WriteStateAsync();
            }
        }

        protected async Task SafeClear()
        {
            await PublishEvents(false);
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
                await DeactivateOnError(RegisterPublishEventsReminder);
            }

            foreach (var @event in events ?? Enumerable.Empty<object>())
            {
                _storage.State.AddEvent(@event);
            }

            await DeactivateOnError(_storage.WriteStateAsync);

            if (publishEventsImmediately && HasUnpublishedEvents)
            {
                await PublishEvents(true);
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

        private async Task RegisterPublishEventsReminder()
        {
            if (_publishEventsReminder == null)
            {
                _publishEventsReminder = await RegisterOrUpdateReminder(
                    PublishReminderName,
                    dueTime: TimeSpan.FromMinutes(_publishOptions.PublishReminderIntervalMinutes),
                    period: TimeSpan.FromMinutes(_publishOptions.PublishReminderIntervalMinutes));
            }
        }

        private async Task UnregisterPublishEventsReminder()
        {
            if (_publishEventsReminder != null)
            {
                await UnregisterReminder(_publishEventsReminder);
                _publishEventsReminder = null;
            }
        }
    }
}
