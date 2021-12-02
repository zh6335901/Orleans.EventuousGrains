using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Streams;

namespace Orlens.EventuousGrains
{
    public interface ISubscriberGrain : IGrainWithGuidKey 
    {
    }

    public abstract class SubscriberGrain : Grain, ISubscriberGrain
    {
        private StreamSubscriptionHandle<object> _subscription;

        private readonly StreamOptions _streamOptions;
        private readonly ILogger _logger;

        protected SubscriberGrain(StreamOptions streamOptions, ILogger logger)
        {
            _streamOptions = streamOptions;
            _logger = logger;
        }

        public override async Task OnActivateAsync()
        {
            var guid = this.GetPrimaryKey();
            var streamProvider = GetStreamProvider(_streamOptions.Provider);
            var stream = streamProvider.GetStream<object>(guid, _streamOptions.Namespace);

            var subscriptionHandles = await stream.GetAllSubscriptionHandles();
            var subs = subscriptionHandles.FirstOrDefault(e => e.HandleId == guid);
            if (subs != null)
            {
                _subscription = subs;
                await subs.ResumeAsync(OnNextAsync);
            }
            else
            {
                _subscription = await stream.SubscribeAsync(OnNextAsync);
            }
        }

        public override Task OnDeactivateAsync()
        {
            _subscription.UnsubscribeAsync();

            return base.OnDeactivateAsync();
        }

        public abstract Task<bool> Handle(object @event, StreamSequenceToken token);

        private async Task OnNextAsync(object @event, StreamSequenceToken token)
        {
            var handled = await Handle(@event, token);

            if (handled)
            {
                _logger.LogInformation(
                    "Handled event of type {@evtType} for subscriber grain id {grainId}, event body: {@eventBody}",
                    @event.GetType().Name,
                    this.GetPrimaryKey(),
                    @event);
            }
        }
    }
}