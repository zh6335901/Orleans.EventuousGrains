namespace Orlens.EventuousGrains
{
    public class EventuousState<TState> where TState : new()
    {
        public TState State { get; internal set; } = new TState();

        public Queue<object> UnpublishedEvents { get; private init; } = new Queue<object>();

        public DateTimeOffset? LastAddEventTime { get; private set; }

        public bool HasUnpublishedEvents => UnpublishedEvents.Count > 0;

        public void AddEvent(object @event) 
        {
            UnpublishedEvents.Enqueue(@event);
            LastAddEventTime = DateTimeOffset.Now;
        } 
    }
}
