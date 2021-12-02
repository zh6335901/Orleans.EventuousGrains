namespace Orlens.EventuousGrains
{
    public sealed record PublishOptions(
        int PublishTimerIntervalSeconds,
        int PublishReminderIntervalMinutes);
}
