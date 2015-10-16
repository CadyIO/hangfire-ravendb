namespace Hangfire.Raven.Entities
{
    public class EnqueuedAndFetchedCount
    {
        public int? EnqueuedCount { get; set; }

        public int? FetchedCount { get; set; }
    }
}
