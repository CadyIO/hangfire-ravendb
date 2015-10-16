using System;

namespace Hangfire.Raven.Entities
{
    public class AggregatedCounter
    {
        public string Id { get; set; }

        public string Key { get; set; }

        public long Value { get; set; }

        public DateTime? ExpireAt { get; set; }
    }
}
