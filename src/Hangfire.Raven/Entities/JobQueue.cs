using System;

namespace Hangfire.Raven.Entities
{
    public class JobQueue
    {
        public string Id { get; set; }
        public string JobId { get; set; }
        public string Queue { get; set; }
        public bool Fetched { get; set; }
    }
}
