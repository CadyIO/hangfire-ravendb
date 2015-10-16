using Hangfire.Raven.Entities.Identity;
using System;

namespace Hangfire.Raven.Entities
{
    public class JobQueue : BaseEntity
    {
        public string JobId { get; set; }
        public string Queue { get; set; }
        public DateTime? FetchedAt { get; set; }
    }
}
