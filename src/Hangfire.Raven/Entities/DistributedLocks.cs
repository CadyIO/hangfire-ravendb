using Hangfire.Raven.Entities.Identity;
using System;

namespace Hangfire.Raven.Entities
{
    public class DistributedLocks : BaseEntity
    {
        public string Resource { get; set; }

        public string ClientId { get; set; }

        public int LockCount { get; set; }

        public DateTime Heartbeat { get; set; }
    }
}
