using Hangfire.Raven.Entities.Identity;
using System;

namespace Hangfire.Raven.Entities
{
    public class RavenSet : BaseEntity
    {
        public string Key { get; set; }

        public double Score { get; set; }

        public string Value { get; set; }

        public DateTime? ExpireAt { get; set; }
    }
}
