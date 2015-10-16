using Hangfire.Raven.Entities.Identity;
using System;

namespace Hangfire.Raven.Entities
{
    public class RavenList : BaseEntity
    {
        public string Key { get; set; }

        public string Value { get; set; }

        public DateTime ExpireAt { get; set; }
    }
}
