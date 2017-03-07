using System;
using System.Linq;
using Hangfire.Raven.Entities;
using Raven.Client.Indexes;
using Raven.Abstractions.Indexing;

namespace Hangfire.Raven.Indexes
{
    public class Hangfire_RavenServers
        : AbstractIndexCreationTask<RavenServer>
    {
        public class Mapping
        {
            public DateTime LastHeartbeat { get; set; }
        }

        public Hangfire_RavenServers()
        {
            Map = results => from result in results
                             select new Mapping {
                                 LastHeartbeat = result.LastHeartbeat
                             };
            Sort("LastHeartbeat", SortOptions.String);
        }
    }
}
