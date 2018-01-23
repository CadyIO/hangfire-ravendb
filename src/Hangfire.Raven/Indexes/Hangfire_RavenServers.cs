using System;
using System.Linq;
using Hangfire.Raven.Entities;
using Raven.Client.Documents.Indexes;

namespace Hangfire.Raven.Indexes {
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
            //Server should do this
            //Sort("LastHeartbeat", SortOptions.String);
        }
    }
}
