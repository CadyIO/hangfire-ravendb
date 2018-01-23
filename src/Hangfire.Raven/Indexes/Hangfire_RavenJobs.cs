using System;
using System.Linq;
using Hangfire.Raven.Entities;
using Raven.Client.Documents.Indexes;

namespace Hangfire.Raven.Indexes {
    public class Hangfire_RavenJobs
        : AbstractIndexCreationTask<RavenJob>
    {
        public class Mapping
        {
            public DateTime CreatedAt { get; set; }
            public DateTime? ExpireAt { get; set; }
            public string StateName { get; set; }
        }

        public Hangfire_RavenJobs()
        {
            Map = results => from result in results
                             select new Mapping {
                                 StateName = result.StateData.Name,
                                 CreatedAt = result.CreatedAt
                             };
            this.Analyze("StateName", "WhitespaceAnalyzer");
        }
    }
}
