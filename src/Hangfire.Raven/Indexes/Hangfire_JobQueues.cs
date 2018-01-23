using Hangfire.Raven.Entities;
using Raven.Client.Documents.Indexes;
using System;
using System.Linq;

namespace Hangfire.Raven.Indexes {
    public class Hangfire_JobQueues
        : AbstractIndexCreationTask<JobQueue>
    {
        public class Mapping
        {
            public DateTime? FetchedAt { get; set; }
            public string Queue { get; set; }
            public string JobId { get; set; }
        }

        public Hangfire_JobQueues()
        {
            Map = results => from result in results
                               select new Mapping
                               {
                                   Queue = result.Queue,
                                   FetchedAt = result.FetchedAt,
                                   JobId = result.JobId
                               };
            Analyze("Queue", "WhitespaceAnalyzer");
            
            //Probably not supported in 4.0
            //Sort("FetchedAt", SortOptions.String);
        }
    }
}
