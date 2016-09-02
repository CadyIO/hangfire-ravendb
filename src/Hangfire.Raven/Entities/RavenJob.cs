using Hangfire.Common;
using Hangfire.Storage;
using System;
using System.Collections.Generic;
using Hangfire.Storage.Monitoring;
using System.Reflection;
using System.Linq;

namespace Hangfire.Raven.Entities
{
    public class RavenJob
    {
        public RavenJob()
        {
            this.Parameters = new Dictionary<string, string>();
            this.History = new List<StateHistoryDto>();
        }
        public class JobWrapper
        {
            public IEnumerable<object> Arguments { get; set; }
            public Type Type { get; set; }
            public MethodInfo Method { get; set; }

            public Job GetJob()
            {
                return new Job(this.Type, this.Method, this.Arguments.ToArray());
            }

            public static JobWrapper Create(Job job)
            {
                var toReturn = new JobWrapper();

                toReturn.Arguments = job.Args;
                toReturn.Method = job.Method;
                toReturn.Type = job.Type;

                return toReturn;
            }
        }

        public string Id { get; set; }
        public JobWrapper Job { get; set; }
        public IDictionary<string, string> Parameters { get; set; }
        public DateTime CreatedAt { get; set; }

        public StateData StateData { get; set; }
        public List<StateHistoryDto> History { get; set; }
    }
}