using Hangfire.Common;
using Hangfire.Storage;
using System;
using System.Collections.Generic;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Raven.Entities
{
    public class RavenJob
    {
        public RavenJob()
        {
            this.Parameters = new Dictionary<string, string>();
            this.History = new List<StateHistoryDto>();
        }

        public string Id { get; set; }
        public InvocationData InvocationData { get; set; }
        public IDictionary<string, string> Parameters { get; set; }
        public DateTime CreatedAt { get; set; }

        public StateData StateData { get; set; }
        public List<StateHistoryDto> History { get; set; }
    }
}