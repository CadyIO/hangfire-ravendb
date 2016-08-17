using System;
using System.Collections.Generic;

namespace Hangfire.Raven.Storage
{
    public class RavenStorageOptions
    {
        private readonly string _clientId = null;

        public RavenStorageOptions()
        {
            JobExpirationCheckInterval = TimeSpan.FromHours(1);
            CountersAggregateInterval = TimeSpan.FromMinutes(5);
            DashboardJobListLimit = 50000;
            TransactionTimeout = TimeSpan.FromMinutes(1);

            _clientId = Guid.NewGuid().ToString().Replace("-", String.Empty);
        }

        public TimeSpan InvisibilityTimeout { get; set; }

        public TimeSpan JobExpirationCheckInterval { get; set; }
        public TimeSpan CountersAggregateInterval { get; set; }

        public int? DashboardJobListLimit { get; set; }
        public TimeSpan TransactionTimeout { get; set; }

        public IEnumerable<string> QueueNames { get; set; }

        public string ClientId
        {
            get { return _clientId; }
        }

    }
}
