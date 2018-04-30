using System;
using System.Collections.Generic;

namespace Hangfire.Raven.Storage
{
    public class RavenStorageOptions
    {
        private readonly string _clientId = null;

        private TimeSpan _queuePollInterval;

        private TimeSpan _distributedLockLifetime;

        public RavenStorageOptions()
        {
            QueuePollInterval = TimeSpan.FromSeconds(15);
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            JobExpirationCheckInterval = TimeSpan.FromHours(1);
            CountersAggregateInterval = TimeSpan.FromMinutes(5);
            TransactionTimeout = TimeSpan.FromMinutes(1);
            DistributedLockLifetime = TimeSpan.FromSeconds(30);

            _clientId = Guid.NewGuid().ToString().Replace("-", string.Empty);
        }

        public TimeSpan QueuePollInterval
        {
            get { return _queuePollInterval; }
            set
            {
                var message = $"The QueuePollInterval property value should be positive. Given: {value}.";

                if (value == TimeSpan.Zero)
                {
                    throw new ArgumentException(message, nameof(value));
                }
                if (value != value.Duration())
                {
                    throw new ArgumentException(message, nameof(value));
                }

                _queuePollInterval = value;
            }
        }

        public TimeSpan InvisibilityTimeout { get; set; }

        public TimeSpan JobExpirationCheckInterval { get; set; }
        public TimeSpan CountersAggregateInterval { get; set; }

        public TimeSpan TransactionTimeout { get; set; }

        public TimeSpan DistributedLockLifetime
        {
            get { return _distributedLockLifetime; }
            set
            {
                var message = $"The DistributedLockLifetime property value should be positive. Given: {value}.";

                if (value == TimeSpan.Zero)
                {
                    throw new ArgumentException(message, nameof(value));
                }
                if (value != value.Duration())
                {
                    throw new ArgumentException(message, nameof(value));
                }

                _distributedLockLifetime = value;
            }
        }

        public IEnumerable<string> QueueNames { get; set; }

        public string ClientId {
            get { return _clientId; }
        }

    }
}
