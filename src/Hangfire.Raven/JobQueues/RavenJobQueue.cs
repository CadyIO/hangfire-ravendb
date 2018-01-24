using System;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Hangfire.Logging;
using System.Linq.Expressions;
using Raven.Client.Exceptions;

namespace Hangfire.Raven.JobQueues {
    public class RavenJobQueue : IPersistentJobQueue {
        private static readonly ILog Logger = LogProvider.For<RavenJobQueue>();

        private readonly RavenStorage _storage;

        private readonly RavenStorageOptions _options;

        public RavenJobQueue([NotNull] RavenStorage storage, RavenStorageOptions options) {
            storage.ThrowIfNull("storage");
            options.ThrowIfNull("options");

            _storage = storage;
            _options = options;
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken) {
            queues.ThrowIfNull(nameof(queues));

            if (queues.Length == 0)
                throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            var seconds = DateTime.UtcNow.AddSeconds(_options.InvisibilityTimeout.Negate().TotalSeconds);
            var fetchConditions = new Expression<Func<JobQueue, bool>>[]
            {
                job => job.FetchedAt == null,
                job => job.FetchedAt < seconds
            };
            var currentQueryIndex = 0;

            do {
                cancellationToken.ThrowIfCancellationRequested();

                var fetchCondition = fetchConditions[currentQueryIndex];
                foreach (var queue in queues) {
                    using (var session = _storage.Repository.OpenSession()) {
                        var query = session.Query<JobQueue>()
                                .Where(fetchCondition)
                                .Where(job => job.Queue == queue);
                        var enumerator = session.Advanced.Stream(query);

                        while (enumerator.MoveNext()) {
                            var job = enumerator.Current.Document;
                            if (!session.Advanced.HasChanged(job)) {
                                job.FetchedAt = DateTime.UtcNow;
                                return new RavenFetchedJob(_storage, job);
                            }
                        }
                    }
                }

                currentQueryIndex = (currentQueryIndex + 1) % fetchConditions.Length;
                if (currentQueryIndex == fetchConditions.Length - 1) {
                    cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }
            while (true);
        }

        public void Enqueue(string queue, string jobId) {
            using (var repository = _storage.Repository.OpenSession()) {
                var jobQueue = new JobQueue {
                    Id = _storage.Repository.GetId(typeof(JobQueue), queue, jobId),
                    JobId = jobId,
                    Queue = queue
                };

                repository.Store(jobQueue);
                repository.SaveChanges();
            }
        }
    }
}
