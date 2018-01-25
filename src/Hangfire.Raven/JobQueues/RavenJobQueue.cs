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
using Hangfire.Raven.Extensions;

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

            var fetchConditions = new Expression<Func<JobQueue, bool>>[]
            {
                job => job.FetchedAt == null,
                job => job.FetchedAt < DateTime.UtcNow.AddSeconds(_options.InvisibilityTimeout.Negate().TotalSeconds)
            };
            var currentQueryIndex = 0;

            do {
                cancellationToken.ThrowIfCancellationRequested();

                var fetchCondition = fetchConditions[currentQueryIndex];
                using (var session = _storage.Repository.OpenSession()) {
                    session.Advanced.UseOptimisticConcurrency = true;
                    foreach (var queue in queues) {
                        var job = session.Query<JobQueue>()
                                .Where(fetchCondition.Compile())
                                .Where(j => j.Queue == queue)
                                .FirstOrDefault();

                        if(job != null) {
                            try {
                                job.FetchedAt = DateTime.UtcNow;
                                session.SaveChanges();
                                return new RavenFetchedJob(_storage, job);
                            } catch(ConcurrencyException) {

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
            using (var session = _storage.Repository.OpenSession()) {
                var jobQueue = new JobQueue {
                    Id = _storage.Repository.GetId(typeof(JobQueue), queue, jobId),
                    JobId = jobId,
                    Queue = queue
                };

                session.Store(jobQueue);
                session.SaveChanges();
            }
        }
    }
}
