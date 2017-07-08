using System;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Raven.Abstractions.Exceptions;
using Raven.Client.Linq;
using Hangfire.Logging;
using Hangfire.Raven.Indexes;
using System.Linq.Expressions;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueue : IPersistentJobQueue
    {
        private static readonly ILog Logger = LogProvider.For<RavenJobQueue>();

        private readonly RavenStorage _storage;

        private readonly RavenStorageOptions _options;

        public RavenJobQueue([NotNull] RavenStorage storage, RavenStorageOptions options)
        {
            storage.ThrowIfNull("storage");
            options.ThrowIfNull("options");

            _storage = storage;
            _options = options;
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            queues.ThrowIfNull("queues");

            if (queues.Length == 0)
            {
                throw new ArgumentException("Queue array must be non-empty.", "queues");
            }

            JobQueue fetchedJob = null;

            var fetchConditions = new Expression<Func<Hangfire_JobQueues.Mapping, bool>>[]
            {
                job => job.FetchedAt == null,
                job => job.FetchedAt < DateTime.UtcNow.AddSeconds(_options.InvisibilityTimeout.Negate().TotalSeconds)
            };
            var currentQueryIndex = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                var fetchCondition = fetchConditions[currentQueryIndex];

                foreach (var queue in queues)
                {
                    using (var repository = _storage.Repository.OpenSession())
                    {
                        foreach (var job in repository.Query<Hangfire_JobQueues.Mapping, Hangfire_JobQueues>()
                            .Where(fetchCondition)
                            .Where(job => job.Queue == queue)
                            .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                            .OfType<JobQueue>())
                        {
                            job.FetchedAt = DateTime.UtcNow;

                            try
                            {
                                // Did someone else already picked it up?
                                repository.Advanced.UseOptimisticConcurrency = true;
                                repository.SaveChanges();

                                fetchedJob = job;
                                break;
                            }
                            catch (ConcurrencyException)
                            {
                                repository.Advanced.Evict(job); // Avoid subsequent concurrency exceptions
                            }
                        }
                    }
                    if (fetchedJob != null)
                    {
                        break;
                    }
                }

                if (fetchedJob == null)
                {
                    if (currentQueryIndex == fetchConditions.Length - 1)
                    {
                        cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }

                currentQueryIndex = (currentQueryIndex + 1) % fetchConditions.Length;
            }
            while (fetchedJob == null);

            return new RavenFetchedJob(_storage, fetchedJob);
        }

        public void Enqueue(string queue, string jobId)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var jobQueue = new JobQueue
                {
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
