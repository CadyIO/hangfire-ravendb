using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Raven.Abstractions.Data;
using System.Collections.Generic;
using Raven.Client;
using System.Linq.Expressions;
using Raven.Json.Linq;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueue
        : IPersistentJobQueue
    {
        private readonly RavenStorage _storage;
        private readonly RavenStorageOptions _options;
        private Dictionary<string, BlockingCollection<JobQueue>> _queue;

        public RavenJobQueue([NotNull] RavenStorage storage, RavenStorageOptions options)
        {
            storage.ThrowIfNull("storage");
            options.ThrowIfNull("options");

            _storage = storage;
            _options = options;
            _queue = new Dictionary<string, BlockingCollection<JobQueue>>();

            using (var session = _storage.Repository.OpenSession())
            {
                // -- Queue listening
                if (options.QueueNames == null)
                {
                    throw new ArgumentNullException("options.QueueNames", "You should define a set of QueueNames.");
                }

                var config = session.Load<RavenJobQueueConfig>("Config/RavenJobQueue") 
                    ?? new RavenJobQueueConfig();

                foreach (var queue in options.QueueNames)
                {
                    Console.WriteLine($"Starting on queue: {queue}");

                    // Creating queue
                    _queue.Add(queue, new BlockingCollection<JobQueue>());

                    // Create subscription (if not already exist)
                    if(!config.Subscriptions.ContainsKey(queue))
                    {
                        config.Subscriptions[queue] = _storage.Repository
                            .Subscriptions()
                            .Create<JobQueue>(new SubscriptionCriteria<JobQueue>()
                            {
                                KeyStartsWith = $"{Repository.GetId(typeof(JobQueue), queue)}/",
                                PropertiesMatch = new Dictionary<Expression<Func<JobQueue, object>>, RavenJToken>()
                                {
                                    { x => x.Fetched, RavenJToken.FromObject(false) }
                                }
                            });
                    }
                    
                    // Open subscription
                    var subscription = _storage.Repository.Subscriptions().Open<JobQueue>(
                        config.Subscriptions[queue], 
                        new SubscriptionConnectionOptions()
                        {
                            IgnoreSubscribersErrors = false,
                            Strategy = SubscriptionOpeningStrategy.ForceAndKeep
                        });

                    // Subscribe to it
                    subscription.Subscribe(new RepositoryObserver<JobQueue>(a =>
                    {
                        Console.WriteLine(a.Id);
                        _queue[queue].Add(a);
                    }));
                }

                session.Store(config);
                session.SaveChanges();
            }
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            queues.ThrowIfNull("queues");

            if (queues.Length == 0)
            {
                throw new ArgumentException("Queue array must be non-empty.", "queues");
            }
            else if (queues.Length > 1)
            {
                throw new ArgumentException("Please use one queue per worker", "queues");
            }

            // Make sure queue exists
            if (!_queue.ContainsKey(queues[0]))
            {
                // Lock and test again
                lock (_queue)
                {
                    if (!_queue.ContainsKey(queues[0]))
                        _queue.Add(queues[0], new BlockingCollection<JobQueue>());
                }
            }

            JobQueue fetchedJob = null;
            do
            {
                fetchedJob = _queue[queues[0]].Take(cancellationToken);
                using (var session = _storage.Repository.OpenSession())
                {
                    fetchedJob = session.Load<JobQueue>(fetchedJob.Id);

                    if (fetchedJob != null &&
                        !fetchedJob.Fetched)
                    {
                        fetchedJob.Fetched = true;

                        try
                        {
                            session.Advanced.UseOptimisticConcurrency = true;
                            session.SaveChanges();
                        }
                        catch
                        {
                            fetchedJob = null;
                        }
                    }
                    else
                    {
                        fetchedJob = null;
                    }
                }
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
                    Id = Repository.GetId(typeof(JobQueue), queue, jobId),
                    JobId = jobId,
                    Queue = queue
                };

                repository.Store(jobQueue);
                repository.SaveChanges();
            }
        }
    }
}