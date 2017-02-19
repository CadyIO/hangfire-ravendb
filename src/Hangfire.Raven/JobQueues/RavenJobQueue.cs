using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Raven.Abstractions.Data;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueue
        : IPersistentJobQueue
    {
        private readonly RavenStorage _storage;
        private readonly RavenStorageOptions _options;
        private Dictionary<string, BlockingCollection<string>> _queue;

        public RavenJobQueue([NotNull] RavenStorage storage, RavenStorageOptions options)
        {
            storage.ThrowIfNull("storage");
            options.ThrowIfNull("options");

            _storage = storage;
            _options = options;
            _queue = new Dictionary<string, BlockingCollection<string>>();

            using (var session = _storage.Repository.OpenSession()) {
                // -- Queue listening
                if (options.QueueNames == null)
                {
                    Console.WriteLine("Starting on ALL Queue's, this is not recommended, please specify using RavenStorageOptions. Use an empty IEnumerable (such as List<string> or string[0]) to not listen to any queue.");
                    var missed = session.Advanced.LoadStartingWith<JobQueue>(Repository.GetId(typeof(JobQueue), ""));
                    foreach (var miss in missed)
                    {
                        this.QueueFiller(new DocumentChangeNotification()
                        {
                            Type = DocumentChangeTypes.Put,
                            Id = miss.Id
                        });
                    }
                    _storage.Repository.DocumentChange(typeof(JobQueue), QueueFiller);
                }
                else
                {
                    foreach (var queue in options.QueueNames)
                    {
                        Console.WriteLine("Starting on queue: {0}", queue);
                        var missed = session.Advanced.LoadStartingWith<JobQueue>(Repository.GetId(typeof(JobQueue), queue));
                        foreach (var miss in missed)
                        {
                            this.QueueFiller(new DocumentChangeNotification()
                            {
                                Type = DocumentChangeTypes.Put,
                                Id = miss.Id
                            });
                        }
                        _storage.Repository.DocumentChange(
                            typeof(JobQueue), 
                            queue, 
                            (DocumentChangeNotification notification) => QueueFiller(notification));
                    }
                }
            }
        }

        private void QueueFiller(DocumentChangeNotification notification)
        {
            if (notification.Type == DocumentChangeTypes.Put)
            {
                // Get Queue:
                var key = notification.Id.Split(new[] { '/' }, 3)[1];

                // Check if exists
                BlockingCollection<string> queue;
                if (!_queue.TryGetValue(key, out queue))
                {
                    lock (_queue)
                    {
                        queue = new BlockingCollection<string>();
                        _queue.Add(key, queue);
                    }
                }

                queue.Add(notification.Id);
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
                        _queue.Add(queues[0], new BlockingCollection<string>());
                }
            }

            JobQueue fetchedJob = null;
            do {

                var jobId = _queue[queues[0]].Take(cancellationToken);

                using (var repository = _storage.Repository.OpenSession()) {
                    fetchedJob = repository.Load<JobQueue>(jobId);
                    if (fetchedJob != null &&
                        fetchedJob.FetchedAt == null)
                        fetchedJob.FetchedAt = DateTime.UtcNow;
                        repository.Store(fetchedJob);

                        try {
                            // Did someone else already picked it up?
                            repository.Advanced.UseOptimisticConcurrency = true;
                            repository.SaveChanges();
                        } catch {
                            fetchedJob = null;
                        }
                    } else {
                        fetchedJob = null;
                    }
                }
            }
            while (fetchedJob == null);

            return new RavenFetchedJob(_storage, fetchedJob);
        }

        public void Enqueue(string queue, string jobId)
        {
            using (var repository = _storage.Repository.OpenSession()) {
                var jobQueue = new JobQueue {
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