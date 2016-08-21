using System;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Storage;
using Raven.Client.Linq;
using System.Linq;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Raven.Abstractions.Data;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueue 
        : IPersistentJobQueue
    {
        private readonly RavenStorage _storage;
        private readonly RavenStorageOptions _options;
        private BlockingCollection<string> _queue;

        public RavenJobQueue([NotNull] RavenStorage storage, RavenStorageOptions options)
        {
            storage.ThrowIfNull("storage");
            options.ThrowIfNull("options");

            _storage = storage;
            _options = options;
            _queue = new BlockingCollection<string>();

            using (var session = _storage.Repository.OpenSession())
            {
                var missed = session.Advanced.LoadStartingWith<JobQueue>(Repository.GetId(typeof(JobQueue), ""));
                foreach(var miss in missed)
                {
                    _queue.Add(miss.Id);
                }
            }

            // -- Queue listening
            if (options.QueueNames == null)
            {
                Console.WriteLine("Starting on ALL Queue's, this is not recommended, please specify using RavenStorageOptions. Use an empty IEnumerable (such as List<string> or string[0]) to not listen to any queue.");
                _storage.Repository.DocumentChange(typeof(JobQueue), QueueFiller);
            }
            else
            {
                foreach (var queue in options.QueueNames)
                {
                    Console.WriteLine("Starting on queue: {0}", queue);
                    _storage.Repository.DocumentChange(typeof(JobQueue), queue, QueueFiller);
                }
            }
        }

        private void QueueFiller(DocumentChangeNotification notification)
        {
            if(notification.Type == DocumentChangeTypes.Put)
                _queue.Add(notification.Id);
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            queues.ThrowIfNull("queues");

            if (queues.Length == 0) {
                throw new ArgumentException("Queue array must be non-empty.", "queues");
            }

            JobQueue fetchedJob = null;
            do
            {
                var jobId = _queue.Take(cancellationToken);

                using (var repository = _storage.Repository.OpenSession())
                {
                    fetchedJob = repository.Load<JobQueue>(jobId);
                    if (fetchedJob != null && 
                        fetchedJob.FetchedAt == null &&
                        queues.Contains(fetchedJob.Queue))
                    {
                        fetchedJob.FetchedAt = DateTime.UtcNow;
                        repository.Store(fetchedJob);

                        try
                        {
                            // Did someone else already picked it up?
                            repository.Advanced.UseOptimisticConcurrency = true;
                            repository.SaveChanges();
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