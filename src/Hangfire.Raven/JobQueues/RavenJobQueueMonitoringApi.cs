using System.Collections.Generic;
using System.Linq;
using Hangfire.Raven.Entities;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Hangfire.Annotations;
using Hangfire.Raven.Indexes;

namespace Hangfire.Raven.JobQueues
{
    internal class RavenJobQueueMonitoringApi 
        : IPersistentJobQueueMonitoringApi
    {
        private RavenStorage _storage;

        public RavenJobQueueMonitoringApi([NotNull] RavenStorage storage)
        {
            storage.ThrowIfNull("storage");

            _storage = storage;
        }

        public IEnumerable<string> GetQueues()
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                return repository.Query<JobQueue>()
                    .Select(x => x.Queue)
                    .Distinct()
                    .ToList();
            }
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int pageFrom, int perPage)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var jobs = repository.Query<Hangfire_JobQueues.Mapping, Hangfire_JobQueues>()
                    .Where(a => a.Queue == queue && a.FetchedAt == null)
                    .Skip(pageFrom)
                    .Take(perPage)
                    .OfType<JobQueue>()
                    .ToList();

                var results = new List<RavenJob>();

                foreach (var item in jobs)
                {
                    var job = repository.Load<RavenJob>(item.JobId);

                    if(job.StateData != null)
                        results.Add(job);
                }

                return results.Select(t => t.Id.Split(new char[] { '/' }, 2)[1]).ToList();
            }

        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int pageFrom, int perPage)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var jobs = repository.Query<Hangfire_JobQueues.Mapping, Hangfire_JobQueues>()
                    .Where(a => a.Queue == queue && a.FetchedAt != null)
                    .Skip(pageFrom)
                    .Take(perPage)
                    .OfType<JobQueue>()
                    .ToList();

                var results = new List<RavenJob>();

                foreach (var item in jobs)
                {
                    var job = repository.Load<RavenJob>(item.JobId);

                    if (job.StateData != null)
                        results.Add(job);
                }

                return results.Select(t => t.Id.Split(new char[] { '/' }, 2)[1]).ToList();
            }
        }

        public EnqueuedAndFetchedCount GetEnqueuedAndFetchedCount(string queue)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var fetchedQuery = _storage.GetJobQueueFacets(repository, a => a.FetchedAt != null && a.Queue == queue);
                var fetchedCount = fetchedQuery.Results["Queue"].Values.Sum(a => a.Hits);

                var enqueuedQuery = _storage.GetJobQueueFacets(repository, a => a.FetchedAt == null && a.Queue == queue);
                var enqueuedCount = enqueuedQuery.Results["Queue"].Values.Sum(a => a.Hits);
                
                return new EnqueuedAndFetchedCount
                {
                    EnqueuedCount = enqueuedCount,
                    FetchedCount = fetchedCount
                };
            }
        }
    }
}