using System.Collections.Generic;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.Raven.Indexes;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueueMonitoringApi
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
            using (var repository = _storage.Repository.OpenSession()) {
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
                return repository.Query<Hangfire_JobQueues.Mapping, Hangfire_JobQueues>()
                    .Where(a => a.Queue == queue && a.FetchedAt == null)
                    .Skip(pageFrom)
                    .Take(perPage)
                    .Select(a => a.JobId)
                    .ToList()
                    .Where(jobId =>
                    {
                        var job = repository.Load<RavenJob>(_storage.Repository.GetId(typeof(RavenJob), jobId));
                        return (job != null) && (job.StateData != null);
                    })
                    .ToList();
            }
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int pageFrom, int perPage)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                return repository.Query<Hangfire_JobQueues.Mapping, Hangfire_JobQueues>()
                    .Where(a => a.Queue == queue && a.FetchedAt != null)
                    .Skip(pageFrom)
                    .Take(perPage)
                    .Select(a => a.JobId)
                    .ToList()
                    .Where(jobId =>
                    {
                        var job = repository.Load<RavenJob>(_storage.Repository.GetId(typeof(RavenJob), jobId));
                        return job != null;
                    })
                    .ToList();
            }
        }

        public EnqueuedAndFetchedCount GetEnqueuedAndFetchedCount(string queue)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var fetchedQuery = _storage.GetJobQueueFacets(repository, a => a.FetchedAt != null && a.Queue == queue).Execute();
                var fetchedCount = fetchedQuery.Values.Sum(a => a.RemainingHits);

                var enqueuedQuery = _storage.GetJobQueueFacets(repository, a => a.FetchedAt == null && a.Queue == queue).Execute();
                var enqueuedCount = enqueuedQuery.Values.Sum(a => a.RemainingHits);

                return new EnqueuedAndFetchedCount
                {
                    EnqueuedCount = enqueuedCount,
                    FetchedCount = fetchedCount
                };
            }
        }
    }
}