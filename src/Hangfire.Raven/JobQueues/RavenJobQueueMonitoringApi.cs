using System.Collections.Generic;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Raven.Client.Documents.Linq;

namespace Hangfire.Raven.JobQueues {
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
                    .ToList();
            }
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int pageFrom, int perPage)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                return repository.Query<JobQueue>()
                    .Where(a => a.Queue == queue && a.FetchedAt == null)
                    .Skip(pageFrom)
                    .Take(perPage)
                    .Select(a => a.JobId)
                    .ToList();
            }
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int pageFrom, int perPage)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                return repository.Query<JobQueue>()
                    .Where(a => a.Queue == queue && a.FetchedAt != null)
                    .Skip(pageFrom)
                    .Take(perPage)
                    .Select(a => a.JobId)
                    .ToList();
            }
        }

        public EnqueuedAndFetchedCount GetEnqueuedAndFetchedCount(string queue)
        {
            using (var session = _storage.Repository.OpenSession())
            {
                var fetchedQuery = session.Query<JobQueue>().Where(a => a.FetchedAt != null && a.Queue == queue);
                var fetchedCount = fetchedQuery.Count();

                var enqueuedQuery = session.Query<JobQueue>().Where(a => a.FetchedAt == null && a.Queue == queue);
                var enqueuedCount = enqueuedQuery.Count();

                return new EnqueuedAndFetchedCount
                {
                    EnqueuedCount = enqueuedCount,
                    FetchedCount = fetchedCount
                };
            }
        }
    }
}