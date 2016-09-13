using System.Collections.Generic;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;

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
            using (var repository = _storage.Repository.OpenSession()) {
                return repository.Query<JobQueue>()
                    .Select(x => x.Queue)
                    .Distinct()
                    .ToList();
            }
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            int start = @from + 1;
            int end = from + perPage;

            using (var repository = _storage.Repository.OpenSession()) {
                var jobs = repository.Query<JobQueue>()
                    .Where(t => t.Queue == queue && t.FetchedAt == null)
                    .Select((data, i) => new { Index = i + 1, Data = data })
                    .Where(_ => (_.Index >= start) && (_.Index <= end))
                    .Select(x => x.Data)
                    .ToList();

                var results = new List<RavenJob>();

                foreach (var item in jobs) {
                    var job = repository.Load<RavenJob>(item.JobId);

                    if (job.StateData != null)
                        results.Add(job);
                }

                return results.Select(t => t.Id.Split(new char[] { '/' }, 2)[1]).ToList();
            }

        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            int start = @from + 1;
            int end = from + perPage;

            using (var repository = _storage.Repository.OpenSession()) {
                var jobs = repository.Query<JobQueue>().Where(t => t.Queue == queue && t.FetchedAt != null)
                                .Select((data, i) => new { Index = i + 1, Data = data })
                                .Where(_ => (_.Index >= start) && (_.Index <= end))
                                .Select(x => x.Data);

                var results = new List<string>();

                foreach (var item in jobs) {
                    var job = repository.Load<RavenJob>(item.JobId);

                    if (job != null) {
                        results.Add(job.Id.Split(new char[] { '/' }, 2)[1]);
                    }
                }

                return results;
            }
        }

        public EnqueuedAndFetchedCount GetEnqueuedAndFetchedCount(string queue)
        {
            using (var repository = _storage.Repository.OpenSession()) {
                int enqueuedCount = repository.Query<JobQueue>().Where(t => t.Queue == queue && t.FetchedAt == null).Count();
                int fetchedCount = repository.Query<JobQueue>().Where(t => t.Queue == queue && t.FetchedAt != null).Count();

                return new EnqueuedAndFetchedCount {
                    EnqueuedCount = enqueuedCount,
                    FetchedCount = fetchedCount
                };
            }
        }
    }
}