// This file is part of Hangfire.
// Copyright © 2013-2014 Sergey Odinokov.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

using System.Collections.Generic;
using System.Linq;
using Hangfire.Raven.Entities;
using HangFire.Raven;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Hangfire.Annotations;

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

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            int start = @from + 1;
            int end = from + perPage;

            using (var repository = _storage.Repository.OpenSession())
            {
                var jobs = repository.Query<JobQueue>().Where(t => t.Queue == queue && t.FetchedAt == null)
                .Select((data, i) => new { Index = i + 1, Data = data })
                .Where(_ => (_.Index >= start) && (_.Index <= end))
                .Select(x => x.Data)
                .ToList();

                var results = new List<RavenJob>();

                foreach (var item in jobs) {
                    var job = repository.Query<RavenJob>().FirstOrDefault(t => t.Id == item.JobId);

                    if(job.StateData != null)
                        results.Add(job);
                }

                return results.Select(t => t.Id).ToList();
            }

        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            int start = @from + 1;
            int end = from + perPage;

            using (var repository = _storage.Repository.OpenSession())
            {
                var jobs = repository.Query<JobQueue>().Where(t => t.Queue == queue && t.FetchedAt != null)
                                .Select((data, i) => new { Index = i + 1, Data = data })
                                .Where(_ => (_.Index >= start) && (_.Index <= end))
                                .Select(x => x.Data);

                var results = new List<string>();

                foreach (var item in jobs)
                {
                    var job = repository.Query<RavenJob>().FirstOrDefault(t => t.Id == item.JobId);

                    if (job != null) {
                        results.Add(job.Id);
                    }
                }

                return results;
            }
        }

        public EnqueuedAndFetchedCount GetEnqueuedAndFetchedCount(string queue)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                int enqueuedCount = repository.Query<JobQueue>().Where(t => t.Queue == queue && t.FetchedAt == null).Count();

                int fetchedCount = repository.Query<JobQueue>().Where(t => t.Queue == queue && t.FetchedAt != null).Count();

                return new EnqueuedAndFetchedCount
                {
                    EnqueuedCount = enqueuedCount,
                    FetchedCount = fetchedCount
                };
            }
        }
    }
}