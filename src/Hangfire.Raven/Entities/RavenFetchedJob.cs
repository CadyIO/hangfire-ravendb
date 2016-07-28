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

using Hangfire.Annotations;
using Hangfire.Storage;
using HangFire.Raven;
using Hangfire.Raven.Storage;

namespace Hangfire.Raven.Entities
{
    public class RavenFetchedJob : IFetchedJob
    {
        private readonly RavenStorage _storage;

        private bool _requeued { get; set; }
        private bool _removedFromQueue { get; set; }
        private bool _disposed { get; set; }

        public string Id { get; set; }
        public string JobId { get; set; }
        public string Queue { get; set; }

        public RavenFetchedJob(
            [NotNull] RavenStorage storage,
            JobQueue jobQueue)
        {
            storage.ThrowIfNull("storage");
            jobQueue.ThrowIfNull("jobQueue");

            _storage = storage;

            JobId = jobQueue.JobId;
            Queue = jobQueue.Queue;
            Id = jobQueue.Id;
        }

        public void RemoveFromQueue()
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var job = repository.Load<JobQueue>(Id);

                if (job != null)
                {
                    repository.Delete(job);
                }
                repository.SaveChanges();
            }

            _removedFromQueue = true;
        }

        public void Requeue()
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var job = repository.Load<JobQueue>(Id);

                job.FetchedAt = null;

                repository.Store(job);
                repository.SaveChanges();
            }

            _requeued = true;
        }

        public void Dispose()
        {
            if (_disposed) {
                return;
            }

            if (!_removedFromQueue && !_requeued) {
                Requeue();
            }

            _disposed = true;
        }
    }
}
