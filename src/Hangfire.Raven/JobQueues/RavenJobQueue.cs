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

using System;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Storage;
using HangFire.Raven;
using Raven.Client.Linq;
using System.Linq;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueue : IPersistentJobQueue
    {
        private readonly RavenStorage _storage;
        private readonly RavenStorageOptions _options;

        private static object _lockMethod = new object();

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

            if (queues.Length == 0) {
                throw new ArgumentException("Queue array must be non-empty.", "queues");
            }

            lock (_lockMethod) {

                JobQueue fetchedJob;

                cancellationToken.ThrowIfCancellationRequested();

                do {
                    using (var repository = new Repository()) {

                        fetchedJob = repository.Session.Query<JobQueue>().FirstOrDefault(t => t.FetchedAt == null
                                                                   && t.Queue.In(queues));

                        if (fetchedJob == null) {
                            fetchedJob = repository.Session.Query<JobQueue>().FirstOrDefault(t =>
                                                                   t.FetchedAt < DateTime.UtcNow.AddSeconds(_options.InvisibilityTimeout.Negate().TotalSeconds)
                                                                   && t.Queue.In(queues));

                            if (fetchedJob == null) {
                                cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                                cancellationToken.ThrowIfCancellationRequested();

                                continue;
                            }
                        }

                        fetchedJob.FetchedAt = DateTime.UtcNow;

                        repository.Save(fetchedJob);
                    }
                } while (fetchedJob == null);

                return new RavenFetchedJob(_storage, fetchedJob);
            }
        }

        public void Enqueue(string queue, string jobId)
        {
            using (var repository = new Repository()) {
                var jobQueue = new JobQueue
                {
                    JobId = jobId,
                    Queue = queue
                };

                repository.Save(jobQueue);
            }
        }
    }
}