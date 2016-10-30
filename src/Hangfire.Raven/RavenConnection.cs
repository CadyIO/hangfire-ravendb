﻿// This file is part of Hangfire.
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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Raven.Client;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.Raven.DistributedLocks;
using static Hangfire.Raven.Entities.RavenJob;

namespace Hangfire.Raven
{
    public class RavenConnection : 
        JobStorageConnection
    {
        private readonly RavenStorage _storage;

        public RavenConnection(RavenStorage ravenStorage)
        {
            ravenStorage.ThrowIfNull("RavenStorage");

            _storage = ravenStorage;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new RavenWriteOnlyTransaction(_storage);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new RavenDistributedLock(_storage, string.Format("HangFire/{0}", resource), timeout);
        }


        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            queues.ThrowIfNull("queues");
            if (queues.Length == 0) {
                throw new ArgumentNullException("queues");
            }

            var providers = queues
                .Select(queue => _storage.QueueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1) {
                throw new InvalidOperationException(string.Format(
                    "Multiple provider instances registered for queues: {0}. You should choose only one type of persistent queues per server instance.",
                    string.Join(", ", queues)));
            }

            var persistentQueue = providers[0].GetJobQueue();
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public override string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters,
            DateTime createdAt,
            TimeSpan expireIn)
        {
            job.ThrowIfNull("job");
            parameters.ThrowIfNull("parameters");

            using (var repository = _storage.Repository.OpenSession())
            {
                var guid = Guid.NewGuid().ToString();

                var ravenJob = new RavenJob
                {
                    Id = Repository.GetId(typeof(RavenJob), guid),
                    Job = JobWrapper.Create(job),
                    CreatedAt = createdAt,
                    Parameters = parameters
                };

                repository.Store(ravenJob);
                repository.Advanced.AddExpire(ravenJob, createdAt + expireIn);

                repository.SaveChanges();

                return guid;
            }
        }

        public override JobData GetJobData(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenJob), key);
                var jobData = repository.Load<RavenJob>(id);

                if (jobData == null)
                {
                    return null;
                }
                var job = jobData.Job.GetJob();
                return new JobData
                {
                    Job = job,
                    State = jobData.StateData?.Name,
                    CreatedAt = jobData.CreatedAt
                };
            }
        }

        public override StateData GetStateData(string jobId)
        {
            jobId.ThrowIfNull("jobId");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenJob), jobId);
                var job = repository.Load<RavenJob>(id);

                if (job == null) {
                    return null;
                }

                return job.StateData;
            }
        }

        public override void SetJobParameter(string jobId, string name, string value)
        {
            jobId.ThrowIfNull("jobId");
            name.ThrowIfNull("name");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenJob), jobId);
                var job = repository.Load<RavenJob>(id);

                job.Parameters[name] = value;
                repository.SaveChanges();
            }
        }

        public override string GetJobParameter(string jobId, string name)
        {
            jobId.ThrowIfNull("jobId");
            name.ThrowIfNull("name");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenJob), jobId);
                var job = repository.Load<RavenJob>(id);

                string value;
                if (!job.Parameters.TryGetValue(name, out value))
                {
                    if (name == "RetryCount")
                    {
                        job.Parameters["RetryCount"] = "0";
                        repository.SaveChanges();

                        return "0";
                    }
                    return null;
                }
                return value;
            }
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenSet), key);
                var set = repository.Load<RavenSet>(id);

                if (set == null)
                    return null;

                return new HashSet<string>(set.Scores.Keys);
            }
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            key.ThrowIfNull("key");

            if (toScore < fromScore) {
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            }

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenSet), key);
                var set = repository.Load<RavenSet>(id);

                if (set == null)
                    return null;

                return set.Scores.OrderBy(a => a.Value).Select(a => a.Key).FirstOrDefault();
            }
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            key.ThrowIfNull("key");
            keyValuePairs.ThrowIfNull("keyValuePairs");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenHash), key);
                var result = repository.Load<RavenHash>(id);

                if (result == null)
                {
                    result = new RavenHash()
                    {
                        Id = id
                    };
                    repository.Store(result);
                }

                foreach (var keyValuePair in keyValuePairs)
                {
                    result.Fields[keyValuePair.Key] = keyValuePair.Value;
                }

                repository.SaveChanges();
            }
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var result = repository.Load<RavenHash>(Repository.GetId(typeof(RavenHash), key));

                return result?.Fields;
            }
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            serverId.ThrowIfNull("serverId");
            context.ThrowIfNull("context");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenServer), serverId);
                var server = repository.Load<RavenServer>(id);

                if(server == null)
                {
                    server = new RavenServer()
                    {
                        Id = id,
                        Data = new RavenServer.ServerData()
                        {
                            StartedAt = DateTime.UtcNow
                        }
                        
                    };
                    repository.Store(server);
                }

                server.Data.WorkerCount += context.WorkerCount;
                server.Data.Queues = context.Queues.Concat(server.Data.Queues ?? new List<string>()).Distinct();

                server.LastHeartbeat = DateTime.UtcNow;

                repository.SaveChanges();
            }
        }

        public override void RemoveServer(string serverId)
        {
            serverId.ThrowIfNull("serverId");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenServer), serverId);
                repository.Delete(id);

                repository.SaveChanges();
            }
        }

        public override void Heartbeat(string serverId)
        {
            serverId.ThrowIfNull("serverId");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenServer), serverId);
                var server = repository.Load<RavenServer>(id);
                
                if (server == null)
                {
                    server = new RavenServer
                    {
                        Id = id
                    };
                }

                server.LastHeartbeat = DateTime.UtcNow;
                repository.SaveChanges();
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut) {
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");
            }

            using (var repository = _storage.Repository.OpenSession())
            {
                var heartBeatCutOff = DateTime.UtcNow.Add(timeOut.Negate());

                var results = repository.Query<RavenServer>()
                                .Where(t => t.LastHeartbeat < heartBeatCutOff)
                                .ToList();

                foreach (var item in results) {
                    repository.Delete(item);
                }
                repository.SaveChanges();

                return results.Count;
            }
        }

        public override long GetSetCount(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenSet), key);
                var set = repository.Load<RavenSet>(id);

                if (set == null)
                    return 0;

                return set.Scores.Count;
            }
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenSet), key);
                var set = repository.Load<RavenSet>(id);

                if (set == null)
                    return new List<string>();

                return set.Scores.Skip(startingFrom - 1)
                            .Take(endingAt - startingFrom + 1)
                            .Select(t => t.Key)
                            .ToList();
            }
        }

        public override TimeSpan GetSetTtl(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenSet), key);
                var set = repository.Load<RavenSet>(id);

                var expireAt = repository.Advanced.GetExpire(set);

                if(expireAt == null)
                    return TimeSpan.FromSeconds(-1);

                return expireAt.Value - DateTime.UtcNow;
            }
        }

        public override long GetCounter(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var values = repository.Query<AggregatedCounter>()
                                .Where(t => t.Key == key)
                                .Select(t => t.Value)
                                .ToArray();

                return values.Any() ? values.Sum() : 0;
            }
        }

        public override long GetHashCount(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var ravenHash = repository.Load<RavenHash>(Repository.GetId(typeof(RavenHash), key));

                return ravenHash.Fields.Count;
            }
        }

        public override TimeSpan GetHashTtl(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var ravenHash = repository.Load<RavenHash>(Repository.GetId(typeof(RavenHash), key));

                var expireAt = repository.Advanced.GetExpire(ravenHash);
                if (!expireAt.HasValue) {
                    return TimeSpan.FromSeconds(-1);
                }

                return expireAt.Value - DateTime.UtcNow;
            }
        }

        public override string GetValueFromHash(string key, string name)
        {
            key.ThrowIfNull("key");
            name.ThrowIfNull("name");

            using (var repository = _storage.Repository.OpenSession())
            {
                var ravenHash = repository.Load<RavenHash>(Repository.GetId(typeof(RavenHash), key));

                string result;
                if(!ravenHash.Fields.TryGetValue(name, out result))
                    return null;

                return result;
            }
        }

        public override long GetListCount(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var count = repository.Query<RavenList>()
                                .Where(t => t.Key == key)
                                .Count();

                return count;
            }
        }

        public override TimeSpan GetListTtl(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                // TODO: Implement!

                return TimeSpan.FromSeconds(-1);
            }
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var results = repository.Query<RavenList>()
                                .Where(t => t.Key == key)
                                .Select(t => t.Value)
                                .Skip(startingFrom - 1)
                                .Take(endingAt - startingFrom + 1)
                                .ToList();

                return results;
            }
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var results = repository.Query<RavenList>()
                                .Where(t => t.Key == key)
                                .Select(t => t.Value)
                                .ToList();

                return results;
            }
        }
    }
}
