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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Raven.Client;
using Hangfire.Raven.DistributedLock;
using HangFire.Raven;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;

namespace Hangfire.Raven
{
    public class RavenConnection : JobStorageConnection
    {
        private readonly RavenStorageOptions _options;
        private readonly RavenStorage _storage;

        public RavenConnection(RavenStorage ravenStorage)
        {
            ravenStorage.ThrowIfNull("RavenStorage");

            _storage = ravenStorage;
            _options = new RavenStorageOptions();
        }

        public RavenConnection(RavenStorage ravenStorage, RavenStorageOptions options)
            : this(ravenStorage)
        {
            options.ThrowIfNull("options");

            _storage = ravenStorage;
            _options = options;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new RavenWriteOnlyTransaction(_storage);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new RavenDistributedLock(_storage, string.Format("HangFire:{0}", resource), timeout, _options);
        }

        public static IDocumentStore _documentStore;

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

            var invocationData = InvocationData.Serialize(job);

            var ravenJob = new RavenJob
            {
                Id = Guid.NewGuid().ToString(),
                InvocationData = JobHelper.ToJson(invocationData),
                Arguments = invocationData.Arguments,
                CreatedAt = createdAt,
                ExpireAt = createdAt.Add(expireIn)
            };

            using (var repository = new Repository()) {
                repository.Save(ravenJob);

                if (parameters.Count > 0) {
                    foreach (var parameter in parameters) {
                        repository.Save(new JobParameter
                        {
                            JobId = ravenJob.Id,
                            Name = parameter.Key,
                            Value = parameter.Value
                        });
                    }
                }

                return ravenJob.Id.ToString();
            }
        }

        public override JobData GetJobData(string id)
        {
            id.ThrowIfNull("id");

            using (var repository = new Repository()) {
                var jobData = repository.Session.Load<RavenJob>(id);

                if (jobData == null) {
                    return null;
                }

                var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);
                invocationData.Arguments = jobData.Arguments;

                Job job = null;
                JobLoadException loadException = null;

                try {
                    job = invocationData.Deserialize();
                } catch (JobLoadException ex) {
                    loadException = ex;
                }

                return new JobData
                {
                    Job = job,
                    State = jobData.StateName,
                    CreatedAt = jobData.CreatedAt,
                    LoadException = loadException
                };
            }
        }

        public override StateData GetStateData(string jobId)
        {
            jobId.ThrowIfNull("jobId");

            using (var repository = new Repository()) {
                RavenJob job = repository.Session.Load<RavenJob>(jobId);
                if (job == null) {
                    return null;
                }

                State state = repository.Session.Query<State>().FirstOrDefault(t => t.Id == job.StateId);
                if (state == null) {
                    return null;
                }

                return new StateData
                {
                    Name = state.Name,
                    Reason = state.Reason,
                    Data = JobHelper.FromJson<Dictionary<string, string>>(state.Data)
                };
            }
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            id.ThrowIfNull("id");
            name.ThrowIfNull("name");

            using (var repository = new Repository()) {
                var results = repository.Session.Query<JobParameter>().Where(t => t.JobId == id && t.Name == name).ToList();

                results.ForEach(t => {
                    t.Value = value;
                    repository.Save(t);
                });
            }
        }

        public override string GetJobParameter(string id, string name)
        {
            id.ThrowIfNull("id");
            name.ThrowIfNull("name");

            using (var repository = new Repository()) {
                var jobParameter = repository.Session.Query<JobParameter>().FirstOrDefault(t => t.JobId == id && t.Name == name);

                return jobParameter != null ? jobParameter.Value : null;
            }
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var results = repository.Session.Query<RavenSet>().Where(t => t.Key == key).Select(t => t.Value).ToList();

                return new HashSet<string>(results);
            }
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            key.ThrowIfNull("key");

            if (toScore < fromScore) {
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            }

            using (var repository = new Repository()) {
                var set = repository.Session.Query<RavenSet>()
                                .Where(t => t.Key == key)
                                .Where(t => t.Score >= fromScore)
                                .Where(t => t.Score <= toScore)
                                .OrderBy(t => t.Score)
                                .FirstOrDefault();


                return set != null ? set.Value : null;
            }
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            key.ThrowIfNull("key");
            keyValuePairs.ThrowIfNull("keyValuePairs");

            using (var repository = new Repository()) {
                foreach (var keyValuePair in keyValuePairs) {
                    var results = repository.Session.Query<RavenHash>().Where(t => t.Key == key && t.Field == keyValuePair.Key).ToList();

                    results.ForEach(t =>
                    {
                        t.Value = keyValuePair.Value;
                        repository.Save(t);
                    });
                }
            }
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            key.ThrowIfNull("key");

            using (var session = new Repository().Session) {
                var result = session.Query<RavenHash>().Where(t => t.Key == key)
                        .ToDictionary(x => x.Field, x => x.Value);

                return result.Count != 0 ? result : null;
            }
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            serverId.ThrowIfNull("serverId");
            context.ThrowIfNull("context");

            var data = new ServerData
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTime.UtcNow,
            };

            using (var repository = new Repository()) {
                var servers = repository.Session.Query<RavenServer>()
                                .Where(t => t.Id == serverId)
                                .ToList();

                var queues = servers.SelectMany(t => JobHelper.FromJson<ServerData>(t.Data).Queues).ToList();
                queues.AddRange(data.Queues.Select(t => t).ToList());
                data.Queues = queues.ToArray();

                data.WorkerCount += servers.Select(t => JobHelper.FromJson<ServerData>(t.Data).WorkerCount).Sum(t => t);

                foreach (var item in servers) {
                    repository.Delete(item);
                }

                var server = new RavenServer
                {
                    Data = JobHelper.ToJson(data),
                    Id = serverId,
                    LastHeartbeat = DateTime.UtcNow
                };

                repository.Save(server);
            }
        }

        public override void RemoveServer(string serverId)
        {
            serverId.ThrowIfNull("serverId");

            using (var repository = new Repository()) {
                var servers = repository.Session.Query<RavenServer>()
                                .Where(t => t.Id == serverId)
                                .ToList();

                foreach (var item in servers) {
                    repository.Delete(item);
                }
            }
        }

        public override void Heartbeat(string serverId)
        {
            serverId.ThrowIfNull("serverId");

            using (var repository = new Repository()) {
                var server = repository.Session.Load<RavenServer>(serverId);

                if (server == null) {
                    server = new RavenServer
                    {
                        Id = serverId
                    };
                }

                server.LastHeartbeat = DateTime.UtcNow;

                repository.Save(server);
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut) {
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");
            }

            using (var repository = new Repository()) {
                var heartBeatCutOff = DateTime.UtcNow.Add(timeOut.Negate());

                var results = repository.Session.Query<RavenServer>()
                                .Where(t => t.LastHeartbeat < heartBeatCutOff)
                                .ToList();

                foreach (var item in results) {
                    repository.Delete(item);
                }

                return results.Count;
            }
        }

        public override long GetSetCount(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var count = repository.Session.Query<RavenSet>()
                                .Where(t => t.Key == key)
                                .Count();

                return count;
            }
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var values = repository.Session.Query<RavenSet>()
                                .Where(t => t.Key == key)
                                .Skip(startingFrom - 1)
                                .Take(endingAt - startingFrom + 1)
                                .Select(t => t.Value)
                                .ToList();

                return values;
            }
        }

        public override TimeSpan GetSetTtl(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var expireAt = repository.Session.Query<RavenSet>()
                                .Where(t => t.Key == key)
                                .Select(t => t.ExpireAt)
                                .ToArray();

                if (expireAt.Any() == false) {
                    return TimeSpan.FromSeconds(-1);
                }

                return expireAt.Where(t => t.HasValue).Min(t => t.Value) - DateTime.UtcNow;
            }
        }

        public override long GetCounter(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var values = repository.Session.Query<AggregatedCounter>()
                                .Where(t => t.Key == key)
                                .Select(t => t.Value)
                                .ToArray();

                return values.Any() ? values.Sum() : 0;
            }
        }

        public override long GetHashCount(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var count = repository.Session.Query<RavenHash>()
                                .Where(t => t.Key == key)
                                .Count();

                return count;
            }
        }

        public override TimeSpan GetHashTtl(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var hashes = repository.Session.Query<RavenHash>()
                                .Where(t => t.Key == key)
                                .ToList();

                DateTime? result = hashes.Any() ? hashes.Min(x => x.ExpireAt) : null;

                if (!result.HasValue) {
                    return TimeSpan.FromSeconds(-1);
                }

                return result.Value - DateTime.UtcNow;
            }
        }

        public override string GetValueFromHash(string key, string name)
        {
            key.ThrowIfNull("key");
            name.ThrowIfNull("name");

            using (var repository = new Repository()) {
                var result = repository.Session.Query<RavenHash>()
                                .Where(t => t.Key == key)
                                .Where(t => t.Field == name)
                                .FirstOrDefault();

                return result != null ? result.Value : null;
            }
        }

        public override long GetListCount(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var count = repository.Session.Query<RavenList>()
                                .Where(t => t.Key == key)
                                .Count();

                return count;
            }
        }

        public override TimeSpan GetListTtl(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var results = repository.Session.Query<RavenList>()
                                .Where(t => t.Key == key)
                                .ToList();

                DateTime? ttl = results.Any() ? results.Min(t => t.ExpireAt) : (DateTime?)null;

                if (!ttl.HasValue) {
                    return TimeSpan.FromSeconds(-1);
                }

                return ttl.Value - DateTime.UtcNow;
            }
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var results = repository.Session.Query<RavenList>()
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

            using (var repository = new Repository()) {
                var results = repository.Session.Query<RavenList>()
                                .Where(t => t.Key == key)
                                .Select(t => t.Value)
                                .ToList();

                return results;
            }
        }
    }
}
