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
using Hangfire.Raven.DistributedLocks;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.Server;
using Hangfire.Storage;
using Hangfire.Raven.Extensions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Commands.Batches;

namespace Hangfire.Raven {
    public class RavenConnection : JobStorageConnection {
        private readonly RavenStorage _storage;

        public RavenConnection(RavenStorage storage) {
            storage.ThrowIfNull("storage");

            _storage = storage;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction() => new RavenWriteOnlyTransaction(_storage);

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout) => new RavenDistributedLock(_storage, $"HangFire/{resource}", timeout, _storage.Options);

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken) {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException(nameof(queues));

            var providers = queues
                .Select(queue => _storage.QueueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1) {
                throw new InvalidOperationException(
                    $"Multiple provider instances registered for queues: {String.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
            }

            var persistentQueue = providers[0].GetJobQueue();
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public override string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters,
            DateTime createdAt,
            TimeSpan expireIn) {
            job.ThrowIfNull(nameof(job));
            parameters.ThrowIfNull(nameof(parameters));

            var invocationData = InvocationData.Serialize(job);
            var guid = Guid.NewGuid().ToString();
            var ravenJob = new RavenJob {
                Id = _storage.Repository.GetId(typeof(RavenJob), guid),
                InvocationData = invocationData,
                CreatedAt = createdAt,
                Parameters = parameters
            };

            using (var session = _storage.Repository.OpenSession()) {
                session.Store(ravenJob);
                session.SetExpiry(ravenJob, createdAt + expireIn);
                session.SaveChanges();
                return guid;
            }
        }

        public override JobData GetJobData(string key) {
            key.ThrowIfNull(nameof(key));

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenJob), key);
                var jobData = repository.Load<RavenJob>(id);

                if (jobData == null) return null;

                Job job = null;
                JobLoadException loadException = null;

                try {
                    job = jobData.InvocationData.Deserialize();
                } catch (JobLoadException ex) {
                    loadException = ex;
                }

                return new JobData {
                    Job = job,
                    State = jobData.StateData?.Name,
                    CreatedAt = jobData.CreatedAt,
                    LoadException = loadException
                };
            }
        }

        public override StateData GetStateData(string jobId) {
            jobId.ThrowIfNull(nameof(jobId));

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
                var job = repository.Load<RavenJob>(id);

                if (job == null) {
                    return null;
                }

                return job.StateData;
            }
        }

        public override void SetJobParameter(string jobId, string name, string value) {
            jobId.ThrowIfNull(nameof(jobId));
            name.ThrowIfNull(nameof(name));

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
                var job = repository.Load<RavenJob>(id);

                job.Parameters[name] = value;

                repository.SaveChanges();
            }
        }

        public override string GetJobParameter(string jobId, string name) {
            jobId.ThrowIfNull(nameof(jobId));
            name.ThrowIfNull(nameof(name));

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
                var job = repository.Load<RavenJob>(id);

                if (job == null) {
                    return null;
                }


                if (!job.Parameters.TryGetValue(name, out string value)) {
                    if (name == "RetryCount") {
                        job.Parameters["RetryCount"] = "0";
                        repository.SaveChanges();

                        return "0";
                    }

                    return null;
                }

                return value;
            }
        }

        public override HashSet<string> GetAllItemsFromSet(string key) {
            key.ThrowIfNull(nameof(key));

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenSet), key);
                var set = repository.Load<RavenSet>(id);

                if (set == null) {
                    return new HashSet<string>();
                }

                return new HashSet<string>(set.Scores.Keys);
            }
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore) {
            key.ThrowIfNull(nameof(key));

            if (toScore < fromScore) {
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            }

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenSet), key);
                var set = repository.Load<RavenSet>(id);

                if (set == null) {
                    return null;
                }

                return set.Scores.Where(a => a.Value >= fromScore && a.Value <= toScore).OrderBy(a => a.Value).Select(a => a.Key).FirstOrDefault();
            }
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs) {
            key.ThrowIfNull(nameof(key));
            keyValuePairs.ThrowIfNull("keyValuePairs");

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenHash), key);
                var result = repository.Load<RavenHash>(id);

                if (result == null) {
                    result = new RavenHash() {
                        Id = id
                    };

                    repository.Store(result);
                }

                foreach (var keyValuePair in keyValuePairs) {
                    result.Fields[keyValuePair.Key] = keyValuePair.Value;
                }

                repository.SaveChanges();
            }
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key) {
            key.ThrowIfNull(nameof(key));

            using (var repository = _storage.Repository.OpenSession()) {
                var result = repository.Load<RavenHash>(_storage.Repository.GetId(typeof(RavenHash), key));

                return result?.Fields;
            }
        }

        public override void AnnounceServer(string serverId, ServerContext context) {
            serverId.ThrowIfNull(nameof(serverId));
            context.ThrowIfNull(nameof(context));

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenServer), serverId);
                var server = repository.Load<RavenServer>(id);

                if (server == null) {
                    server = new RavenServer() {
                        Id = id,
                        Data = new RavenServer.ServerData() {
                            StartedAt = DateTime.UtcNow
                        }
                    };

                    repository.Store(server);
                }

                server.Data.WorkerCount = context.WorkerCount;
                server.Data.Queues = context.Queues;
                server.Data.StartedAt = DateTime.UtcNow;

                server.LastHeartbeat = DateTime.UtcNow;

                repository.SaveChanges();
            }
        }

        public override void RemoveServer(string serverId) {
            serverId.ThrowIfNull(nameof(serverId));

            using (var session = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenServer), serverId);

                session.Delete(id);

                session.SaveChanges();
            }
        }

        public override void Heartbeat(string serverId) {
            serverId.ThrowIfNull(nameof(serverId));

            using (var session = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenServer), serverId);

                //todo rewrite as patch
                var server = session.Load<RavenServer>(id);

                if (server == null) {
                    server = new RavenServer {
                        Id = id
                    };

                    session.Store(server);
                }

                server.LastHeartbeat = DateTime.UtcNow;

                session.SaveChanges();
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut) {
            if (timeOut.Duration() != timeOut) {
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");
            }

            using (var session = _storage.Repository.OpenSession()) {
                var heartBeatCutOff = DateTime.UtcNow.Add(timeOut.Negate());

                var results = session.Query<RavenServer>()
                                .Where(t => t.LastHeartbeat < heartBeatCutOff)
                                .ToList();

                foreach (var item in results)
                    session.Delete(item);

                session.SaveChanges();

                return results.Count;
            }
        }

        public override long GetSetCount(string key) {
            key.ThrowIfNull(nameof(key));

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenSet), key);
                var set = repository.Load<RavenSet>(id);

                if (set == null) {
                    return 0;
                }

                return set.Scores.Count;
            }
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt) {
            key.ThrowIfNull(nameof(key));

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenSet), key);
                var set = repository.Load<RavenSet>(id);

                if (set == null) {
                    return new List<string>();
                }

                return set.Scores.Skip(startingFrom)
                            .Take(endingAt - startingFrom + 1)
                            .Select(t => t.Key)
                            .ToList();
            }
        }

        public override TimeSpan GetSetTtl(string key) {
            key.ThrowIfNull(nameof(key));

            using (var session = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenSet), key);
                var set = session.Load<RavenSet>(id);

                if (set == null) {
                    return TimeSpan.FromSeconds(-1);
                }


                var expireAt = session.GetExpiry(set);

                if (expireAt == null)
                    return TimeSpan.FromSeconds(-1);

                return (DateTime)expireAt - DateTime.UtcNow;
            }
        }

        public override long GetCounter(string key) {
            key.ThrowIfNull(nameof(key));

            using (var session = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(Counter), key);
                var counter = session.Load<Counter>(id);

                if (counter == null) {
                    return 0;
                }

                return counter.Value;
            }
        }

        public override long GetHashCount(string key) {
            key.ThrowIfNull(nameof(key));

            using (var session = _storage.Repository.OpenSession()) {
                var ravenHash = session.Load<RavenHash>(_storage.Repository.GetId(typeof(RavenHash), key));

                if (ravenHash == null) {
                    return 0;
                }

                return ravenHash.Fields.Count;
            }
        }

        public override TimeSpan GetHashTtl(string key) {
            key.ThrowIfNull(nameof(key));

            using (var session = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenHash), key);
                var ravenHash = session.Load<RavenHash>(id);

                if (ravenHash == null) {
                    return TimeSpan.FromSeconds(-1);
                }

                var expireAt = session.GetExpiry(ravenHash);

                if (expireAt == null)
                    return TimeSpan.FromSeconds(-1);

                return (DateTime)expireAt - DateTime.UtcNow;
            }
        }

        public override string GetValueFromHash(string key, string name) {
            key.ThrowIfNull(nameof(key));
            name.ThrowIfNull(nameof(name));

            using (var repository = _storage.Repository.OpenSession()) {
                var ravenHash = repository.Load<RavenHash>(_storage.Repository.GetId(typeof(RavenHash), key));

                if (ravenHash == null) {
                    return null;
                }


                if (!ravenHash.Fields.TryGetValue(name, out string result)) {
                    return null;
                }

                return result;
            }
        }

        public override long GetListCount(string key) {
            key.ThrowIfNull(nameof(key));

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenList), key);
                var list = repository.Load<RavenList>(id);

                if (list == null) {
                    return 0;
                }

                return list.Values.Count;
            }
        }

        public override TimeSpan GetListTtl(string key) {
            key.ThrowIfNull(nameof(key));

            using (var session = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenList), key);
                var list = session.Load<RavenList>(id);

                if (list == null) {
                    return TimeSpan.FromSeconds(-1);
                }

                var expireAt = session.GetExpiry(list);

                if (expireAt == null)
                    return TimeSpan.FromSeconds(-1);

                return (DateTime)expireAt - DateTime.UtcNow;
            }
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt) {
            key.ThrowIfNull(nameof(key));

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenList), key);
                var list = repository.Load<RavenList>(id);

                if (list == null) {
                    return new List<string>();
                }

                return list.Values.Skip(startingFrom)
                            .Take(endingAt - startingFrom + 1)
                            .ToList();
            }
        }

        public override List<string> GetAllItemsFromList(string key) {
            key.ThrowIfNull(nameof(key));

            using (var repository = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenList), key);
                var list = repository.Load<RavenList>(id);

                if (list == null) {
                    return new List<string>();
                }

                return list.Values;
            }
        }
    }
}
