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
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using HangFire.Raven;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;

namespace Hangfire.Raven
{
    internal class RavenWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly Queue<Action<RavenConnection>> _commandQueue
            = new Queue<Action<RavenConnection>>();

        private readonly SortedSet<string> _lockedResources = new SortedSet<string>();
        private readonly RavenStorage _storage;
        public RavenWriteOnlyTransaction([NotNull] RavenStorage storage)
        {
            storage.ThrowIfNull("storage");

            _storage = storage;
        }

        public override void Commit()
        {
            foreach (var command in _commandQueue) {
                command(new RavenConnection(_storage));
            }
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            using (var repository = new Repository()) {
                var result = repository.Session.Load<RavenJob>(jobId);

                result.ExpireAt = DateTime.UtcNow.Add(expireIn);

                repository.Save(result);
            }
        }

        public override void PersistJob(string jobId)
        {
            using (var repository = new Repository()) {
                var result = repository.Session.Load<RavenJob>(jobId);

                result.ExpireAt = null;

                repository.Save(result);
            }
        }

        public override void SetJobState(string jobId, IState state)
        {
            using (var repository = new Repository()) {
                var jobState = repository.Session.Query<State>().FirstOrDefault(t => t.JobId == jobId);

                if (jobState == null) {
                    jobState = new State
                    {
                        CreatedAt = DateTime.UtcNow,
                        JobId = jobId
                    };
                }

                jobState.Reason = state.Reason;
                jobState.Name = state.Name;
                jobState.Data = JobHelper.ToJson(state.SerializeData());

                repository.Save(jobState);

                var result = repository.Session.Load<RavenJob>(jobId);

                result.StateId = jobState.Id;
                result.StateName = state.Name;
                result.StateData = JobHelper.ToJson(state.SerializeData());
                result.StateReason = state.Reason;

                repository.Save(result);
            }
        }

        public override void AddJobState(string jobId, IState state)
        {
            using (var repository = new Repository()) {
                var jobState = new State
                {
                    Id = Guid.NewGuid().ToString(),
                    JobId = jobId,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = DateTime.UtcNow,
                    Data = JobHelper.ToJson(state.SerializeData())
                };

                repository.Save(jobState);
            }
        }

        public override void AddToQueue(string queue, string jobId)
        {
            var provider = _storage.QueueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue();

            persistentQueue.Enqueue(queue, jobId);
        }

        public override void IncrementCounter(string key)
        {
            using (var repository = new Repository()) {
                var counter = repository.Session.Query<Counter>().FirstOrDefault(t => t.Key == key);

                if (counter == null) {
                    counter = new Counter
                    {
                        Id = Guid.NewGuid().ToString(),
                        Key = key,
                        Value = 0
                    };
                }

                counter.Value += 1;

                repository.Save(counter);
            }
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            using (var repository = new Repository()) {
                var counter = repository.Session.Query<Counter>().FirstOrDefault(t => t.Key == key);

                if (counter == null) {
                    counter = new Counter
                    {
                        Id = Guid.NewGuid().ToString(),
                        ExpireAt = DateTime.UtcNow.Add(expireIn),
                        Key = key,
                        Value = 0
                    };
                }

                counter.Value += 1;

                repository.Save(counter);
            }
        }

        public override void DecrementCounter(string key)
        {
            using (var repository = new Repository()) {
                var counter = repository.Session.Query<Counter>().FirstOrDefault(t => t.Key == key);

                if (counter == null) {
                    counter = new Counter
                    {
                        Id = Guid.NewGuid().ToString(),
                        Key = key,
                        Value = 0
                    };
                }

                counter.Value -= 1;

                repository.Save(counter);
            }
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            using (var repository = new Repository()) {
                var counter = repository.Session.Query<Counter>().FirstOrDefault(t => t.Key == key);

                if (counter == null) {
                    counter = new Counter
                    {
                        Id = Guid.NewGuid().ToString(),
                        ExpireAt = DateTime.UtcNow.Add(expireIn),
                        Key = key,
                        Value = 0
                    };
                }

                counter.Value -= 1;

                repository.Save(counter);
            }
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            using (var repository = new Repository()) {
                var set = repository.Session.Query<RavenSet>().FirstOrDefault(t => t.Key == key && t.Value == value);

                if (set == null) {
                    set = new RavenSet
                    {
                        Id = Guid.NewGuid().ToString(),
                        Key = key,
                        Value = value
                    };
                }

                set.Score = score;

                repository.Save(set);
            }
        }

        public override void RemoveFromSet(string key, string value)
        {
            using (var repository = new Repository()) {
                var set = repository.Session.Query<RavenSet>().FirstOrDefault(t => t.Key == key && t.Value == value);

                if (set == null) {
                    return;
                }

                repository.Delete(set);
            }
        }

        public override void InsertToList(string key, string value)
        {
            using (var repository = new Repository()) {
                var list = repository.Session.Query<RavenList>().FirstOrDefault(t => t.Key == key && t.Value == value);

                if (list == null) {
                    list = new RavenList
                    {
                        Id = Guid.NewGuid().ToString(),
                        Key = key
                    };
                }

                list.Value = value;

                repository.Save(list);
            }
        }

        public override void RemoveFromList(string key, string value)
        {
            using (var repository = new Repository()) {
                var list = repository.Session.Query<RavenList>().FirstOrDefault(t => t.Key == key && t.Value == value);

                if (list == null) {
                    return;
                }

                repository.Delete(list);
            }
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            using (var repository = new Repository()) {
                var list = repository.Session.Query<RavenList>().Where(t => t.Key == key).ToList();

                if (!list.Any()) {
                    return;
                }

                for (var i = 0; i < list.Count; ++i) {
                    if (i >= keepStartingFrom && i <= keepEndingAt) {
                        continue;
                    }

                    repository.Delete(list[i]);
                }
            }
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            key.ThrowIfNull("key");
            keyValuePairs.ThrowIfNull("keyValuePairs");

            using (var repository = new Repository()) {
                foreach (var keyValuePair in keyValuePairs) {
                    var pair = keyValuePair;

                    var results = repository.Session.Query<RavenHash>().Where(t => t.Key == key && t.Field == pair.Key).ToList();

                    if (!results.Any()) {
                        results.Add(new RavenHash
                        {
                            Key = key,
                            Field = pair.Key,
                        });
                    }

                    foreach (var item in results) {
                        item.Value = pair.Value;

                        repository.Save(item);
                    }
                }
            }
        }

        public override void RemoveHash(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var result = repository.Session.Query<RavenHash>().Where(t => t.Key == key).ToList();

                foreach (var item in result) {
                    repository.Session.Delete(item);
                }
            }
        }

        public override void RemoveSet(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var result = repository.Session.Query<RavenSet>().Where(t => t.Key == key).ToList();

                foreach (var item in result) {
                    repository.Session.Delete(item);
                }
            }
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            key.ThrowIfNull("key");

            using (var repository = new Repository()) {
                var result = repository.Session.Query<RavenSet>().Where(t => t.Key == key).ToList();

                foreach (var item in result) {
                    item.ExpireAt = DateTime.UtcNow.Add(expireIn);

                    repository.Save(item);
                }
            }
        }

        internal void QueueCommand(Action<RavenConnection> action)
        {
            _commandQueue.Enqueue(action);
        }
    }
}