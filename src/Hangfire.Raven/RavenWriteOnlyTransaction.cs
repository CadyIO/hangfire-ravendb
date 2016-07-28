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
using Hangfire.Storage.Monitoring;

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
            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenJob), jobId);
                var result = repository.Load<RavenJob>(id);

                result.ExpireAt = DateTime.UtcNow.Add(expireIn);

                repository.SaveChanges();
            }
        }

        public override void PersistJob(string jobId)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenJob), jobId);
                var result = repository.Load<RavenJob>(id);

                result.ExpireAt = null;

                repository.SaveChanges();
            }
        }

        public override void SetJobState(string jobId, IState state)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenJob), jobId);
                var result = repository.Load<RavenJob>(id);
                
                result.History.Add(new StateHistoryDto()
                {
                    StateName = state.Name,
                    Data = state.SerializeData(),
                    Reason = state.Reason,
                    CreatedAt = DateTime.UtcNow
                });

                result.StateData = new StateData()
                {
                    Name = state.Name,
                    Data = state.SerializeData(),
                    Reason = state.Reason
                };

                repository.SaveChanges();
            }
        }

        public override void AddJobState(string jobId, IState state)
        {
            this.SetJobState(jobId, state);
        }

        public override void AddToQueue(string queue, string jobId)
        {
            var provider = _storage.QueueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue();

            persistentQueue.Enqueue(queue, jobId);
        }

        public override void IncrementCounter(string key)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var counter = repository.Query<Counter>().FirstOrDefault(t => t.Key == key);

                if (counter == null) {
                    counter = new Counter
                    {
                        Id = Guid.NewGuid().ToString(),
                        Key = key,
                        Value = 0
                    };
                }

                counter.Value += 1;

                repository.Store(counter);
                repository.SaveChanges();
            }
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var counter = repository.Query<Counter>().FirstOrDefault(t => t.Key == key);

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

                repository.Store(counter);
                repository.SaveChanges();
            }
        }

        public override void DecrementCounter(string key)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var counter = repository.Query<Counter>().FirstOrDefault(t => t.Key == key);

                if (counter == null) {
                    counter = new Counter
                    {
                        Id = Guid.NewGuid().ToString(),
                        Key = key,
                        Value = 0
                    };
                }

                counter.Value -= 1;

                repository.Store(counter);
                repository.SaveChanges();
            }
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var counter = repository.Query<Counter>().FirstOrDefault(t => t.Key == key);

                if (counter == null)
                {
                    counter = new Counter
                    {
                        Id = Guid.NewGuid().ToString(),
                        ExpireAt = DateTime.UtcNow.Add(expireIn),
                        Key = key,
                        Value = 0
                    };
                }

                counter.Value -= 1;

                repository.Store(counter);
                repository.SaveChanges();
            }
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenSet), key);
                var set = repository.Load<RavenSet>(id);

                if (set == null)
                {
                    set = new RavenSet()
                    {
                        Id = id
                    };
                    repository.Store(set);
                }

                set.Scores[value] = score;
                repository.SaveChanges();
            }
        }

        public override void RemoveFromSet(string key, string value)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenSet), key);

                var set = repository.Load<RavenSet>(id);
                set.Scores.Remove(value);

                if(set.Scores.Count == 0)
                { 
                    repository.Delete(id);
                }
                repository.SaveChanges();
            }
        }

        public override void InsertToList(string key, string value)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var list = repository.Query<RavenList>().FirstOrDefault(t => t.Key == key && t.Value == value);

                if (list == null) {
                    list = new RavenList
                    {
                        Id = Guid.NewGuid().ToString(),
                        Key = key
                    };
                }

                list.Value = value;

                repository.Store(list);
                repository.SaveChanges();
            }
        }

        public override void RemoveFromList(string key, string value)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var list = repository.Query<RavenList>().FirstOrDefault(t => t.Key == key && t.Value == value);

                if (list == null) {
                    return;
                }

                repository.Delete(list);
                repository.SaveChanges();
            }
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var list = repository.Query<RavenList>().Where(t => t.Key == key).ToList();

                if (!list.Any()) {
                    return;
                }

                for (var i = 0; i < list.Count; ++i) {
                    if (i >= keepStartingFrom && i <= keepEndingAt) {
                        continue;
                    }

                    repository.Delete(list[i]);
                }

                repository.SaveChanges();
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

                // Not exists? Insert
                if(result == null)
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

        public override void RemoveHash(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                repository.Delete(Repository.GetId(typeof(RavenHash), key));
                repository.SaveChanges();
            }
        }

        public override void RemoveSet(string key)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenSet), key);
                repository.Delete(id);
                repository.SaveChanges();
            }
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            key.ThrowIfNull("key");

            using (var repository = _storage.Repository.OpenSession())
            {
                var id = Repository.GetId(typeof(RavenSet), key);
                var set = repository.Load<RavenSet>(id);

                set.ExpireAt = DateTime.UtcNow.Add(expireIn);
                repository.SaveChanges();
            }
        }

        internal void QueueCommand(Action<RavenConnection> action)
        {
            _commandQueue.Enqueue(action);
        }
    }
}