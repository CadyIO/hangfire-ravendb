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
using Raven.Client;
using Raven.Abstractions.Data;

namespace Hangfire.Raven
{
    internal class RavenWriteOnlyTransaction 
        : JobStorageTransaction
    {
        private readonly RavenStorage _storage;
        private IDocumentSession _session;
        private List<KeyValuePair<string,PatchRequest>> _patchRequests;

        public RavenWriteOnlyTransaction([NotNull] RavenStorage storage)
        {
            storage.ThrowIfNull("storage");
            _storage = storage;

            _patchRequests = new List<KeyValuePair<string, PatchRequest>>();
            _session = _storage.Repository.OpenSession();
            _session.Advanced.UseOptimisticConcurrency = true;
        }

        public override void Commit()
        {
            try
            {
                _session.SaveChanges();
                _session.Dispose();

                var toPatch = _patchRequests.ToLookup(a => a.Key, a => a.Value);
                foreach(var item in toPatch)
                {
                    _session.Advanced.DocumentStore.DatabaseCommands.Patch(item.Key, item.ToArray());
                }
            }
            catch 
            {
                Console.WriteLine("- Concurrency exception");
                throw;
            }
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            var id = Repository.GetId(typeof(RavenJob), jobId);
            var result = _session.Load<RavenJob>(id);

            result.ExpireAt = DateTime.UtcNow.Add(expireIn);
        }

        public override void PersistJob(string jobId)
        {
            var id = Repository.GetId(typeof(RavenJob), jobId);
            var result = _session.Load<RavenJob>(id);

            result.ExpireAt = null;
        }

        public override void SetJobState(string jobId, IState state)
        {
            var id = Repository.GetId(typeof(RavenJob), jobId);
            var result = _session.Load<RavenJob>(id);
                
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
            this.IncrementCounter(key, TimeSpan.MinValue);
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            var id = Repository.GetId(typeof(Counter), key);
            if (_session.Load<Counter>(id) == null)
            {
                var counter = new Counter()
                {
                    Id = id,
                    ExpireAt = expireIn != TimeSpan.MinValue ?
                        DateTime.UtcNow.Add(expireIn) : (DateTime?)null,
                    Value = 1
                };
                _session.Store(counter);
            }
            else
            {
                _patchRequests.Add(new KeyValuePair<string, PatchRequest>(id, new PatchRequest()
                {
                    Type = PatchCommandType.Inc,
                    Name = "Value",
                    Value = 1
                }));
            }
        }

        public override void DecrementCounter(string key)
        {
            this.DecrementCounter(key, TimeSpan.MinValue);
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            var id = Repository.GetId(typeof(Counter), key);
            if (_session.Load<Counter>(id) == null)
            {
                var counter = new Counter()
                {
                    Id = id,
                    ExpireAt = expireIn != TimeSpan.MinValue ?
                        DateTime.UtcNow.Add(expireIn) : (DateTime?)null,
                    Value = -1
                };
                _session.Store(counter);
            }
            else
            {
                _patchRequests.Add(new KeyValuePair<string, PatchRequest>(id, new PatchRequest()
                {
                    Type = PatchCommandType.Inc,
                    Name = "Value",
                    Value = -1
                }));
            }
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            var id = Repository.GetId(typeof(RavenSet), key);
            var set = _session.Load<RavenSet>(id) ?? new RavenSet()
            {
                Id = id
            };

            _session.Store(set);
            set.Scores[value] = score;
        }

        public override void RemoveFromSet(string key, string value)
        {
            var id = Repository.GetId(typeof(RavenSet), key);

            var set = _session.Load<RavenSet>(id);
            set.Scores.Remove(value);

            if(set.Scores.Count == 0)
            {
                _session.Delete(set);
            }
        }

        public override void RemoveSet(string key)
        {
            key.ThrowIfNull("key");

            var id = Repository.GetId(typeof(RavenSet), key);
            _session.Delete(id);
        }

        public override void InsertToList(string key, string value)
        {
            var list = _session.Query<RavenList>().FirstOrDefault(t => t.Key == key && t.Value == value);

            if (list == null) {
                list = new RavenList
                {
                    Id = Guid.NewGuid().ToString(),
                    Key = key
                };
            }

            list.Value = value;
            _session.Store(list);
        }

        public override void RemoveFromList(string key, string value)
        {
            var list = _session.Query<RavenList>().FirstOrDefault(t => t.Key == key && t.Value == value);

            if (list == null) {
                return;
            }

            _session.Delete(list);
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            var list = _session.Query<RavenList>().Where(t => t.Key == key).ToList();

            if (!list.Any()) {
                return;
            }

            for (var i = 0; i < list.Count; ++i) {
                if (i >= keepStartingFrom && i <= keepEndingAt) {
                    continue;
                }

                _session.Delete(list[i]);
            }
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            key.ThrowIfNull("key");
            keyValuePairs.ThrowIfNull("keyValuePairs");
            
            var id = Repository.GetId(typeof(RavenHash), key);
            var result = _session.Load<RavenHash>(id);

            // Not exists? Insert
            if(result == null)
            {
                result = new RavenHash()
                {
                    Id = id
                };
                _session.Store(result);
            }

            foreach (var keyValuePair in keyValuePairs)
            {
                result.Fields[keyValuePair.Key] = keyValuePair.Value;
            }
        }

        public override void RemoveHash(string key)
        {
            key.ThrowIfNull("key");

            _session.Delete(Repository.GetId(typeof(RavenHash), key));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            key.ThrowIfNull("key");

            var id = Repository.GetId(typeof(RavenSet), key);
            var set = _session.Load<RavenSet>(id);

            set.ExpireAt = DateTime.UtcNow.Add(expireIn);
        }
    }
}