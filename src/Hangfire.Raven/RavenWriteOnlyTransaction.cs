using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Hangfire.Logging;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client;

namespace Hangfire.Raven
{
    public class RavenWriteOnlyTransaction
        : JobStorageTransaction
    {
        private static readonly ILog Logger = LogProvider.For<RavenWriteOnlyTransaction>();

        private readonly RavenStorage _storage;
        private IDocumentSession _session;
        private List<KeyValuePair<string, PatchRequest>> _patchRequests;

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
            try {
                _session.SaveChanges();
                _session.Dispose();

                var toPatch = _patchRequests.ToLookup(a => a.Key, a => a.Value);

                foreach (var item in toPatch) {
                    _session.Advanced.Defer(item.Select(x => new PatchCommandData(item.Key, null, x, null)).ToArray());
                }
            } catch {
                Logger.Error("- Concurrency exception");

                throw;
            }
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
            var result = _session.Load<RavenJob>(id);

            //_session.Advanced.AddExpire(result, DateTime.UtcNow + expireIn);
        }

        public override void PersistJob(string jobId)
        {
            var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
            var result = _session.Load<RavenJob>(id);

            //_session.Advanced.RemoveExpire(result);
        }

        public override void SetJobState(string jobId, IState state)
        {
            var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
            var result = _session.Load<RavenJob>(id);

            result.History.Insert(0, new StateHistoryDto() {
                StateName = state.Name,
                Data = state.SerializeData(),
                Reason = state.Reason,
                CreatedAt = DateTime.UtcNow
            });

            result.StateData = new StateData() {
                Name = state.Name,
                Data = state.SerializeData(),
                Reason = state.Reason
            };
        }

        public override void AddJobState(string jobId, IState state)
        {
            SetJobState(jobId, state);
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            key.ThrowIfNull("key");
            items.ThrowIfNull("items");

            var id = _storage.Repository.GetId(typeof(RavenSet), key);

            var set = _session.Load<RavenSet>(id) ?? new RavenSet() {
                Id = id
            };

            _session.Store(set);

            foreach (var item in items) {
                set.Scores[item] = 0.0;
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
            IncrementCounter(key, TimeSpan.MinValue);
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            var id = _storage.Repository.GetId(typeof(Counter), key);

            if (_session.Load<Counter>(id) == null) {
                var counter = new Counter() {
                    Id = id,
                    Value = 1
                };

                _session.Store(counter);

                /*if (expireIn != TimeSpan.MinValue) {
                    _session.Advanced.AddExpire(counter, DateTime.UtcNow + expireIn);
                }*/
            } else {
                _patchRequests.Add(new KeyValuePair<string, PatchRequest>(id, new PatchRequest() {
                    Script = @"this.Value += 1"
                }));
            }
        }

        public override void DecrementCounter(string key)
        {
            DecrementCounter(key, TimeSpan.MinValue);
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            var id = _storage.Repository.GetId(typeof(Counter), key);

            if (_session.Load<Counter>(id) == null) {
                var counter = new Counter() {
                    Id = id,
                    Value = -1
                };

                _session.Store(counter);

                if (expireIn != TimeSpan.MinValue) {
                    var metadata = _session.Advanced.GetMetadataFor(id);
                    metadata[Constants.Documents.Metadata.Expires] = (DateTime.UtcNow + expireIn).ToString("O");
                }
            } else {
                _patchRequests.Add(new KeyValuePair<string, PatchRequest>(id, new PatchRequest() {
                    Script = @"this.Value -= 1"
                }));
            }
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            var id = _storage.Repository.GetId(typeof(RavenSet), key);

            var set = _session.Load<RavenSet>(id) ?? new RavenSet() {
                Id = id
            };

            _session.Store(set);

            set.Scores[value] = score;
        }

        public override void RemoveFromSet(string key, string value)
        {
            var id = _storage.Repository.GetId(typeof(RavenSet), key);

            var set = _session.Load<RavenSet>(id);

            if (set == null) {
                return;
            }

            set.Scores.Remove(value);

            if (set.Scores.Count == 0) {
                _session.Delete(set);
            }
        }

        public override void RemoveSet(string key)
        {
            key.ThrowIfNull("key");

            var id = _storage.Repository.GetId(typeof(RavenSet), key);

            _session.Delete(id);
        }

        public override void ExpireSet([NotNull] string key, TimeSpan expireIn)
        {
            key.ThrowIfNull("key");

            var id = _storage.Repository.GetId(typeof(RavenSet), key);
            var set = _session.Load<RavenSet>(id);

            //_session.Advanced.AddExpire(set, DateTime.UtcNow + expireIn);
        }

        public override void PersistSet([NotNull] string key)
        {
            key.ThrowIfNull("key");

            var id = _storage.Repository.GetId(typeof(RavenSet), key);
            var set = _session.Load<RavenSet>(id);

            //_session.Advanced.RemoveExpire(set);
        }


        public override void InsertToList(string key, string value)
        {
            var id = _storage.Repository.GetId(typeof(RavenList), key);

            var list = _session.Load<RavenList>(id) ?? new RavenList() {
                Id = id
            };

            _session.Store(list);

            list.Values.Add(value);
        }

        public override void RemoveFromList(string key, string value)
        {
            var id = _storage.Repository.GetId(typeof(RavenList), key);

            var list = _session.Load<RavenList>(id);

            if (list == null) {
                return;
            }

            list.Values.RemoveAll(v => v == value);

            if (list.Values.Count == 0) {
                _session.Delete(list);
            }
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            var id = _storage.Repository.GetId(typeof(RavenList), key);

            var list = _session.Load<RavenList>(id);

            if (list == null) {
                return;
            }

            list.Values = list.Values.Skip(keepStartingFrom).Take(keepEndingAt - keepStartingFrom + 1).ToList();

            if (list.Values.Count == 0) {
                _session.Delete(list);
            }
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            key.ThrowIfNull("key");

            var id = _storage.Repository.GetId(typeof(RavenList), key);
            var list = _session.Load<RavenList>(id);

            //_session.Advanced.AddExpire(list, DateTime.UtcNow + expireIn);
        }

        public override void PersistList(string key)
        {
            key.ThrowIfNull("key");

            var id = _storage.Repository.GetId(typeof(RavenList), key);
            var list = _session.Load<RavenList>(id);

            //_session.Advanced.RemoveExpire(list);
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            key.ThrowIfNull("key");
            keyValuePairs.ThrowIfNull("keyValuePairs");

            var id = _storage.Repository.GetId(typeof(RavenHash), key);
            var result = _session.Load<RavenHash>(id);

            // Not exists? Insert
            if (result == null) {
                result = new RavenHash() {
                    Id = id
                };

                _session.Store(result);
            }

            foreach (var keyValuePair in keyValuePairs) {
                result.Fields[keyValuePair.Key] = keyValuePair.Value;
            }
        }

        public override void RemoveHash(string key)
        {
            key.ThrowIfNull("key");

            _session.Delete(_storage.Repository.GetId(typeof(RavenHash), key));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            key.ThrowIfNull("key");

            var id = _storage.Repository.GetId(typeof(RavenHash), key);
            var set = _session.Load<RavenHash>(id);

            //_session.Advanced.AddExpire(set, DateTime.UtcNow + expireIn);
        }

        public override void PersistHash([NotNull] string key)
        {
            key.ThrowIfNull("key");

            var id = _storage.Repository.GetId(typeof(RavenHash), key);
            var set = _session.Load<RavenHash>(id);

            //_session.Advanced.RemoveExpire(set);
        }
    }
}