using Raven.Client;
using System;
using Raven.Client.Document;
using Raven.Client.Embedded;
using System.Threading.Tasks;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using System.Collections.Concurrent;
using Raven.Client.Linq;
using System.Linq;
using System.Linq.Expressions;
using Hangfire.Raven.Entities.Identity;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Listeners;
using Dewey.Types;

namespace HangFire.Raven
{
    public class Repository : IDisposable
    {
        public static string ConnectionString { get; set; }
        public static string ConnectionUrl { get; set; }
        public static string DefaultDatabase { get; set; }
        public static string APIKey { get; set; }

        private static IDocumentStore _documentStore;
        private IDocumentSession _session;
        private IAsyncDocumentSession _asyncSession;
        private string _database;
        private string _apiKey;

        public static bool Embedded { get; set; }

        public string Database => _database ?? DefaultDatabase;

        public string ApiKeyAccess => _apiKey ?? string.Empty;

        public void Destroy()
        {
            if (!DocumentStore.DatabaseExists(Database)) {
                return;
            }

            DocumentStore.DatabaseCommands.GlobalAdmin.DeleteDatabase(Database, hardDelete: true);
        }

        public void Create()
        {
            if (DocumentStore.DatabaseExists(Database)) {
                return;
            }

            DocumentStore.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists(Database);
        }

        public IDocumentStore DocumentStore
        {
            get
            {
                if (_documentStore == null) {
                    if (Embedded) {
                        _documentStore = new EmbeddableDocumentStore
                        {
                            RunInMemory = true
                        };

                        ((EmbeddableDocumentStore)_documentStore).Configuration.Storage.Voron.AllowOn32Bits = true;
                    } else {
                        if (!string.IsNullOrEmpty(ConnectionString))
                        {
                            _documentStore = new DocumentStore
                            {
                                ConnectionStringName = ConnectionString,
                                ApiKey = APIKey

                            };
                        }
                        else
                        {
                            _documentStore = new DocumentStore
                            {
                                Url = ConnectionUrl,
                                ApiKey = APIKey
                            };
                        }
                    }

                    _documentStore.Conventions.DefaultQueryingConsistency = ConsistencyOptions.AlwaysWaitForNonStaleResultsAsOfLastWrite;
                    _documentStore.Listeners.RegisterListener(new TakeNewestConflictResolutionListener());
                    _documentStore.Conventions.DefaultQueryingConsistency = ConsistencyOptions.AlwaysWaitForNonStaleResultsAsOfLastWrite;

                    _documentStore.Initialize();
                }

                return _documentStore;
            }
        }

        public IDocumentSession Session => _session ?? (_session = DocumentStore.OpenSession(Database));

        public IAsyncDocumentSession AsyncSession => _asyncSession ?? (_asyncSession = DocumentStore.OpenAsyncSession(Database));

        #region constructor

        public Repository()
        {
            _database = DefaultDatabase;
            _apiKey = APIKey;
        }

        public Repository(string database, string APIKey)
        {
            if (database.IsEmpty()) {
                _database = DefaultDatabase;
            } else {
                _database = database;                
            }

            if (APIKey.IsEmpty())
            {
                _apiKey = ApiKeyAccess;
            }
            else
            {
                _apiKey = APIKey;
            }


        }

        #endregion

        #region Bulk Insert

        private BulkInsertOptions GetBulkInsertOptions(bool overwrite = true) => new BulkInsertOptions
        {
            BatchSize = 100,
            WriteTimeoutMilliseconds = 360000,
            OverwriteExisting = overwrite
        };

        public void BulkInsert<T>(ConcurrentBag<T> entities, bool overwrite = true) where T : BaseEntity
        {
            if (_database.IsEmpty()) {
                _database = DefaultDatabase;
            }

            using (var bulkInsert = DocumentStore.BulkInsert(Database, GetBulkInsertOptions(overwrite))) {
                foreach (var item in entities) {
                    if (item == null) {
                        continue;
                    }

                    if (item.Id.IsEmpty()) {
                        item.Id = Guid.NewGuid().ToString();
                    }

                    bulkInsert.Store(item);
                }
            }
        }

        public void BulkInsert<T>(List<T> entities, bool overwrite = true) where T : BaseEntity
        {
            var concurrentBag = new ConcurrentBag<T>();

            entities.ForEach(t => concurrentBag.Add(t));

            BulkInsert(concurrentBag, overwrite);
        }

        #endregion

        #region Find

        public T FindById<T>(string id) where T : BaseEntity
        {
            if (id == null) {
                throw new ArgumentException("Id must be provided.", nameof(id));
            }

            return Session.Load<T>(id);
        }

        public async Task<T> FindByIdAsync<T>(string id) where T : BaseEntity
        {
            if (id == null) {
                throw new ArgumentException("Id must be provided.", nameof(id));
            }

            return await AsyncSession.LoadAsync<T>(id);
        }

        #endregion

        #region Save

        public void Save<T>(T entity) where T : BaseEntity
        {
            if (entity == null) {
                throw new ArgumentException("Entity must be provided.", nameof(entity));
            }
            
            if (entity.Id.IsEmpty()) {
                entity.Id = Guid.NewGuid().ToString();
            }

            Session.Store(entity);
            Session.SaveChanges();
        }

        public async Task SaveAsync<T>(T entity) where T : BaseEntity
        {
            if (entity == null) {
                throw new ArgumentException("Entity must be provided.", nameof(entity));
            }
            
            if (entity.Id.IsEmpty()) {
                entity.Id = Guid.NewGuid().ToString();
            }

            await AsyncSession.StoreAsync(entity);
            await AsyncSession.SaveChangesAsync();
        }

        public async Task SaveNoValidateAsync<T>(T entity) where T : BaseEntity
        {
            if (entity == null) {
                throw new ArgumentException("Entity must be provided.", nameof(entity));
            }

            if (entity.Id.IsEmpty()) {
                entity.Id = Guid.NewGuid().ToString();
            }

            await AsyncSession.StoreAsync(entity);
            await AsyncSession.SaveChangesAsync();
        }

        public void SaveAll<T>(List<T> entities) where T : BaseEntity
        {
            if (entities.IsEmpty()) {
                throw new ArgumentException("Data must be provided.", nameof(entities));
            }

            foreach (var item in entities) {
                Save(item);
            }
        }

        public async Task SaveAllAsync<T>(List<T> entities) where T : BaseEntity
        {
            if (entities.IsEmpty()) {
                throw new ArgumentException("Data must be provided.", nameof(entities));
            }

            foreach (var item in entities) {
                await SaveAsync(item);
            }
        }

        #endregion

        #region Delete

        public void Delete(string id)
        {
            if (id == null) {
                throw new ArgumentException("Id must be provided.", nameof(id));
            }

            Session.Delete(id);

            Session.SaveChanges();
        }

        public async Task DeleteAsync(string id)
        {
            if (id == null) {
                throw new ArgumentException("Id must be provided.", nameof(id));
            }

            AsyncSession.Delete(id);

            await AsyncSession.SaveChangesAsync();
        }

        public void Delete<T>(T entity) where T : BaseEntity
        {
            if (entity == null) {
                throw new ArgumentException("Entity must be provided.", nameof(entity));
            }

            Session.Delete(entity.Id);

            Session.SaveChanges();
        }

        public async Task DeleteAsync<T>(T entity) where T : BaseEntity
        {
            if (entity == null) {
                throw new ArgumentException("Entity must be provided.", nameof(entity));
            }

            AsyncSession.Delete(entity.Id);

            await AsyncSession.SaveChangesAsync();
        }

        #endregion

        #region Query

        public IRavenQueryable<T> Query<T>() where T : BaseEntity
        {
            return Session.Query<T>();
        }

        public IRavenQueryable<T> QueryAsync<T>() where T : BaseEntity
        {
            return AsyncSession.Query<T>();
        }

        #endregion

        #region Index

        public IRavenQueryable<T> Index<T>(string indexName, bool isMapReduce = false) where T : BaseEntity
        {
            return Session.Query<T>(indexName, isMapReduce);
        }

        public IRavenQueryable<T> IndexAsync<T>(string indexName, bool isMapReduce = false) where T : BaseEntity
        {
            return AsyncSession.Query<T>(indexName, isMapReduce);
        }

        #endregion

        #region Count

        public int Count<T>() where T : BaseEntity
        {
            return Session.Query<T>().Count();
        }

        public async Task<int> CountAsync<T>() where T : BaseEntity
        {
            return await AsyncSession.Query<T>().CountAsync();
        }

        public int Count<T>(Expression<Func<T, bool>> predicate) where T : BaseEntity
        {
            return Session.Query<T>().Where(predicate).Count();
        }

        public async Task<int> CountAsync<T>(Expression<Func<T, bool>> predicate) where T : BaseEntity
        {
            return await AsyncSession.Query<T>().Where(predicate).CountAsync();
        }

        #endregion

        #region IDisposable

        private bool _disposed = false;

        protected void Dispose(bool disposing)
        {
            if (!_disposed) {
                if (disposing) {
                    _session = null;
                    _asyncSession = null;
                }

                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
}