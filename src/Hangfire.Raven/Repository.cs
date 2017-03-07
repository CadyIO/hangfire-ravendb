using System;
using System.Collections.Generic;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Listeners;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using System.IO;

namespace Hangfire.Raven
{
    public class RepositoryConfig
    {
        public string ConnectionStringName { get; set; }
        public string ConnectionUrl { get; set; }
        public string Database { get; set; }
        public string ApiKey { get; set; }
    }

    public class RepositoryObserver<T>
        : IObserver<T>
    {
        private Action<T> _action;

        public RepositoryObserver(Action<T> input)
        {
            _action = input;
        }

        public void OnCompleted()
        {

        }

        public void OnError(Exception error)
        {

        }

        public void OnNext(T value)
        {
            _action.Invoke(value);
        }
    }

    public class Repository : IRepository
    {
        private DocumentStore _documentStore;

        private string _database;

        public Repository(RepositoryConfig config)
        {
            if (!string.IsNullOrEmpty(config.ConnectionStringName)) {
                _documentStore = new DocumentStore {
                    ConnectionStringName = config.ConnectionStringName
                };
            } else {
                _documentStore = new DocumentStore {
                    Url = config.ConnectionUrl,
                    ApiKey = config.ApiKey,
                    DefaultDatabase = config.Database
                };
            }

            _documentStore.Listeners.RegisterListener(new TakeNewestConflictResolutionListener());
            _documentStore.Initialize(ensureDatabaseExists: false);

            _database = _documentStore.DefaultDatabase;
        }

        public FacetResults GetFacets(string index, IndexQuery query, List<Facet> facets)
        {
            return _documentStore.DatabaseCommands.GetFacets(index, query, facets);
        }

        public void ExecuteIndexes(List<AbstractIndexCreationTask> indexes)
        {
            _documentStore.ExecuteIndexes(indexes);
        }

        public string GetId(Type type, params string[] id)
        {
            return _documentStore.Conventions.FindFullDocumentKeyFromNonStringIdentifier(string.Join("/", id), type, false);
        }

        public void Destroy()
        {
            if (_database == null || !_documentStore.DatabaseExists(_database)) {
                return;
            }

            _documentStore.DatabaseCommands.GlobalAdmin.DeleteDatabase(_database, hardDelete: true);
        }

        public void Create()
        {
            if (_database == null || _documentStore.DatabaseExists(_database)) {
                return;
            }

            _documentStore
                .DatabaseCommands
                .GlobalAdmin
                .CreateDatabase(new DatabaseDocument {
                    Id = "Raven/Databases/" + _database,
                    Settings = {
                        { "Raven/ActiveBundles", "DocumentExpiration" },
                        { "Raven/StorageTypeName", "voron" },
                        { "Raven/DataDir", Path.Combine("~", _database) },
                    }
                });
        }

        public IDisposable DocumentChange(Type documentType, Action<DocumentChangeNotification> action)
        {
            return _documentStore.Changes(_database).ForDocumentsStartingWith(GetId(documentType, ""))
                .Subscribe(new RepositoryObserver<DocumentChangeNotification>(action));
        }

        public IDisposable DocumentChange(Type documentType, string suffix, Action<DocumentChangeNotification> action)
        {
            return _documentStore.Changes(_database).ForDocumentsStartingWith(
                    GetId(documentType, string.Format("{0}/", suffix))
                )
                .Subscribe(new RepositoryObserver<DocumentChangeNotification>(action));
        }

        public IDocumentSession OpenSession()
        {
            return _documentStore.OpenSession(_database);
        }

        public IAsyncDocumentSession OpenAsyncSession()
        {
            return _documentStore.OpenAsyncSession(_database);
        }

        public void Dispose()
        {
            _documentStore.Dispose();
        }
    }
}