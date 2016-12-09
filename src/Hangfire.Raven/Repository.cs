using System;
using System.Collections.Generic;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Indexes;
using Hangfire.Raven.Listeners;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;

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

    public class Repository
    {
        private RepositoryConfig _config;
        private static IDocumentStore _documentStore;

        public Repository(RepositoryConfig config)
        {
            _config = config;

            if (!string.IsNullOrEmpty(_config.ConnectionStringName)) {
                _documentStore = new DocumentStore {
                    ConnectionStringName = _config.ConnectionStringName
                };
            } else {
                _documentStore = new DocumentStore {
                    Url = _config.ConnectionUrl,
                    ApiKey = _config.ApiKey,
                    DefaultDatabase = _config.Database
                };
            }

            _documentStore.Listeners.RegisterListener(new TakeNewestConflictResolutionListener());
            _documentStore.Initialize();

            new Hangfire_RavenJobs().Execute(_documentStore);
        }

        public FacetResults GetFacets(string index, IndexQuery query, List<Facet> facets)
        {
            return _documentStore.DatabaseCommands.GetFacets(index, query, facets);
        }

        public void ExecuteIndexes(List<AbstractIndexCreationTask> indexes)
        {
            _documentStore.ExecuteIndexes(indexes);
        }

        public static string GetId(Type type, params string[] id)
        {
            return _documentStore.Conventions.FindFullDocumentKeyFromNonStringIdentifier(string.Join("/", id), type, false);
        }

        public void Destroy()
        {
            if (!_documentStore.DatabaseExists(_config.Database)) {
                return;
            }

            _documentStore.DatabaseCommands.GlobalAdmin.DeleteDatabase(_config.Database, hardDelete: true);
        }

        public void Create()
        {
            if (_documentStore.DatabaseExists(_config.Database)) {
                return;
            }

            _documentStore.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists(_config.Database);
        }

        public IDisposable DocumentChange(Type documentType, Action<DocumentChangeNotification> action)
        {
            return _documentStore.Changes(_config.Database).ForDocumentsStartingWith(GetId(documentType, ""))
                .Subscribe(new RepositoryObserver<DocumentChangeNotification>(action));
        }

        public IDisposable DocumentChange(Type documentType, string suffix, Action<DocumentChangeNotification> action)
        {
            return _documentStore.Changes(_config.Database).ForDocumentsStartingWith(
                    GetId(documentType, string.Format("{0}/", suffix))
                )
                .Subscribe(new RepositoryObserver<DocumentChangeNotification>(action));
        }

        public IDocumentSession OpenSession()
        {
            return _documentStore.OpenSession(_config.Database);
        }

        public IAsyncDocumentSession OpenAsyncSession()
        {
            return _documentStore.OpenAsyncSession(_config.Database);
        }
    }
}