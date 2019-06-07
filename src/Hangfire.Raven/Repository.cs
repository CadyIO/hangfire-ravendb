using System;
using System.Collections.Generic;
using Hangfire.Raven.Extensions;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Expiration;
using System.Security.Cryptography.X509Certificates;

namespace Hangfire.Raven
{
    public class RepositoryConfig
    {
        public string ConnectionUrl { get; set; }
        public string Database { get; set; }
        public X509Certificate2 Certificate { get; set; }
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

        private readonly string _database;

        public Repository(RepositoryConfig config)
        {
            _documentStore = new DocumentStore
            {
                Urls = new[] { config.ConnectionUrl },
                Database = config.Database,
                Certificate = config.Certificate
            };

            _documentStore.Initialize();

            _database = _documentStore.Database;
        }

        public void ExecuteIndexes(List<AbstractIndexCreationTask> indexes)
        {
            _documentStore.ExecuteIndexes(indexes);
        }

        public void Destroy()
        {
            if (_database == null || !_documentStore.DatabaseExists(_database))
            {
                return;
            }

            _documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(_database, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
        }

        public void Create()
        {
            if (_database == null || _documentStore.DatabaseExists(_database))
            {
                return;
            }

            _documentStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(_database)));
            _documentStore.Maintenance.Send(new ConfigureExpirationOperation(new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = 60
            }));
        }

        public void Dispose() => _documentStore.Dispose();

        IDocumentSession IRepository.OpenSession() => _documentStore.OpenSession();

        public string GetId(Type type, params string[] id) => type.ToString() + '/' + string.Join("/", id);
    }
}