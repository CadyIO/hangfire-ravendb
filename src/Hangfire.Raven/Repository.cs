using System;
using System.Collections.Generic;
using Hangfire.Raven.Extensions;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;

namespace Hangfire.Raven {
    public class RepositoryConfig {
        public string ConnectionStringName { get; set; }
        public string ConnectionUrl { get; set; }
        public string Database { get; set; }
        public string ApiKey { get; set; }
    }

    public class RepositoryObserver<T>
        : IObserver<T> {
        private Action<T> _action;

        public RepositoryObserver(Action<T> input) {
            _action = input;
        }

        public void OnCompleted() {

        }

        public void OnError(Exception error) {

        }

        public void OnNext(T value) {
            _action.Invoke(value);
        }
    }

    public class Repository : IRepository {
        private DocumentStore _documentStore;

        private string _database;

        public Repository(RepositoryConfig config) {
            if (!string.IsNullOrEmpty(config.ConnectionStringName)) {
                /*
                 * TODO
                 * 
                 * As the configuration system has been changed in .NET Core, we removed the ConnectionStringName property. Instead you can use the .NET core configuration mechanism, retrieve the connection string entry from appsettings.json, convert it, and manually set Urls and Database properties.
                 */
                /*_documentStore = new DocumentStore {
                    ConnectionStringName = config.ConnectionStringName
                };*/
            } else {
                _documentStore = new DocumentStore {
                    Urls = new[] { config.ConnectionUrl },
                    ///ApiKey = config.ApiKey,
                    Database = config.Database
                };
            }

            _documentStore.Initialize();

            _database = _documentStore.Database;
        }

        public void ExecuteIndexes(List<AbstractIndexCreationTask> indexes) {
            _documentStore.ExecuteIndexes(indexes);
        }

        public void Destroy() {
            if (_database == null || !_documentStore.DatabaseExists(_database)) {
                return;
            }

            _documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(_database, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
        }

        public void Create() {
            if (_database == null || _documentStore.DatabaseExists(_database)) {
                return;
            }

            _documentStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(_database)));
        }

        public void Dispose() => _documentStore.Dispose();

        IDocumentSession IRepository.OpenSession() => _documentStore.OpenSession();

        OperationExecutor IRepository.GetOperationExecutor() => _documentStore.Operations;

        public string GetId(Type type, params string[] id) => type.ToString() + '/' + string.Join("/", id);
    }
}