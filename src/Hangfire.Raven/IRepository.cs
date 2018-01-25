using System;
using System.Collections.Generic;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;

namespace Hangfire.Raven {
    public interface IRepository : IDisposable
    {
        void Create();
        void Destroy();
        void ExecuteIndexes(List<AbstractIndexCreationTask> indexes);
        string GetId(Type type, params string[] id);
        IDocumentSession OpenSession();
        OperationExecutor GetOperationExecutor();
    }
}