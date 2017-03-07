using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;

namespace Hangfire.Raven
{
    public interface IRepository : IDisposable
    {
        void Create();
        void Destroy();
        IDisposable DocumentChange(Type documentType, Action<DocumentChangeNotification> action);
        IDisposable DocumentChange(Type documentType, string suffix, Action<DocumentChangeNotification> action);
        void ExecuteIndexes(List<AbstractIndexCreationTask> indexes);
        FacetResults GetFacets(string index, IndexQuery query, List<Facet> facets);
        string GetId(Type type, params string[] id);
        IAsyncDocumentSession OpenAsyncSession();
        IDocumentSession OpenSession();
    }
}