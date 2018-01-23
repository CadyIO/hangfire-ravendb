using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;

namespace Hangfire.Raven {
    public interface IRepository : IDisposable
    {
        void Create();
        void Destroy();
        void ExecuteIndexes(List<AbstractIndexCreationTask> indexes);
        IList<FacetResult> GetFacets(string index, IndexQuery query, List<Facet> facets);
        string GetId(Type type, params string[] id);
        IAsyncDocumentSession OpenAsyncSession();
        IDocumentSession OpenSession();
    }
}