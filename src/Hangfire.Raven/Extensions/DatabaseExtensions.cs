using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using System.Linq;

namespace Hangfire.Raven.Extensions {
    public static class DatabaseExtensions
    {
        public static bool DatabaseExists(this IDocumentStore documentStore, string database)
        {
            var operation = new GetDatabaseRecordOperation(database);
            var databaseRecord = documentStore.Maintenance.Server.Send(operation);
            return databaseRecord != null;
        }
    }
}
