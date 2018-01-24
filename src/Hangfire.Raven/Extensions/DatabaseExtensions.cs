using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using System.Linq;

namespace Hangfire.Raven.Extensions {
    public static class DatabaseExtensions
    {
        public static bool DatabaseExists(this IDocumentStore documentStore, string database)
        {
            var operation = new GetDatabaseNamesOperation(0, 25);
            string[] databaseNames = documentStore.Maintenance.Server.Send(operation);
            return databaseNames.Contains(database);
        }
    }
}
