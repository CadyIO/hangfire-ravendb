using Raven.Client.Documents;

namespace Hangfire.Raven.Extensions {
    public static class DatabaseExtensions
    {
        public static bool DatabaseExists(this IDocumentStore documentStore, string database)
        {
            //todo check if db exists
            //var result = documentStore.ForSystemDatabase().Head("Raven/Databases/" + database);

            return false;
        }
    }
}
