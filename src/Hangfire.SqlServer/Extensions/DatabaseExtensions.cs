using Raven.Client;

namespace Hangfire.Raven.Extensions
{
    public static class DatabaseExtensions
    {
        public static bool DatabaseExists(this IDocumentStore documentStore, string database)
        {
            var result = documentStore.DatabaseCommands.Head("Raven/Databases/" + database);

            return (result != null);
        }
    }
}
