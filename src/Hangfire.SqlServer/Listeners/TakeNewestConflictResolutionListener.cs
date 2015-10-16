using Raven.Abstractions.Data;
using Raven.Client.Listeners;
using System.Linq;

namespace Hangfire.Raven.Listeners
{
    public class TakeNewestConflictResolutionListener : IDocumentConflictListener
    {
        public bool TryResolveConflict(string key, JsonDocument[] conflictedDocs, out JsonDocument resolvedDocument)
        {
            var maxDate = conflictedDocs.Max(x => x.LastModified);
            resolvedDocument = conflictedDocs
                                .FirstOrDefault(x => x.LastModified == maxDate);

            if (resolvedDocument == null)
                return false;

            resolvedDocument.Metadata.Remove("@id");
            resolvedDocument.Metadata.Remove("@etag");

            return true;
        }
    }
}
