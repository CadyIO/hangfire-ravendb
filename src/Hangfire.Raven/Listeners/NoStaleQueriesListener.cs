using Raven.Client;
using Raven.Client.Listeners;

namespace Hangfire.Raven.Listeners
{
    public class NoStaleQueriesListener : IDocumentQueryListener
    {
        public void BeforeQueryExecuted(IDocumentQueryCustomization queryCustomization)
        {
            queryCustomization.WaitForNonStaleResults();
        }
    }
}
