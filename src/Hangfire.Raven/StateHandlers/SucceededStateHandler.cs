using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Raven.StateHandlers
{
    public class SucceededStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.InsertToList("succeeded", context.BackgroundJob.Id);
            transaction.TrimList("succeeded", 0, 99);
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromList("succeeded", context.BackgroundJob.Id);
        }

        public string StateName {
            get { return SucceededState.StateName; }
        }
    }
}
