using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Raven.StateHandlers
{
    public class DeletedStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.InsertToList("deleted", context.BackgroundJob.Id);
            transaction.TrimList("deleted", 0, 99);
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromList("deleted", context.BackgroundJob.Id);
        }

        public string StateName {
            get { return DeletedState.StateName; }
        }
    }
}
