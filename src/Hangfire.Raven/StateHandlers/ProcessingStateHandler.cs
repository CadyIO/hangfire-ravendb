using System;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Raven.StateHandlers
{
    public class ProcessingStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.AddToSet("processing", context.BackgroundJob.Id, JobHelper.ToTimestamp(DateTime.UtcNow));
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromSet("processing", context.BackgroundJob.Id);
        }

        public string StateName {
            get { return ProcessingState.StateName; }
        }
    }
}
