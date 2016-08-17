using System;
using Hangfire.Annotations;
using Hangfire.Raven.Storage;
using HangFire.Raven;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueueProvider 
        : IPersistentJobQueueProvider
    {
        private readonly IPersistentJobQueue _jobQueue;
        private readonly IPersistentJobQueueMonitoringApi _monitoringApi;

        public RavenJobQueueProvider([NotNull] RavenStorage storage, [NotNull] RavenStorageOptions options)
        {
            storage.ThrowIfNull("storage");
            options.ThrowIfNull("options");

            _jobQueue = new RavenJobQueue(storage, options);
            _monitoringApi = new RavenJobQueueMonitoringApi(storage);
        }

        public IPersistentJobQueue GetJobQueue()
        {
            return _jobQueue;
        }

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi()
        {
            return _monitoringApi;
        }
    }
}