using System.Collections.Generic;
using Hangfire.Logging;
using Hangfire.Raven.Indexes;
using Hangfire.Raven.JobQueues;
using Hangfire.Storage;
using Raven.Client.Indexes;
using Raven.Abstractions.Data;
using System.Linq.Expressions;
using Raven.Client;
using Raven.Client.Linq;

namespace Hangfire.Raven.Storage
{
    public class RavenStorage : JobStorage
    {
        private readonly RavenStorageOptions _options;
        private readonly Repository _repository;

        public RavenStorage(RepositoryConfig config)
            : this(config, new RavenStorageOptions())
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="config"></param>
        /// <param name="options"></param>
        public RavenStorage(RepositoryConfig config, RavenStorageOptions options)
        {
            options.ThrowIfNull("options");

            _options = options;
            _repository = new Repository(config);

            _repository.ExecuteIndexes(new List<AbstractIndexCreationTask>()
            {
                new Hangfire_RavenJobs(),
                new Hangfire_JobQueues()
            });

            InitializeQueueProviders();
        }

        public RavenStorageOptions Options { get { return _options; } }
        public Repository Repository { get { return _repository; } }

        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new RavenStorageMonitoringApi(this);
        }

        public override IStorageConnection GetConnection()
        {
            return new RavenConnection(this);
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Raven job storage:");
        }

        public FacetResults GetRavenJobFacets(
            IDocumentSession session,
            Expression<Func<Hangfire_RavenJobs.Mapping, bool>> clause)
        {
            var query = session.Query<Hangfire_RavenJobs.Mapping, Hangfire_RavenJobs>();
            if(clause != null)
                query = query.Where(clause);

            return query.ToFacets(new[] {
                new Facet
                        {
                            Name = "StateName"
                        }
                });
        }
        public FacetResults GetJobQueueFacets(
            IDocumentSession session,
            Expression<Func<Hangfire_JobQueues.Mapping, bool>> clause)
        {
            var query = session.Query<Hangfire_JobQueues.Mapping, Hangfire_JobQueues>();
            if (clause != null)
                query = query.Where(clause);

            return query.ToFacets(new[] {
                new Facet
                        {
                            Name = "Queue"
                        }
                });
        }

        private void InitializeQueueProviders()
        {
            var defaultQueueProvider = new RavenJobQueueProvider(this, _options);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
        }
    }
}