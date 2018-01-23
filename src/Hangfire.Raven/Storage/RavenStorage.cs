using System;
using System.Collections.Generic;
using Hangfire.Logging;
using Hangfire.Raven.Indexes;
using Hangfire.Raven.JobQueues;
using Hangfire.Storage;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;

namespace Hangfire.Raven.Storage {
    public class RavenStorage : JobStorage
    {
        private readonly RavenStorageOptions _options;
        private readonly IRepository _repository;

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
            : this(new Repository(config), options)
        {
        }

        public RavenStorage(IRepository repository)
            : this(repository, new RavenStorageOptions())
        {
        }

        public RavenStorage(IRepository repository, RavenStorageOptions options)
        {
            repository.ThrowIfNull("repository");
            options.ThrowIfNull("options");

            _options = options;
            _repository = repository;

            _repository.Create();
            _repository.ExecuteIndexes(new List<AbstractIndexCreationTask>()
            {
                new Hangfire_RavenJobs(),
                new Hangfire_JobQueues(),
                new Hangfire_RavenServers()
            });

            InitializeQueueProviders();
        }

        public RavenStorageOptions Options { get { return _options; } }
        public IRepository Repository { get { return _repository; } }

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

        public IAggregationQuery<Hangfire_RavenJobs.Mapping> GetRavenJobFacets(
                    IDocumentSession session,
                    Expression<Func<Hangfire_RavenJobs.Mapping, bool>> clause)
        {
            var query = session.Query<Hangfire_RavenJobs.Mapping, Hangfire_RavenJobs>();
            if (clause != null)
                query = query.Where(clause);

            return query.AggregateBy(new[] {
                new Facet
                        {
                            FieldName = "StateName"
                        }
                }); ;
        } 

        public IAggregationQuery<Hangfire_JobQueues.Mapping> GetJobQueueFacets(
            IDocumentSession session,
            Expression<Func<Hangfire_JobQueues.Mapping, bool>> clause)
        {
            var query = session.Query<Hangfire_JobQueues.Mapping, Hangfire_JobQueues>();
            if (clause != null)
                query = query.Where(clause);

            return query.AggregateBy(new[] {
                new Facet
                        {
                            FieldName = "Queue"
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