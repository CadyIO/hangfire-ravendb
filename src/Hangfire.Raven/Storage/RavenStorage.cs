// This file is part of Hangfire.
// Copyright © 2013-2014 Sergey Odinokov.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using Raven.Client.Document;
using Hangfire.Raven.JobQueues;
using HangFire.Raven;

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

        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, _options.JobExpirationCheckInterval);
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Raven job storage:");
        }

        private void InitializeQueueProviders()
        {
            var defaultQueueProvider = new RavenJobQueueProvider(this, _options);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
        }
    }
}