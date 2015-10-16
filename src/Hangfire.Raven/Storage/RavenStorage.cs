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

namespace Hangfire.Raven.Storage
{
    public class RavenStorage : JobStorage
    {
        private readonly RavenStorageOptions _options;

        public RavenStorage()
            : this(new RavenStorageOptions())
        {
        }

        /// <summary>
        /// Initializes RavenStorage from the provided SqlServerStorageOptions and either the provided connection
        /// string or the connection string with provided name pulled from the application config file.       
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="options"></param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionString"/> argument is neither 
        /// a valid Raven connection string nor the name of a connection string in the application
        /// config file.</exception>
        public RavenStorage(RavenStorageOptions options)
        {
            options.ThrowIfNull("options");

            _options = options;

            InitializeQueueProviders();
        }

        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public override IMonitoringApi GetMonitoringApi()
        {
            //return new SqlServerMonitoringApi(this, _options.DashboardJobListLimit);

            return null;
        }

        public override IStorageConnection GetConnection()
        {
            return new RavenConnection(this);
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(_options.JobExpirationCheckInterval);
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Raven job storage:");
            logger.InfoFormat("Queue poll interval: {0}.", _options.QueuePollInterval);
        }

        private void InitializeQueueProviders()
        {
            var defaultQueueProvider = new RavenJobQueueProvider(this, _options);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
        }
    }
}