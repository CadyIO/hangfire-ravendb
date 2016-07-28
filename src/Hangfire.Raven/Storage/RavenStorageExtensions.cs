// This file is part of Hangfire.
// Copyright © 2015 Sergey Odinokov.
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
using Hangfire;
using Hangfire.Raven.Storage;
using Hangfire.Raven;

// ReSharper disable once CheckNamespace
namespace HangFire.Raven.Storage
{
    public static class SqlServerStorageExtensions
    {
        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(this IGlobalConfiguration configuration, RavenStorage storage)
        {
            storage.ThrowIfNull("storage");

            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(this IGlobalConfiguration configuration, string connectionString)
        {
            configuration.ThrowIfNull("configuration");
            connectionString.ThrowIfNull("connectionString");

            var config = new RepositoryConfig()
            {
                ConnectionString = connectionString
            };
            var storage = new RavenStorage(config);

            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(this IGlobalConfiguration configuration, string connectionUrl, string database)
        {
            configuration.ThrowIfNull("configuration");
            connectionUrl.ThrowIfNull("connectionUrl");
            database.ThrowIfNull("database");

            if (!connectionUrl.StartsWith("http")) {
                throw new ArgumentException("Connection Url must begin with http or https!");
            }

            var config = new RepositoryConfig()
            {
                ConnectionUrl = connectionUrl,
                Database = database
            };
            var storage = new RavenStorage(config);

            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(this IGlobalConfiguration configuration, string connectionUrl, string database, string APIKey)
        {
            configuration.ThrowIfNull("configuration");
            connectionUrl.ThrowIfNull("connectionUrl");
            database.ThrowIfNull("database");

            if (!connectionUrl.StartsWith("http")) {
                throw new ArgumentException("Connection Url must begin with http or https!");
            }

            var config = new RepositoryConfig()
            {
                ConnectionUrl = connectionUrl,
                Database = database,
                APIKey = APIKey
            };

            var storage = new RavenStorage(config);

            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(this IGlobalConfiguration configuration, string connectionUrl, string database, RavenStorageOptions options)
        {
            configuration.ThrowIfNull("configuration");
            connectionUrl.ThrowIfNull("connectionUrl");
            database.ThrowIfNull("database");
            options.ThrowIfNull("options");

            if (!connectionUrl.StartsWith("http")) {
                throw new ArgumentException("Connection Url must begin with http or https!");
            }

            var config = new RepositoryConfig()
            {
                ConnectionUrl = connectionUrl,
                Database = database
            };

            var storage = new RavenStorage(config);

            return configuration.UseStorage(storage);
        }
    }
}
