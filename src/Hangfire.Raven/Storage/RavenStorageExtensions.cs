using System;
using Raven.Client;
using Raven.Json.Linq;

namespace Hangfire.Raven.Storage
{
    public static class RavenServerStorageExtensions
    {
        public static void AddExpire<T>(this ISyncAdvancedSessionOperation advanced, T obj, DateTime dateTime)
        {
            advanced.GetMetadataFor(obj)["Raven-Expiration-Date"] = new RavenJValue(dateTime);
        }
        public static void RemoveExpire<T>(this ISyncAdvancedSessionOperation advanced, T obj)
        {
            advanced.GetMetadataFor(obj).Remove("Raven-Expiration-Date");
        }
        public static DateTime? GetExpire<T>(this ISyncAdvancedSessionOperation advanced, T obj)
        {
            RavenJToken token;
            if (advanced.GetMetadataFor(obj).TryGetValue("Raven-Expiration-Date", out token)) {
                var date = token.Value<DateTime>();
                return date;
            }
            return null;
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(this IGlobalConfiguration configuration, RavenStorage storage)
        {
            storage.ThrowIfNull("storage");

            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(this IGlobalConfiguration configuration, string connectionString)
        {
            configuration.ThrowIfNull("configuration");
            connectionString.ThrowIfNull("connectionString");

            var config = new RepositoryConfig() {
                ConnectionStringName = connectionString
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

            var config = new RepositoryConfig() {
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

            var config = new RepositoryConfig() {
                ConnectionUrl = connectionUrl,
                Database = database,
                ApiKey = APIKey
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

            var config = new RepositoryConfig() {
                ConnectionUrl = connectionUrl,
                Database = database
            };

            var storage = new RavenStorage(config, options);

            return configuration.UseStorage(storage);
        }

#if NETFULL
        public static IGlobalConfiguration<RavenStorage> UseEmbeddedRavenStorage(this IGlobalConfiguration configuration)
        {
            configuration.ThrowIfNull("configuration");

            var config = new RepositoryConfig()
            {
                Embedded = true
            };

            var storage = new RavenStorage(config);

            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<RavenStorage> UseEmbeddedRavenStorage(this IGlobalConfiguration configuration, string connectionStringName)
        {
            configuration.ThrowIfNull("configuration");

            var config = new RepositoryConfig()
            {
                Embedded = true,
                ConnectionStringName = connectionStringName
            };

            var storage = new RavenStorage(config);

            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<RavenStorage> UseEmbeddedRavenStorage(this IGlobalConfiguration configuration, string connectionUrl, string database)
        {
            configuration.ThrowIfNull("configuration");
            configuration.ThrowIfNull("connectionUrl");
            configuration.ThrowIfNull("database");

            var config = new RepositoryConfig()
            {
                Embedded = true,
                ConnectionUrl = connectionUrl,
                Database = database
            };

            var storage = new RavenStorage(config);

            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<RavenStorage> UseEmbeddedRavenStorage(this IGlobalConfiguration configuration, string connectionUrl, string database, RavenStorageOptions options)
        {
            configuration.ThrowIfNull("configuration");
            configuration.ThrowIfNull("connectionUrl");
            configuration.ThrowIfNull("database");
            configuration.ThrowIfNull("options");

            var config = new RepositoryConfig()
            {
                Embedded = true,
                ConnectionUrl = connectionUrl,
                Database = database
            };

            var storage = new RavenStorage(config, options);

            return configuration.UseStorage(storage);
        }
#endif
    }
}
