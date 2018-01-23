using System;
using Raven.Client.Documents.Session;

namespace Hangfire.Raven.Storage {
    public static class RavenServerStorageExtensions
    {
        public static void AddExpire<T>(this IAsyncAdvancedSessionOperations advanced, T obj, DateTime dateTime)
        {
            advanced.GetMetadataFor(obj)["@expires"] = dateTime;
        }
        public static void RemoveExpire<T>(this IAsyncAdvancedSessionOperations advanced, T obj)
        {
            advanced.GetMetadataFor(obj).Remove("Raven-Expiration-Date");
        }
        public static DateTime? GetExpire<T>(this IAsyncAdvancedSessionOperations advanced, T obj)
        {
            if (advanced.GetMetadataFor(obj).TryGetValue("Raven-Expiration-Date", out object dateTime)) {
                return (DateTime)dateTime;
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
    }
}
