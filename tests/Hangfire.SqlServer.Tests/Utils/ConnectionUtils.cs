using System;
using System.Data.SqlClient;

namespace Hangfire.SqlServer.Tests
{
    public static class ConnectionUtils
    {
        private const string DatabaseVariable = "Hangfire_SqlServer_DatabaseName";
        private const string ConnectionStringTemplateVariable 
            = "Hangfire_SqlServer_ConnectionStringTemplate";

        private const string MasterDatabaseName = "master";
        private const string DefaultDatabaseName = @"Hangfire.Raven.Tests";
        private const string DefaultConnectionStringTemplate
            = @"Server=.\sqlexpress;Database={0};Trusted_Connection=True;";
    }
}
