using System;
using System.Data.SqlClient;
using System.Reflection;
using System.Threading;
using System.Transactions;
using Dapper;
using Xunit;

namespace Hangfire.SqlServer.Tests
{
    public class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        private static readonly object GlobalLock = new object();

        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(GlobalLock);
        }

        public override void After(MethodInfo methodUnderTest)
        {
            Monitor.Exit(GlobalLock);
        }
    }
}
