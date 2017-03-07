using System;
using Hangfire.Storage;
using Xunit;
using Hangfire.Raven.Storage;

namespace Hangfire.Raven.Tests
{
    public class RavenStorageFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenRepositoryIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new RavenStorage((IRepository)null, new RavenStorageOptions()));

            Assert.Equal("repository", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            using (var repository = new TestRepository())
            {
                var exception = Assert.Throws<ArgumentNullException>(() => new RavenStorage(repository, null));

                Assert.Equal("options", exception.ParamName);
            }
        }

        [Fact]
        public void GetMonitoringApi_ReturnsNonNullInstance()
        {
            UseStorage(storage =>
            {
                IMonitoringApi api = storage.GetMonitoringApi();
                Assert.NotNull(api);
            });
        }

        [Fact]
        public void GetConnection_ReturnsNonNullInstance()
        {
            UseStorage(storage =>
            {
                using (IStorageConnection connection = storage.GetConnection())
                {
                    Assert.NotNull(connection);
                }
            });
        }

        private static void UseStorage(Action<RavenStorage> action)
        {
            using (var repository = new TestRepository())
            {
                action(new RavenStorage(repository));
            }
        }
    }
}
