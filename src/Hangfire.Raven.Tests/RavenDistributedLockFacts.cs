using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Storage;
using Xunit;
using Hangfire.Raven.DistributedLocks;
using Hangfire.Raven.Storage;
using Hangfire.Raven.Entities;
using Raven.Client.Linq;
using System.Linq;

namespace Hangfire.Raven.Tests
{
    public class RavenDistributedLockFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNull()
        {
            UseStorage(storage =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new RavenDistributedLock(storage, null, TimeSpan.Zero, new RavenStorageOptions()));

                Assert.Equal("resource", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new RavenDistributedLock(null, "resource1", TimeSpan.Zero, new RavenStorageOptions()));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact]
        public void Ctor_SetLock_WhenResourceIsNotLocked()
        {
            UseStorage(storage =>
            {
                using (new RavenDistributedLock(storage, "resource1", TimeSpan.Zero, new RavenStorageOptions()))
                using (var session = storage.Repository.OpenSession())
                {
                    var locksCount = session.Query<DistributedLock>().Where(_ => _.Resource == "resource1").Count();
                    Assert.Equal(1, locksCount);
                }
            });
        }

        [Fact]
        public void Ctor_SetReleaseLock_WhenResourceIsNotLocked()
        {
            UseStorage(storage =>
            {
                using (new RavenDistributedLock(storage, "resource1", TimeSpan.Zero, new RavenStorageOptions()))
                using (var session = storage.Repository.OpenSession())
                {
                    var locksCount = session.Query<DistributedLock>().Where(_ => _.Resource == "resource1").Count();
                    Assert.Equal(1, locksCount);
                }
                using (var session = storage.Repository.OpenSession())
                {
                    var locksCountAfter = session.Query<DistributedLock>().Where(_ => _.Resource == "resource1").Count();
                    Assert.Equal(0, locksCountAfter);
                }
            });
        }

        [Fact]
        public void Ctor_AcquireLockWithinSameThread_WhenResourceIsLocked()
        {
            UseStorage(storage =>
            {
                using (new RavenDistributedLock(storage, "resource1", TimeSpan.Zero, new RavenStorageOptions()))
                {
                    int locksCount;
                    using (var session = storage.Repository.OpenSession())
                    {
                        locksCount = session.Query<DistributedLock>().Where(_ => _.Resource == "resource1").Count();
                        Assert.Equal(1, locksCount);
                    }

                    using (new RavenDistributedLock(storage, "resource1", TimeSpan.Zero, new RavenStorageOptions()))
                    using (var session = storage.Repository.OpenSession())
                    {
                        locksCount = session.Query<DistributedLock>().Where(_ => _.Resource == "resource1").Count();
                        Assert.Equal(1, locksCount);
                    }
                }
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsLocked()
        {
            UseStorage(storage =>
            {
                using (new RavenDistributedLock(storage, "resource1", TimeSpan.Zero, new RavenStorageOptions()))
                using (var session = storage.Repository.OpenSession())
                {
                    var locksCount = session.Query<DistributedLock>().Where(_ => _.Resource == "resource1").Count();
                    Assert.Equal(1, locksCount);

                    Task.Run(() =>
                    {
                        Assert.Throws<DistributedLockTimeoutException>(() =>
                                new RavenDistributedLock(storage, "resource1", TimeSpan.Zero, new RavenStorageOptions()));
                    }).Wait();
                }
            });
        }

        [Fact]
        public void Ctor_WaitForLock_SignaledAtLockRelease()
        {
            UseStorage(storage =>
            {
                Task.Run(() =>
                {
                    using (new RavenDistributedLock(storage, "resource1", TimeSpan.Zero, new RavenStorageOptions()))
                    {
                        Thread.Sleep(TimeSpan.FromSeconds(5));
                    }
                });

                // Wait just a bit to make sure the above lock is acuired
                Thread.Sleep(TimeSpan.FromSeconds(1));

                // Record when we try to aquire the lock
                var startTime = DateTime.Now;
                using (new RavenDistributedLock(storage, "resource1", TimeSpan.FromSeconds(30), new RavenStorageOptions()))
                {
                    Assert.InRange(DateTime.Now - startTime, TimeSpan.Zero, TimeSpan.FromSeconds(5));
                }
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            UseStorage(storage =>
            {
                var exception = Assert.Throws<ArgumentNullException>(() =>
                    new RavenDistributedLock(storage, "resource1", TimeSpan.Zero, null));

                Assert.Equal("options", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_SetLockExpireAtWorks_WhenResourceIsNotLocked()
        {
            UseStorage(storage =>
            {
                using (new RavenDistributedLock(storage, "resource1", TimeSpan.Zero, new RavenStorageOptions { DistributedLockLifetime = TimeSpan.FromSeconds(3) }))
                using (var session = storage.Repository.OpenSession())
                {
                    DateTime initialExpireAt = DateTime.UtcNow;
                    Thread.Sleep(TimeSpan.FromSeconds(5));

                    DistributedLock lockEntry = session.Query<DistributedLock>().Where(_ => _.Resource == "resource1").FirstOrDefault();
                    Assert.NotNull(lockEntry);
                    var expireAt = session.Advanced.GetExpire(lockEntry);
                    Assert.True(expireAt > initialExpireAt);
                }
            });
        }

        private static void UseStorage(Action<RavenStorage> action)
        {
            using (var repository = new TestRepository())
            {
                var storage = new RavenStorage(repository);
                action(storage);
            }
        }
    }
}
