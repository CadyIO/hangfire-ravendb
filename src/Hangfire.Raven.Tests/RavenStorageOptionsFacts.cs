using Hangfire.Raven.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenStorageOptionsFacts
    {
        [Fact]
        public void Ctor_SetsTheDefaultOptions()
        {
            RavenStorageOptions options = new RavenStorageOptions();

            Assert.True(options.InvisibilityTimeout > TimeSpan.Zero);
        }

        [Fact]
        public void Ctor_SetsTheDefaultOptions_ShouldGenerateClientId()
        {
            var options = new RavenStorageOptions();
            Assert.False(String.IsNullOrWhiteSpace(options.ClientId));
        }

        [Fact]
        public void Ctor_SetsTheDefaultOptions_ShouldGenerateUniqueClientId()
        {
            var options1 = new RavenStorageOptions();
            var options2 = new RavenStorageOptions();
            var options3 = new RavenStorageOptions();

            IEnumerable<string> result = new[] { options1.ClientId, options2.ClientId, options3.ClientId }.Distinct();

            Assert.Equal(3, result.Count());
        }

        [Fact]
        public void Set_QueuePollInterval_ShouldThrowAnException_WhenGivenIntervalIsEqualToZero()
        {
            var options = new RavenStorageOptions();
            Assert.Throws<ArgumentException>(
                () => options.QueuePollInterval = TimeSpan.Zero);
        }

        [Fact]
        public void Set_QueuePollInterval_ShouldThrowAnException_WhenGivenIntervalIsNegative()
        {
            var options = new RavenStorageOptions();
            Assert.Throws<ArgumentException>(
                () => options.QueuePollInterval = TimeSpan.FromSeconds(-1));
        }

        [Fact]
        public void Set_QueuePollInterval_SetsTheValue()
        {
            var options = new RavenStorageOptions
            {
                QueuePollInterval = TimeSpan.FromSeconds(1)
            };
            Assert.Equal(TimeSpan.FromSeconds(1), options.QueuePollInterval);
        }

        [Fact]
        public void Set_DistributedLockLifetime_ShouldThrowAnException_WhenGivenValueIsEqualToZero()
        {
            var options = new RavenStorageOptions();
            Assert.Throws<ArgumentException>(
                () => options.DistributedLockLifetime = TimeSpan.Zero);
        }

        [Fact]
        public void Set_DistributedLockLifetime_ShouldThrowAnException_WhenGivenValueIsNegative()
        {
            var options = new RavenStorageOptions();
            Assert.Throws<ArgumentException>(
                () => options.DistributedLockLifetime = TimeSpan.FromSeconds(-1));
        }

        [Fact]
        public void Set_DistributedLockLifetime_SetsTheValue()
        {
            var options = new RavenStorageOptions
            {
                DistributedLockLifetime = TimeSpan.FromSeconds(1)
            };
            Assert.Equal(TimeSpan.FromSeconds(1), options.DistributedLockLifetime);
        }
    }
}
