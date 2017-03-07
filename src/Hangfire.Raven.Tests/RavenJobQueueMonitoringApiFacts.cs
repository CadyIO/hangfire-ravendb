using Hangfire.Raven.Entities;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using System;
using System.Linq;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenJobQueueMonitoringApiFacts
    {
        private const string QueueName1 = "queueName1";
        private const string QueueName2 = "queueName2";

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new RavenJobQueueMonitoringApi(null));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact]
        public void GetQueues_ShouldReturnEmpty_WhenNoQueuesExist()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                var queues = ravenJobQueueMonitoringApi.GetQueues();

                Assert.Empty(queues);
            });
        }

        [Fact]
        public void GetQueues_ShouldReturnOneQueue_WhenOneQueueExists()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                CreateJobQueue(storage, QueueName1, false);

                var queues = ravenJobQueueMonitoringApi.GetQueues().ToList();

                Assert.Equal(1, queues.Count);
                Assert.Equal(QueueName1, queues.First());
            });
        }

        [Fact]
        public void GetQueues_ShouldReturnTwoUniqueQueues_WhenThreeNonUniqueQueuesExist()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                CreateJobQueue(storage, QueueName1, false);
                CreateJobQueue(storage, QueueName1, false);
                CreateJobQueue(storage, QueueName2, false);

                var queues = ravenJobQueueMonitoringApi.GetQueues().ToList();

                Assert.Equal(2, queues.Count);
                Assert.True(queues.Contains(QueueName1));
                Assert.True(queues.Contains(QueueName2));
            });
        }

        [Fact]
        public void GetEnqueuedJobIds_ShouldReturnEmpty_WheNoQueuesExist()
        {
            UseStorage(storage =>
            {
                var mongoJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10);

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact]
        public void GetEnqueuedJobIds_ShouldReturnEmpty_WhenOneJobWithAFetchedStateExists()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                CreateJobQueue(storage, QueueName1, true);

                var enqueuedJobIds = ravenJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact]
        public void GetEnqueuedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                var jobQueue = CreateJobQueue(storage, QueueName1, false);

                var enqueuedJobIds = ravenJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(1, enqueuedJobIds.Count);
                Assert.Equal(jobQueue.JobId, enqueuedJobIds.First());
            });
        }

        [Fact]
        public void GetEnqueuedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                var jobQueue = CreateJobQueue(storage, QueueName1, false);
                var jobQueue2 = CreateJobQueue(storage, QueueName1, false);
                var jobQueue3 = CreateJobQueue(storage, QueueName1, false);

                var enqueuedJobIds = ravenJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(3, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueue.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueue2.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueue3.JobId));
            });
        }

        [Fact]
        public void GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                var jobQueue = CreateJobQueue(storage, QueueName1, false);
                var jobQueue2 = CreateJobQueue(storage, QueueName1, false);
                CreateJobQueue(storage, QueueName2, false);

                var enqueuedJobIds = ravenJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueue.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueue2.JobId));
            });
        }

        [Fact]
        public void GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                var jobQueue = CreateJobQueue(storage, QueueName1, false);
                var jobQueue2 = CreateJobQueue(storage, QueueName1, false);
                CreateJobQueue(storage, QueueName1, false);

                var enqueuedJobIds = ravenJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 2).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueue.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueue2.JobId));
            });
        }

        [Fact]
        public void GetFetchedJobIds_ShouldReturnEmpty_WheNoQueuesExist()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                var enqueuedJobIds = ravenJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10);

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact]
        public void GetFetchedJobIds_ShouldReturnEmpty_WhenOneJobWithNonFetchedStateExists()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                CreateJobQueue(storage, QueueName1, false);

                var enqueuedJobIds = ravenJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact]
        public void GetFetchedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                var jobQueue = CreateJobQueue(storage, QueueName1, true);

                var enqueuedJobIds = ravenJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(1, enqueuedJobIds.Count);
                Assert.Equal(jobQueue.JobId, enqueuedJobIds.First());
            });
        }

        [Fact]
        public void GetFetchedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                var jobQueue = CreateJobQueue(storage, QueueName1, true);
                var jobQueue2 = CreateJobQueue(storage, QueueName1, true);
                var jobQueue3 = CreateJobQueue(storage, QueueName1, true);

                var enqueuedJobIds = ravenJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(3, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueue.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueue2.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueue3.JobId));
            });
        }

        [Fact]
        public void GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                var jobQueue = CreateJobQueue(storage, QueueName1, true);
                var jobQueue2 = CreateJobQueue(storage, QueueName1, true);
                CreateJobQueue(storage, QueueName2, true);

                var enqueuedJobIds = ravenJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueue.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueue2.JobId));
            });
        }

        [Fact]
        public void GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            UseStorage(storage =>
            {
                var ravenJobQueueMonitoringApi = CreateRavenJobQueueMonitoringApi(storage);

                var jobQueue = CreateJobQueue(storage, QueueName1, true);
                var jobQueue2 = CreateJobQueue(storage, QueueName1, true);
                CreateJobQueue(storage, QueueName1, true);

                var enqueuedJobIds = ravenJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 2).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueue.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueue2.JobId));
            });
        }

        private static JobQueue CreateJobQueue(RavenStorage storage, string queue, bool isFetched)
        {
            var jobId = Guid.NewGuid().ToString();

            var job = new RavenJob
            {
                Id = storage.Repository.GetId(typeof(RavenJob), jobId),
                CreatedAt = DateTime.UtcNow,
                StateData = new StateData()
            };

            var jobQueue = new JobQueue
            {
                Id = storage.Repository.GetId(typeof(JobQueue), queue, jobId),
                Queue = queue,
                JobId = jobId
            };

            if (isFetched)
            {
                jobQueue.FetchedAt = DateTime.UtcNow.AddDays(-1);
            }

            using (var session = storage.Repository.OpenSession())
            {
                session.Store(job);
                session.Store(jobQueue);
                session.SaveChanges();
            }

            return jobQueue;
        }

        private static RavenJobQueueMonitoringApi CreateRavenJobQueueMonitoringApi(RavenStorage storage)
        {
            return new RavenJobQueueMonitoringApi(storage);
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
