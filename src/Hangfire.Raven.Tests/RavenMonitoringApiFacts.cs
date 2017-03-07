using System;
using System.Collections.Generic;
using System.Diagnostics;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Xunit;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Hangfire.Raven.Entities;
using System.Linq;

namespace Hangfire.Raven.Tests
{
    public class RavenMonitoringApiFacts
    {
        private const string DefaultQueue = "default";
        private const string FetchedStateName = "Fetched";
        private const int From = 0;
        private const int PerPage = 5;
        private readonly Mock<IPersistentJobQueue> _queue;
        private readonly Mock<IPersistentJobQueueProvider> _provider;
        private readonly Mock<IPersistentJobQueueMonitoringApi> _persistentJobQueueMonitoringApi;
        private readonly PersistentJobQueueProviderCollection _providers;

        public RavenMonitoringApiFacts()
        {
            _queue = new Mock<IPersistentJobQueue>();
            _persistentJobQueueMonitoringApi = new Mock<IPersistentJobQueueMonitoringApi>();

            _provider = new Mock<IPersistentJobQueueProvider>();
            _provider.Setup(x => x.GetJobQueue()).Returns(_queue.Object);
            _provider.Setup(x => x.GetJobQueueMonitoringApi())
                .Returns(_persistentJobQueueMonitoringApi.Object);

            _providers = new PersistentJobQueueProviderCollection(_provider.Object);
        }

        [Fact]
        public void GetStatistics_ReturnsZero_WhenNoJobsExist()
        {
            UseMonitoringApi((repository, monitoringApi) =>
            {
                var result = monitoringApi.GetStatistics();
                Assert.Equal(0, result.Enqueued);
                Assert.Equal(0, result.Failed);
                Assert.Equal(0, result.Processing);
                Assert.Equal(0, result.Scheduled);
            });
        }

        [Fact]
        public void GetStatistics_ReturnsExpectedCounts_WhenJobsExist()
        {
            UseMonitoringApi((repository, monitoringApi) =>
            {
                CreateJobInState(repository, "1", EnqueuedState.StateName);
                CreateJobInState(repository, "2", EnqueuedState.StateName);
                CreateJobInState(repository, "4", FailedState.StateName);
                CreateJobInState(repository, "5", ProcessingState.StateName);
                CreateJobInState(repository, "6", ScheduledState.StateName);
                CreateJobInState(repository, "7", ScheduledState.StateName);

                var result = monitoringApi.GetStatistics();
                Assert.Equal(2, result.Enqueued);
                Assert.Equal(1, result.Failed);
                Assert.Equal(1, result.Processing);
                Assert.Equal(2, result.Scheduled);
            });
        }

        [Fact]
        public void JobDetails_ReturnsNull_WhenThereIsNoSuchJob()
        {
            UseMonitoringApi((repository, monitoringApi) =>
            {
                var result = monitoringApi.JobDetails("547527");
                Assert.Null(result);
            });
        }

        [Fact]
        public void JobDetails_ReturnsResult_WhenJobExists()
        {
            UseMonitoringApi((repository, monitoringApi) =>
            {
                var job1 = CreateJobInState(repository, "1", EnqueuedState.StateName);

                var result = monitoringApi.JobDetails(job1.Id.Split(new[] { '/' }, 2)[1]);

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal("Arguments", result.Job.Args[0]);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
                Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
            });
        }

        [Fact]
        public void EnqueuedJobs_ReturnsEmpty_WhenThereIsNoJobs()
        {
            UseMonitoringApi((repository, monitoringApi) =>
            {
                var jobIds = new List<string>();

                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact]
        public void EnqueuedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsNotFetched()
        {
            UseMonitoringApi((repository, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(repository, "1", EnqueuedState.StateName);

                var jobIds = new List<string> { unfetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds.Select(x => x.Split(new[] { '/' }, 2)[1]));

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(1, resultList.Count);
            });
        }

        [Fact]
        public void EnqueuedJobs_ReturnsEmpty_WhenOneJobExistsThatIsFetched()
        {
            UseMonitoringApi((repository, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(repository, "1", FetchedStateName);

                var jobIds = new List<string> { fetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds.Select(x => x.Split(new[] { '/' }, 2)[1]));

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact]
        public void EnqueuedJobs_ReturnsUnfetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
        {
            UseMonitoringApi((repository, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(repository, "1", EnqueuedState.StateName);
                var unfetchedJob2 = CreateJobInState(repository, "2", EnqueuedState.StateName);
                var fetchedJob = CreateJobInState(repository, "3", FetchedStateName);

                var jobIds = new List<string> { unfetchedJob.Id, unfetchedJob2.Id, fetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds.Select(x => x.Split(new[] { '/' }, 2)[1]));

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(2, resultList.Count);
            });
        }

        [Fact]
        public void FetchedJobs_ReturnsEmpty_WhenThereIsNoJobs()
        {
            UseMonitoringApi((repository, monitoringApi) =>
            {
                var jobIds = new List<string>();

                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact]
        public void FetchedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsFetched()
        {
            UseMonitoringApi((repository, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(repository, "1", FetchedStateName);

                var jobIds = new List<string> { fetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds.Select(x => x.Split(new[] { '/' }, 2)[1]));

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(1, resultList.Count);
            });
        }

        [Fact]
        public void FetchedJobs_ReturnsEmpty_WhenOneJobExistsThatIsNotFetched()
        {
            UseMonitoringApi((repository, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(repository, "1", EnqueuedState.StateName);

                var jobIds = new List<string> { unfetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds.Select(x => x.Split(new[] { '/' }, 2)[1]));

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact]
        public void FetchedJobs_ReturnsFetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
        {
            UseMonitoringApi((repository, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(repository, "1", FetchedStateName);
                var fetchedJob2 = CreateJobInState(repository, "2", FetchedStateName);
                var unfetchedJob = CreateJobInState(repository, "3", EnqueuedState.StateName);

                var jobIds = new List<string> { fetchedJob.Id, fetchedJob2.Id, unfetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds.Select(x => x.Split(new[] { '/' }, 2)[1]));

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(2, resultList.Count);
            });
        }

        public static void SampleMethod(string arg)
        {
            Debug.WriteLine(arg);
        }

        private void UseMonitoringApi(Action<IRepository, RavenStorageMonitoringApi> action)
        {
            using (var repository = new TestRepository())
            {
                var storage = new Mock<RavenStorage>(repository);
                storage.Setup(x => x.QueueProviders).Returns(_providers);

                action(repository, new RavenStorageMonitoringApi(storage.Object));
            }
        }

        private RavenJob CreateJobInState(IRepository repository, string jobId, string stateName)
        {
            var job = Job.FromExpression(() => SampleMethod("Arguments"));

            var ravenJob = new RavenJob
            {
                Id = repository.GetId(typeof(RavenJob), jobId),
                InvocationData = InvocationData.Serialize(job),
                CreatedAt = DateTime.UtcNow,
                StateData = new StateData
                {
                    Name = stateName,
                    Data =
                        stateName == EnqueuedState.StateName
                            ? new Dictionary<string, string> { ["EnqueuedAt"] = $"{DateTime.UtcNow:o}" }
                            : new Dictionary<string, string>(),
                }
            };

            var jobQueue = new JobQueue
            {
                FetchedAt = null,
                Id = repository.GetId(typeof(JobQueue), DefaultQueue, jobId),
                JobId = jobId,
                Queue = DefaultQueue
            };

            if (stateName == FetchedStateName)
            {
                jobQueue.FetchedAt = DateTime.UtcNow;
            }

            using (var session = repository.OpenSession())
            {
                session.Store(ravenJob);
                session.Store(jobQueue);
                session.SaveChanges();
            }

            return ravenJob;
        }
    }
}
