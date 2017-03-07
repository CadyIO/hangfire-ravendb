using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Moq;
using Xunit;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Hangfire.Raven.Entities;

namespace Hangfire.Raven.Tests
{
    public class RavenConnectionFacts
    {
        private readonly Mock<IPersistentJobQueue> _queue;
        private readonly Mock<IPersistentJobQueueProvider> _provider;
        private readonly PersistentJobQueueProviderCollection _providers;

        public RavenConnectionFacts()
        {
            _queue = new Mock<IPersistentJobQueue>();

            _provider = new Mock<IPersistentJobQueueProvider>();
            _provider.Setup(x => x.GetJobQueue()).Returns(_queue.Object);

            _providers = new PersistentJobQueueProviderCollection(_provider.Object);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new RavenConnection(null));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact]
        public void FetchNextJob_DelegatesItsExecution_ToTheQueue()
        {
            UseConnection((repository, connection) =>
            {
                var token = new CancellationToken();
                var queues = new[] { "default" };

                connection.FetchNextJob(queues, token);

                _queue.Verify(x => x.Dequeue(queues, token));
            });
        }

        [Fact]
        public void FetchNextJob_Throws_IfMultipleProvidersResolved()
        {
            UseConnection((repository, connection) =>
            {
                var token = new CancellationToken();
                var anotherProvider = new Mock<IPersistentJobQueueProvider>();
                _providers.Add(anotherProvider.Object, new[] { "critical" });

                Assert.Throws<InvalidOperationException>(
                    () => connection.FetchNextJob(new[] { "critical", "default" }, token));
            });
        }

        [Fact]
        public void CreateWriteTransaction_ReturnsNonNullInstance()
        {
            UseConnection((repository, connection) =>
            {
                var transaction = connection.CreateWriteTransaction();
                Assert.NotNull(transaction);
            });
        }

        [Fact]
        public void AcquireLock_ReturnsNonNullInstance()
        {
            UseConnection((repository, connection) =>
            {
                var @lock = connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1));
                Assert.NotNull(@lock);
            });
        }

        [Fact]
        public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(
                        null,
                        new Dictionary<string, string>(),
                        DateTime.UtcNow,
                        TimeSpan.Zero));

                Assert.Equal("job", exception.ParamName);
            });
        }

        [Fact]
        public void CreateExpiredJob_ThrowsAnException_WhenParametersCollectionIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(
                        Job.FromExpression(() => SampleMethod("hello")),
                        null,
                        DateTime.UtcNow,
                        TimeSpan.Zero));

                Assert.Equal("parameters", exception.ParamName);
            });
        }

        [Fact]
        public void CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
        {
            UseConnection((repository, connection) =>
            {
                var createdAt = new DateTime(2012, 12, 12, 0, 0, 0, 0, DateTimeKind.Utc);
                var jobId = connection.CreateExpiredJob(
                    Job.FromExpression(() => SampleMethod("Hello")),
                    new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
                    createdAt,
                    TimeSpan.FromDays(1));

                Assert.NotNull(jobId);
                Assert.NotEmpty(jobId);

                using (var session = repository.OpenSession())
                {
                    var ravenJob = session.Query<RavenJob>().Single();
                    Assert.Equal(jobId, ravenJob.Id.Split(new[] { '/' }, 2)[1]);
                    Assert.Equal(createdAt, ravenJob.CreatedAt);
                    Assert.Equal(null, ravenJob.StateData);

                    var invocationData = ravenJob.InvocationData;

                    var job = invocationData.Deserialize();
                    Assert.Equal(typeof(RavenConnectionFacts), job.Type);
                    Assert.Equal("SampleMethod", job.Method.Name);
                    Assert.Equal("Hello", job.Args[0]);

                    var expireAt = session.Advanced.GetExpire(ravenJob);
                    Assert.True(createdAt.AddDays(1).AddMinutes(-1) < expireAt);
                    Assert.True(expireAt < createdAt.AddDays(1).AddMinutes(1));

                    var parameters = ravenJob.Parameters;

                    Assert.Equal("Value1", parameters["Key1"]);
                    Assert.Equal("Value2", parameters["Key2"]);
                }
            });
        }

        [Fact]
        public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection((repository, connection) => Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobData(null)));
        }

        [Fact]
        public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetJobData("547527");
                Assert.Null(result);
            });
        }

        [Fact]
        public void GetJobData_ReturnsResult_WhenJobExists()
        {
            UseConnection((repository, connection) =>
            {
                var job = Job.FromExpression(() => SampleMethod("Arguments"));
                var jobId = Guid.NewGuid().ToString();

                var ravenJob = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), jobId),
                    InvocationData = InvocationData.Serialize(job),
                    StateData = new StateData { Name = "Succeeded" },
                    CreatedAt = DateTime.UtcNow
                };

                using (var session = repository.OpenSession())
                {
                    session.Store(ravenJob);
                    session.SaveChanges();
                }

                var result = connection.GetJobData(jobId);

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal("Succeeded", result.State);
                Assert.Equal("Arguments", result.Job.Args[0]);
                Assert.Null(result.LoadException);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
                Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
            });
        }

        [Fact]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(
                (repository, connection) => Assert.Throws<ArgumentNullException>(
                    () => connection.GetStateData(null)));
        }

        [Fact]
        public void GetStateData_ReturnsNull_IfThereIsNoSuchState()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetStateData("547527");
                Assert.Null(result);
            });
        }

        [Fact]
        public void GetStateData_ReturnsCorrectData()
        {
            UseConnection((repository, connection) =>
            {
                var data = new Dictionary<string, string>
                {
                    { "Key", "Value" }
                };
                var jobId = Guid.NewGuid().ToString();

                var ravenJob = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), jobId),
                    InvocationData = null,
                    StateData = new StateData
                    {
                        Name = "Name",
                        Reason = "Reason",
                        Data = data
                    },
                    CreatedAt = DateTime.UtcNow
                };

                using (var session = repository.OpenSession())
                {
                    session.Store(ravenJob);
                    session.SaveChanges();
                }

                var result = connection.GetStateData(jobId);
                Assert.NotNull(result);

                Assert.Equal("Name", result.Name);
                Assert.Equal("Reason", result.Reason);
                Assert.Equal("Value", result.Data["Key"]);
            });
        }

        [Fact]
        public void GetJobData_ReturnsJobLoadException_IfThereWasADeserializationException()
        {
            UseConnection((repository, connection) =>
            {
                var jobId = Guid.NewGuid().ToString();
                var ravenJob = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), jobId),
                    InvocationData = new InvocationData(null, null, null, null),
                    CreatedAt = DateTime.UtcNow
                };

                using (var session = repository.OpenSession())
                {
                    session.Store(ravenJob);
                    session.SaveChanges();
                }

                var result = connection.GetJobData(jobId);

                Assert.NotNull(result.LoadException);
            });
        }

        [Fact]
        public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter(null, "name", "value"));

                Assert.Equal("jobId", exception.ParamName);
            });
        }

        [Fact]
        public void SetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter("547527b4c6b6cc26a02d021d", null, "value"));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
        {
            UseConnection((repository, connection) =>
            {
                var jobId = Guid.NewGuid().ToString();
                var ravenJob = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), jobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };

                using (var session = repository.OpenSession())
                {
                    session.Store(ravenJob);
                    session.SaveChanges();
                }

                connection.SetJobParameter(jobId, "Name", "Value");

                using (var session = repository.OpenSession())
                {
                    var parameter = session.Load<RavenJob>(ravenJob.Id).Parameters.Single(p => p.Key == "Name");

                    Assert.Equal("Value", parameter.Value);
                }
            });
        }

        [Fact]
        public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
        {
            UseConnection((repository, connection) =>
            {
                var jobId = Guid.NewGuid().ToString();
                var ravenJob = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), jobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };

                using (var session = repository.OpenSession())
                {
                    session.Store(ravenJob);
                    session.SaveChanges();
                }

                connection.SetJobParameter(jobId, "Name", "Value");
                connection.SetJobParameter(jobId, "Name", "AnotherValue");

                using (var session = repository.OpenSession())
                {
                    var parameter = session.Load<RavenJob>(ravenJob.Id).Parameters.Single(p => p.Key == "Name");

                    Assert.Equal("AnotherValue", parameter.Value);
                }
            });
        }

        [Fact]
        public void SetParameter_CanAcceptNulls_AsValues()
        {
            UseConnection((repository, connection) =>
            {
                var jobId = Guid.NewGuid().ToString();
                var ravenJob = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), jobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };

                using (var session = repository.OpenSession())
                {
                    session.Store(ravenJob);
                    session.SaveChanges();
                }

                connection.SetJobParameter(jobId, "Name", null);

                using (var session = repository.OpenSession())
                {
                    var parameter = session.Load<RavenJob>(ravenJob.Id).Parameters.Single(p => p.Key == "Name");

                    Assert.Equal(null, parameter.Value);
                }
            });
        }

        [Fact]
        public void GetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter(null, "hello"));

                Assert.Equal("jobId", exception.ParamName);
            });
        }

        [Fact]
        public void GetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter("547527b4c6b6cc26a02d021d", null));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void GetParameter_ReturnsNull_WhenParameterDoesNotExists()
        {
            UseConnection((repository, connection) =>
            {
                var value = connection.GetJobParameter("1", "hello");
                Assert.Null(value);
            });
        }

        [Fact]
        public void GetParameter_ReturnsParameterValue_WhenJobExists()
        {
            UseConnection((repository, connection) =>
            {
                var jobId = Guid.NewGuid().ToString();
                var ravenJob = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), jobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow,
                    Parameters = new Dictionary<string, string> { { "name", "value" } }
                };

                using (var session = repository.OpenSession())
                {
                    session.Store(ravenJob);
                    session.SaveChanges();
                }

                var value = connection.GetJobParameter(jobId, "name");

                Assert.Equal("value", value);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetFirstByLowestScoreFromSet(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore()
        {
            UseConnection((repository, connection) => Assert.Throws<ArgumentException>(
                () => connection.GetFirstByLowestScoreFromSet("key", 0, -1)));
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetFirstByLowestScoreFromSet(
                    "key", 0, 1);

                Assert.Null(result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
        {
            UseConnection((repository, connection) =>
            {
                using (var session = repository.OpenSession())
                {
                    session.Store(new RavenSet
                    {
                        Id = repository.GetId(typeof(RavenSet), "key"),
                        Scores = new Dictionary<string, double>
                        {
                            { "1.0", 1.0 },
                            { "-1.0", -1.0 },
                            { "-5.0", -5.0 }
                        }
                    });
                    session.Store(new RavenSet
                    {
                        Id = repository.GetId(typeof(RavenSet), "another-key"),
                        Scores = new Dictionary<string, double>
                        {
                            { "-2.0", -2.0 }
                        }
                    });

                    session.SaveChanges();
                }

                var result = connection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

                Assert.Equal("-1.0", result);
            });
        }

        [Fact]
        public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer(null, new ServerContext()));

                Assert.Equal("serverId", exception.ParamName);
            });
        }

        [Fact]
        public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer("server", null));

                Assert.Equal("context", exception.ParamName);
            });
        }

        [Fact]
        public void AnnounceServer_CreatesOrUpdatesARecord()
        {
            UseConnection((repository, connection) =>
            {
                var context1 = new ServerContext
                {
                    Queues = new[] { "critical", "default" },
                    WorkerCount = 4
                };
                connection.AnnounceServer("server", context1);

                string serverId;
                using (var session = repository.OpenSession())
                {
                    var server = session.Query<RavenServer>().Single();
                    serverId = repository.GetId(typeof(RavenServer), "server");

                    Assert.Equal(serverId, server.Id);
                    Assert.Equal(context1.WorkerCount, server.Data.WorkerCount);
                    Assert.Equal(context1.Queues, server.Data.Queues);
                    Assert.NotNull(server.LastHeartbeat);
                }

                var context2 = new ServerContext
                {
                    Queues = new[] { "default" },
                    WorkerCount = 1000
                };
                connection.AnnounceServer("server", context2);

                using (var session = repository.OpenSession())
                {
                    var sameServer = session.Query<RavenServer>().Single();

                    Assert.Equal(serverId, sameServer.Id);
                    Assert.Equal(context2.WorkerCount, sameServer.Data.WorkerCount);
                }
            });
        }

        [Fact]
        public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection((repository, connection) => Assert.Throws<ArgumentNullException>(
                () => connection.RemoveServer(null)));
        }

        [Fact]
        public void RemoveServer_RemovesAServerRecord()
        {
            UseConnection((repository, connection) =>
            {
                using (var session = repository.OpenSession())
                {
                    session.Store(new RavenServer()
                    {
                        Id = repository.GetId(typeof(RavenServer), "Server1"),
                        Data = null,
                        LastHeartbeat = DateTime.UtcNow
                    });
                    session.Store(new RavenServer()
                    {
                        Id = repository.GetId(typeof(RavenServer), "Server2"),
                        Data = null,
                        LastHeartbeat = DateTime.UtcNow
                    });
                    session.SaveChanges();
                }

                connection.RemoveServer("Server1");

                using (var session = repository.OpenSession())
                {
                    var server = session.Query<RavenServer>().Single();
                    Assert.NotEqual("Server1", server.Id, StringComparer.OrdinalIgnoreCase);
                }
            });
        }

        [Fact]
        public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection((repository, connection) => Assert.Throws<ArgumentNullException>(
                () => connection.Heartbeat(null)));
        }

        [Fact]
        public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
        {
            UseConnection((repository, connection) =>
            {
                var server1 = new RavenServer()
                {
                    Id = repository.GetId(typeof(RavenServer), "server1"),
                    Data = null,
                    LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                };
                var server2 = new RavenServer()
                {
                    Id = repository.GetId(typeof(RavenServer), "server2"),
                    Data = null,
                    LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                };

                using (var session = repository.OpenSession())
                {
                    session.Store(server1);
                    session.Store(server2);
                    session.SaveChanges();
                }

                connection.Heartbeat("server1");

                using (var session = repository.OpenSession())
                {
                    var servers = session.Query<RavenServer>()
                        .ToDictionary(x => x.Id, x => x.LastHeartbeat);

                    Assert.NotEqual(2012, servers[server1.Id].Year);
                    Assert.Equal(2012, servers[server2.Id].Year);
                }
            });
        }

        [Fact]
        public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
        {
            UseConnection((repository, connection) => Assert.Throws<ArgumentException>(
                () => connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-5))));
        }

        [Fact]
        public void RemoveTimedOutServers_DoItsWorkPerfectly()
        {
            UseConnection((repository, connection) =>
            {
                var server1 = new RavenServer()
                {
                    Id = repository.GetId(typeof(RavenServer), "server1"),
                    Data = null,
                    LastHeartbeat = DateTime.UtcNow.AddDays(-1)
                };
                var server2 = new RavenServer()
                {
                    Id = repository.GetId(typeof(RavenServer), "server2"),
                    Data = null,
                    LastHeartbeat = DateTime.UtcNow.AddHours(-12)
                };

                using (var session = repository.OpenSession())
                {
                    session.Store(server1);
                    session.Store(server2);
                    session.SaveChanges();
                }

                connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

                using (var session = repository.OpenSession())
                {
                    var liveServer = session.Query<RavenServer>().Single();
                    Assert.Equal(server2.Id, liveServer.Id);
                }
            });
        }

        [Fact]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllItemsFromSet(null)));
        }

        [Fact]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetAllItemsFromSet("some-set");

                Assert.NotNull(result);
                Assert.Equal(0, result.Count);
            });
        }

        [Fact]
        public void GetAllItemsFromSet_ReturnsAllItems()
        {
            UseConnection((repository, connection) =>
            {
                // Arrange
                using (var session = repository.OpenSession())
                {
                    session.Store(new RavenSet
                    {
                        Id = repository.GetId(typeof(RavenSet), "some-set"),
                        Scores = new Dictionary<string, double>
                        {
                            { "1", 0.0 },
                            { "2", 0.0 }
                        }
                    });
                    session.Store(new RavenSet
                    {
                        Id = repository.GetId(typeof(RavenSet), "another-set"),
                        Scores = new Dictionary<string, double>
                        {
                            { "3", 0.0 }
                        }
                    });
                    session.SaveChanges();
                }

                // Act
                var result = connection.GetAllItemsFromSet("some-set");

                // Assert
                Assert.Equal(2, result.Count);
                Assert.Contains("1", result);
                Assert.Contains("2", result);
            });
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash(null, new Dictionary<string, string>()));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash("some-hash", null));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnection((repository, connection) =>
            {
                connection.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                });

                using (var session = repository.OpenSession())
                {
                    var result = session.Load<RavenHash>(repository.GetId(typeof(RavenHash), "some-hash")).Fields;

                    Assert.Equal("Value1", result["Key1"]);
                    Assert.Equal("Value2", result["Key2"]);
                }
            });
        }

        [Fact]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllEntriesFromHash(null)));
        }

        [Fact]
        public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetAllEntriesFromHash("some-hash");
                Assert.Null(result);
            });
        }

        [Fact]
        public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
        {
            UseConnection((repository, connection) =>
            {
                // Arrange
                using (var session = repository.OpenSession())
                {
                    session.Store(new RavenHash
                    {
                        Id = repository.GetId(typeof(RavenHash), "some-hash"),
                        Fields = new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" }
                        }
                    });
                    session.Store(new RavenHash
                    {
                        Id = repository.GetId(typeof(RavenHash), "another-hash"),
                        Fields = new Dictionary<string, string>
                        {
                            { "Key3", "Value3" }
                        }
                    });
                    session.SaveChanges();
                }

                // Act
                var result = connection.GetAllEntriesFromHash("some-hash");

                // Assert
                Assert.NotNull(result);
                Assert.Equal(2, result.Count);
                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact]
        public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetSetCount(null));
            });
        }

        [Fact]
        public void GetSetCount_ReturnsZero_WhenSetDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetSetCount("my-set");
                Assert.Equal(0, result);
            });
        }

        [Fact]
        public void GetSetCount_ReturnsNumberOfElements_InASet()
        {
            UseConnection((repository, connection) =>
            {
                using (var session = repository.OpenSession())
                {
                    session.Store(new RavenSet
                    {
                        Id = repository.GetId(typeof(RavenSet), "set-1"),
                        Scores = new Dictionary<string, double>
                        {
                            { "value-1", 0.0 },
                            { "value-2", 0.0 }
                        }
                    });
                    session.Store(new RavenSet
                    {
                        Id = repository.GetId(typeof(RavenSet), "set-2"),
                        Scores = new Dictionary<string, double>
                        {
                            { "value-1", 0.0 }
                        }
                    });
                    session.SaveChanges();
                }

                var result = connection.GetSetCount("set-1");

                Assert.Equal(2, result);
            });
        }

        [Fact]
        public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                Assert.Throws<ArgumentNullException>(() => connection.GetRangeFromSet(null, 0, 1));
            });
        }

        [Fact]
        public void GetRangeFromSet_ReturnsPagedElements()
        {
            UseConnection((repository, connection) =>
            {
                using (var session = repository.OpenSession())
                {
                    session.Store(new RavenSet
                    {
                        Id = repository.GetId(typeof(RavenSet), "set-1"),
                        Scores = new Dictionary<string, double>
                        {
                            { "1", 0.0 },
                            { "2", 0.0 },
                            { "3", 0.0 },
                            { "4", 0.0 },
                            { "5", 0.0 }
                        }
                    });
                    session.Store(new RavenSet
                    {
                        Id = repository.GetId(typeof(RavenSet), "set-2"),
                        Scores = new Dictionary<string, double>
                        {
                            { "4", 0.0 }
                        }
                    });
                    session.SaveChanges();
                }

                var result = connection.GetRangeFromSet("set-1", 2, 3);

                Assert.Equal(new[] { "3", "4" }, result);
            });
        }

        [Fact]
        public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                Assert.Throws<ArgumentNullException>(() => connection.GetSetTtl(null));
            });
        }

        [Fact]
        public void GetSetTtl_ReturnsNegativeValue_WhenSetDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetSetTtl("my-set");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetSetTtl_ReturnsExpirationTime_OfAGivenSet()
        {
            UseConnection((repository, connection) =>
            {
                // Arrange
                using (var session = repository.OpenSession())
                {
                    var ravenSet = new RavenSet
                    {
                        Id = repository.GetId(typeof(RavenSet), "set-1"),
                        Scores = new Dictionary<string, double>
                        {
                            { "1", 0.0 }
                        }
                    };
                    session.Store(ravenSet);
                    session.Advanced.AddExpire(ravenSet, DateTime.UtcNow.AddMinutes(60));
                    session.Store(new RavenSet
                    {
                        Id = repository.GetId(typeof(RavenSet), "set-2"),
                        Scores = new Dictionary<string, double>
                        {
                            { "2", 0.0 }
                        }
                    });
                    session.SaveChanges();
                }

                // Act
                var result = connection.GetSetTtl("set-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact]
        public void GetCounter_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetCounter(null));
            });
        }

        [Fact]
        public void GetCounter_ReturnsZero_WhenKeyDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetCounter("my-counter");
                Assert.Equal(0, result);
            });
        }

        [Fact]
        public void GetCounter_ReturnsValue_WhenCounterExists()
        {
            UseConnection((repository, connection) =>
            {
                // Arrange
                using (var session = repository.OpenSession())
                {
                    session.Store(new Counter
                    {
                        Id = repository.GetId(typeof(Counter), "counter-1"),
                        Value = 2
                    });
                    session.Store(new Counter
                    {
                        Id = repository.GetId(typeof(Counter), "counter-2"),
                        Value = 1
                    });
                    session.SaveChanges();
                }

                // Act
                var result = connection.GetCounter("counter-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact]
        public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                Assert.Throws<ArgumentNullException>(() => connection.GetHashCount(null));
            });
        }

        [Fact]
        public void GetHashCount_ReturnsZero_WhenKeyDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetHashCount("my-hash");
                Assert.Equal(0, result);
            });
        }

        [Fact]
        public void GetHashCount_ReturnsNumber_OfHashFields()
        {
            UseConnection((repository, connection) =>
            {
                // Arrange
                using (var session = repository.OpenSession())
                {
                    session.Store(new RavenHash
                    {
                        Id = repository.GetId(typeof(RavenHash), "hash-1"),
                        Fields = new Dictionary<string, string>
                        {
                            { "field-1", "" },
                            { "field-2", "" }
                        }
                    });
                    session.Store(new RavenHash
                    {
                        Id = repository.GetId(typeof(RavenHash), "hash-2"),
                        Fields = new Dictionary<string, string>
                        {
                            { "field-1", "" }
                        }
                    });
                    session.SaveChanges();
                }

                // Act
                var result = connection.GetHashCount("hash-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact]
        public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetHashTtl(null));
            });
        }

        [Fact]
        public void GetHashTtl_ReturnsNegativeValue_WhenHashDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetHashTtl("my-hash");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetHashTtl_ReturnsExpirationTimeForHash()
        {
            UseConnection((repository, connection) =>
            {
                // Arrange
                using (var session = repository.OpenSession())
                {
                    var ravenHash = new RavenHash
                    {
                        Id = repository.GetId(typeof(RavenHash), "hash-1"),
                        Fields = new Dictionary<string, string>
                        {
                            { "field", "" }
                        }
                    };
                    session.Store(ravenHash);
                    session.Advanced.AddExpire(ravenHash, DateTime.UtcNow.AddHours(1));
                    session.Store(new RavenHash
                    {
                        Id = repository.GetId(typeof(RavenHash), "hash-2"),
                        Fields = new Dictionary<string, string>
                        {
                            { "field", "" }
                        }
                    });
                    session.SaveChanges();
                }

                // Act
                var result = connection.GetHashTtl("hash-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact]
        public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash(null, "name"));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash("key", null));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetValueFromHash("my-hash", "name");
                Assert.Null(result);
            });
        }

        [Fact]
        public void GetValueFromHash_ReturnsValue_OfAGivenField()
        {
            UseConnection((repository, connection) =>
            {
                // Arrange
                using (var session = repository.OpenSession())
                {
                    session.Store(new RavenHash
                    {
                        Id = repository.GetId(typeof(RavenHash), "hash-1"),
                        Fields = new Dictionary<string, string>
                        {
                            { "field-1", "1" },
                            { "field-2", "2" }
                        }
                    });
                    session.Store(new RavenHash
                    {
                        Id = repository.GetId(typeof(RavenHash), "hash-2"),
                        Fields = new Dictionary<string, string>
                        {
                            { "field-1", "3" }
                        }
                    });
                    session.SaveChanges();
                }

                // Act
                var result = connection.GetValueFromHash("hash-1", "field-1");

                // Assert
                Assert.Equal("1", result);
            });
        }

        [Fact]
        public void GetListCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetListCount(null));
            });
        }

        [Fact]
        public void GetListCount_ReturnsZero_WhenListDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetListCount("my-list");
                Assert.Equal(0, result);
            });
        }

        [Fact]
        public void GetListCount_ReturnsTheNumberOfListElements()
        {
            UseConnection((repository, connection) =>
            {
                // Arrange
                using (var session = repository.OpenSession())
                {
                    session.Store(new RavenList
                    {
                        Id = repository.GetId(typeof(RavenList), "list-1"),
                        Values = new List<string> { "1", "2" }
                    });
                    session.Store(new RavenList
                    {
                        Id = repository.GetId(typeof(RavenList), "list-2"),
                        Values = new List<string> { "1" }
                    });
                    session.SaveChanges();
                }

                // Act
                var result = connection.GetListCount("list-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact]
        public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetListTtl(null));
            });
        }

        [Fact]
        public void GetListTtl_ReturnsNegativeValue_WhenListDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetListTtl("my-list");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetListTtl_ReturnsExpirationTimeForList()
        {
            UseConnection((repository, connection) =>
            {
                // Arrange
                using (var session = repository.OpenSession())
                {
                    var ravenList = new RavenList
                    {
                        Id = repository.GetId(typeof(RavenList), "list-1"),
                        Values = new List<string> { "1" }
                    };
                    session.Store(ravenList);
                    session.Advanced.AddExpire(ravenList, DateTime.UtcNow.AddHours(1));
                    session.Store(new RavenList
                    {
                        Id = repository.GetId(typeof(RavenList), "list-2"),
                        Values = new List<string> { "1" }
                    });
                    session.SaveChanges();
                }

                // Act
                var result = connection.GetListTtl("list-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact]
        public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetRangeFromList(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetRangeFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetRangeFromList("my-list", 0, 1);
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetRangeFromList_ReturnsAllEntries_WithinGivenBounds()
        {
            UseConnection((repository, connection) =>
            {
                // Arrange
                using (var session = repository.OpenSession())
                {
                    session.Store(new RavenList
                    {
                        Id = repository.GetId(typeof(RavenList), "list-1"),
                        Values = new List<string> { "1", "3", "4", "5" }
                    });
                    session.Store(new RavenList
                    {
                        Id = repository.GetId(typeof(RavenList), "list-2"),
                        Values = new List<string> { "2" }
                    });
                    session.SaveChanges();
                }

                // Act
                var result = connection.GetRangeFromList("list-1", 1, 2);

                // Assert
                Assert.Equal(new[] { "3", "4" }, result);
            });
        }

        [Fact]
        public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((repository, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetAllItemsFromList(null));
            });
        }

        [Fact]
        public void GetAllItemsFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            UseConnection((repository, connection) =>
            {
                var result = connection.GetAllItemsFromList("my-list");
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetAllItemsFromList_ReturnsAllItems_FromAGivenList()
        {
            UseConnection((repository, connection) =>
            {
                // Arrange
                using (var session = repository.OpenSession())
                {
                    session.Store(new RavenList
                    {
                        Id = repository.GetId(typeof(RavenList), "list-1"),
                        Values = new List<string> { "1", "3" }
                    });
                    session.Store(new RavenList
                    {
                        Id = repository.GetId(typeof(RavenList), "list-2"),
                        Values = new List<string> { "2" }
                    });
                    session.SaveChanges();
                }

                // Act
                var result = connection.GetAllItemsFromList("list-1");

                // Assert
                Assert.Equal(new[] { "1", "3" }, result);
            });
        }

        private void UseConnection(Action<IRepository, RavenConnection> action)
        {
            using (var repository = new TestRepository())
            {
                var storage = new Mock<RavenStorage>(repository);
                storage.Setup(x => x.QueueProviders).Returns(_providers);

                using (var connection = new RavenConnection(storage.Object))
                {
                    action(repository, connection);
                }
            }
        }

        public static void SampleMethod(string arg)
        {
            Debug.WriteLine(arg);
        }
    }
}
