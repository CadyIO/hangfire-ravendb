using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Raven.Entities;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Hangfire.Raven.JobQueues;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;
using Hangfire.Raven.Extensions;

namespace Hangfire.Raven.Storage {
    public class RavenStorageMonitoringApi
        : IMonitoringApi {
        private RavenStorage _storage;

        public RavenStorageMonitoringApi([NotNull]RavenStorage storage) {
            storage.ThrowIfNull("storage");

            _storage = storage;
        }

        public long EnqueuedCount(string queue) {
            var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.EnqueuedCount ?? 0;
        }
        public long FetchedCount(string queue) {
            var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.FetchedCount ?? 0;

        }
        public long DeletedListCount() {
            return GetNumberOfJobsByStateName(DeletedState.StateName);
        }
        public long FailedCount() {
            return GetNumberOfJobsByStateName(FailedState.StateName);
        }
        public long ProcessingCount() {
            return GetNumberOfJobsByStateName(ProcessingState.StateName);
        }
        public long ScheduledCount() {
            return GetNumberOfJobsByStateName(ScheduledState.StateName);
        }
        public long SucceededListCount() {
            return GetNumberOfJobsByStateName(SucceededState.StateName);
        }
        private long GetNumberOfJobsByStateName(string stateName) {
            using (var session = _storage.Repository.OpenSession()) {
                return session.Query<RavenJob>().Count(x => x.StateData.Name == stateName);
            }
        }

        public IDictionary<DateTime, long> FailedByDatesCount() {
            return GetTimelineStats("failed");
        }
        public IDictionary<DateTime, long> HourlyFailedJobs() {
            return GetHourlyTimelineStats("failed");
        }
        public IDictionary<DateTime, long> HourlySucceededJobs() {
            return GetHourlyTimelineStats("succeeded");
        }
        public IDictionary<DateTime, long> SucceededByDatesCount() {
            return GetTimelineStats("succeeded");
        }
        private Dictionary<DateTime, long> GetHourlyTimelineStats(string type) {
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();

            for (var i = 0; i < 24; i++) {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            return GetTimelineStats(dates, x => string.Format("stats:{0}:{1}", type, x.ToString("yyyy-MM-dd-HH")));
        }
        private Dictionary<DateTime, long> GetTimelineStats(string type) {
            var endDate = DateTime.UtcNow.Date;
            var dates = new List<DateTime>();

            for (var i = 0; i < 7; i++) {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            return GetTimelineStats(dates, x => string.Format("stats:{0}:{1}", type, x.ToString("yyyy-MM-dd")));
        }
        private Dictionary<DateTime, long> GetTimelineStats(List<DateTime> dates,
            Func<DateTime, string> formatorAction) {
            var stats = new Dictionary<DateTime, long>();
            using (var repository = _storage.Repository.OpenSession()) {
                foreach (var item in dates) {
                    var id = _storage.Repository.GetId(typeof(Counter), formatorAction(item));
                    var counters = repository.Load<Counter>(id);

                    if (counters != null)
                        stats.Add(item, counters.Value);
                    else
                        stats.Add(item, 0);
                }
            }

            return stats;
        }
        public StatisticsDto GetStatistics() {
            using (var session = _storage.Repository.OpenSession()) {
                session.Query<RavenServer>()
                    .Statistics(out QueryStatistics stat)
                    .Take(0)
                    .ToList();

                var recurringJobs = session.Load<RavenSet>(_storage.Repository.GetId(typeof(RavenSet), "recurring-jobs"));

                var jobs = session.Query<RavenJob>().GroupBy(x => x.StateData.Name).Select(x => new { state = x.Key, count = x.Count() }).ToList();

                var jobQueueCount = session.Query<JobQueue>().Count();

                return new StatisticsDto() {
                    Servers = stat.TotalResults,
                    Queues = jobQueueCount,
                    Recurring = recurringJobs?.Scores?.Count ?? 0,
                    Succeeded = jobs.FirstOrDefault(a => a.state == SucceededState.StateName)?.count ?? 0,
                    Scheduled = jobs.FirstOrDefault(a => a.state == ScheduledState.StateName)?.count ?? 0,
                    Enqueued = jobs.FirstOrDefault(a => a.state == EnqueuedState.StateName)?.count ?? 0,
                    Failed = jobs.FirstOrDefault(a => a.state == FailedState.StateName)?.count ?? 0,
                    Processing = jobs.FirstOrDefault(a => a.state == ProcessingState.StateName)?.count ?? 0,
                    Deleted = jobs.FirstOrDefault(a => a.state == DeletedState.StateName)?.count ?? 0,
                };
            }
        }




        public JobList<DeletedJobDto> DeletedJobs(int from, int count) {
            return GetJobs(
                from,
                count,
                DeletedState.StateName,
                (jsonJob, job, stateData) => new DeletedJobDto {
                    Job = job,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
                });
        }
        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage) {
            var queueApi = GetQueueApi(queue);
            var enqueuedJobIds = queueApi.GetEnqueuedJobIds(queue, from, perPage);

            return EnqueuedJobs(enqueuedJobIds);
        }
        public JobList<FailedJobDto> FailedJobs(int from, int count) {
            return GetJobs(from, count,
                FailedState.StateName,
                (jsonJob, job, stateData) => new FailedJobDto {
                    Job = job,
                    Reason = jsonJob.StateData.Reason,
                    ExceptionDetails = stateData["ExceptionDetails"],
                    ExceptionMessage = stateData["ExceptionMessage"],
                    ExceptionType = stateData["ExceptionType"],
                    FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
                });
        }
        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage) {
            var queueApi = GetQueueApi(queue);
            var fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, perPage);

            return FetchedJobs(fetchedJobIds);
        }
        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count) {
            return GetJobs(from, count,
                ScheduledState.StateName,
                (jsonJob, job, stateData) => new ScheduledJobDto {
                    Job = job,
                    EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                    ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
                });
        }
        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count) {
            return GetJobs(from, count,
                ProcessingState.StateName,
                (jsonJob, job, stateData) => new ProcessingJobDto {
                    Job = job,
                    ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                    StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"])
                });
        }
        public JobList<SucceededJobDto> SucceededJobs(int from, int count) {
            var toReturn = GetJobs(from, count,
                SucceededState.StateName,
                (jsonJob, job, stateData) => new SucceededJobDto {
                    Job = job,
                    InSucceededState = true,
                    Result = stateData.ContainsKey("Result") ? stateData["Result"] : null,
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                        ? (long?)long.Parse(stateData["PerformanceDuration"]) +
                          (long?)long.Parse(stateData["Latency"])
                        : null,
                    SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
                });
            return toReturn;
        }




        public JobDetailsDto JobDetails(string jobId) {
            jobId.ThrowIfNull("jobId");

            using (var session = _storage.Repository.OpenSession()) {
                var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
                var job = session.Load<RavenJob>(id);

                if (job == null) {
                    return null;
                }

                return new JobDetailsDto {
                    CreatedAt = job.CreatedAt,
                    ExpireAt = session.GetExpiry(job),
                    Job = DeserializeJob(job.InvocationData),
                    History = job.History,
                    Properties = job.Parameters
                };
            }
        }
        public IList<QueueWithTopEnqueuedJobsDto> Queues() {
            using (var session = _storage.Repository.OpenSession()) {
                var query = session.Query<JobQueue>().ToList();

                var results = from item in query
                              group item by item.Queue into g
                              let total = g.Count()
                              let fetched = g.Count(a => a.FetchedAt.HasValue)
                              select new QueueWithTopEnqueuedJobsDto() {
                                  Name = g.Key,
                                  Length = total - fetched,
                                  Fetched = fetched,
                                  FirstJobs = EnqueuedJobs(g.Take(5).Select(a => a.JobId))
                              };


                return results.ToList();
            }
        }
        public IList<ServerDto> Servers() {
            using (var repository = _storage.Repository.OpenSession()) {
                var servers = repository.Query<RavenServer>().ToList();

                var query =
                    from server in servers
                    select new ServerDto {
                        Name = server.Id.Split(new[] { '/' }, 2)[1],
                        Heartbeat = server.LastHeartbeat,
                        Queues = server.Data.Queues.ToList(),
                        StartedAt = server.Data.StartedAt ?? DateTime.MinValue,
                        WorkersCount = server.Data.WorkerCount
                    };

                return query.ToList();
            }
        }


        private JobList<TDto> GetJobs<TDto>(
            int from,
            int count,
            string stateName,
            Func<RavenJob, Job, Dictionary<string, string>, TDto> selector) {
            using (var repository = _storage.Repository.OpenSession()) {
                var jobs = repository.Query<RavenJob>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(a => a.StateData.Name == stateName)
                    .OrderByDescending(a => a.CreatedAt)
                    .Skip(from)
                    .Take(count)
                    .ToList();

                return DeserializeJobs(jobs, selector);
            }
        }
        private JobList<FetchedJobDto> FetchedJobs(IEnumerable<string> jobIds) {
            using (var repository = _storage.Repository.OpenSession()) {
                var jobs = repository.Load<RavenJob>(
                        jobIds.Select(a => _storage.Repository.GetId(typeof(RavenJob), a))
                    )
                    .Where(a => a.Value != null)
                    .Select(p => p.Value)
                    .ToList();

                return DeserializeJobs(jobs, (jsonJob, job, stateData) => new FetchedJobDto {
                    Job = job,
                    State = jsonJob.StateData?.Name,
                    FetchedAt = jsonJob.StateData?.Name == ProcessingState.StateName
                        ? JobHelper.DeserializeNullableDateTime(stateData["StartedAt"])
                        : null
                });
            }
        }
        private JobList<EnqueuedJobDto> EnqueuedJobs(IEnumerable<string> jobIds) {
            using (var repository = _storage.Repository.OpenSession()) {
                var jobs = repository.Load<RavenJob>(
                        jobIds.Select(a => _storage.Repository.GetId(typeof(RavenJob), a))
                    )
                    .Where(a => a.Value != null)
                    .Select(p => p.Value)
                    .ToList();

                return DeserializeJobs(jobs, (jsonJob, job, stateData) => new EnqueuedJobDto {
                    Job = job,
                    State = jsonJob.StateData?.Name,
                    EnqueuedAt = jsonJob.StateData?.Name == EnqueuedState.StateName
                        ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                        : null
                });
            }
        }



        private Job DeserializeJob(InvocationData invocationData) {
            try {
                return invocationData.Deserialize();
            } catch (JobLoadException) {
                return null;
            }
        }

        private JobList<TDto> DeserializeJobs<TDto>(
            IEnumerable<RavenJob> jobs,
            Func<RavenJob, Job, Dictionary<string, string>, TDto> selector) {
            var result = from job in jobs
                         let stateData = (job.StateData != null) && (job.StateData.Data != null)
                             ? new Dictionary<string, string>(job.StateData.Data, StringComparer.OrdinalIgnoreCase)
                             : null
                         let dto = selector(job, DeserializeJob(job.InvocationData), stateData)
                         select new KeyValuePair<string, TDto>(job.Id.Split(new char[] { '/' }, 2)[1], dto);

            return new JobList<TDto>(result);
        }

        private IPersistentJobQueueMonitoringApi GetQueueApi(string queueName) {
            var provider = _storage.QueueProviders.GetProvider(queueName);
            var monitoringApi = provider.GetJobQueueMonitoringApi();

            return monitoringApi;
        }
    }
}