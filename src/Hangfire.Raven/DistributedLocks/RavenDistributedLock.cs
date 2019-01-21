using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.Logging;
using Hangfire.Storage;
using Raven.Client.Exceptions;
using Hangfire.Raven.Extensions;

namespace Hangfire.Raven.DistributedLocks {
    public class RavenDistributedLock : IDisposable {
        private static readonly ILog Logger = LogProvider.For<RavenDistributedLock>();

        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks
                    = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());

        private static readonly TimeSpan KeepAliveInterval = TimeSpan.FromMinutes(1);

        private RavenStorage _storage;

        private string _resource;

        private readonly RavenStorageOptions _options;

        private DistributedLock _distributedLock;

        private Timer _heartbeatTimer;

        private bool _completed;

        private readonly object _lockObject = new object();

        private string EventWaitHandleName => $@"{GetType().FullName}.{_resource}";

        public RavenDistributedLock(RavenStorage storage, string resource, TimeSpan timeout, RavenStorageOptions options) {
            storage.ThrowIfNull("storage");
            if (string.IsNullOrEmpty(resource)) {
                throw new ArgumentNullException(nameof(resource));
            }
            if (timeout.TotalSeconds > int.MaxValue) {
                throw new ArgumentException($"The timeout specified is too large. Please supply a timeout equal to or less than {int.MaxValue} seconds", nameof(timeout));
            }
            options.ThrowIfNull("options");

            _storage = storage;
            _resource = resource;
            _options = options;

            if (!AcquiredLocks.Value.ContainsKey(_resource) || AcquiredLocks.Value[_resource] == 0) {
                Acquire(timeout);
                AcquiredLocks.Value[_resource] = 1;
                StartHeartBeat();
            } else {
                AcquiredLocks.Value[_resource]++;
            }
        }

        public void Dispose() {
            if (_completed) return;

            _completed = true;

            if (!AcquiredLocks.Value.ContainsKey(_resource)) return;

            AcquiredLocks.Value[_resource]--;

            if (AcquiredLocks.Value[_resource] > 0) return;

            lock (_lockObject) {
                AcquiredLocks.Value.Remove(_resource);

                if (_heartbeatTimer != null) {
                    _heartbeatTimer.Dispose();
                    _heartbeatTimer = null;
                }

                Release();
            }
        }

        private void Acquire(TimeSpan timeout) {
            try {
                var lockTimeoutTime = DateTime.Now.Add(timeout);
                var waitFor = (int)(timeout.TotalMilliseconds > 10000 ? 2000 : timeout.TotalMilliseconds / 5);

                while (lockTimeoutTime >= DateTime.Now) {
                    _distributedLock = new DistributedLock() {
                        ClientId = _storage.Options.ClientId,
                        Resource = _resource
                    };

                    using (var session = _storage.Repository.OpenSession()) {
                        session.Advanced.UseOptimisticConcurrency = true;

                        session.Store(_distributedLock);
                        session.SetExpiry(_distributedLock, _options.DistributedLockLifetime);

                        try {
                            // Blocking session!                 
                            session.SaveChanges();
                            return;
                        } catch (ConcurrencyException) {
                            _distributedLock = null;
                            try {
                                var eventWaitHandle = new EventWaitHandle(false, EventResetMode.AutoReset, EventWaitHandleName);
                                eventWaitHandle.WaitOne(waitFor);
                            } catch (PlatformNotSupportedException) {
                                Thread.Sleep(waitFor);
                            }
                        }
                    }
                }

                throw new DistributedLockTimeoutException(_resource);
            } catch (DistributedLockTimeoutException) {
                throw;
            } catch (Exception ex) {
                throw new RavenDistributedLockException($"Could not place a lock on the resource \'{_resource}\': Check inner exception for details.", ex);
            }
        }

        private void Release() {
            try {
                if (_distributedLock != null) {
                    // Non blocking session!
                    using (var session = _storage.Repository.OpenSession()) {
                        session.Delete(_distributedLock.Id);
                        session.SaveChanges();
                        _distributedLock = null;
                    }
                }
                if (EventWaitHandle.TryOpenExisting(EventWaitHandleName, out EventWaitHandle eventWaitHandler)) {
                    eventWaitHandler.Set();
                }
            } catch (PlatformNotSupportedException) {
            } catch (Exception ex) {
                _distributedLock = null;
                throw new RavenDistributedLockException($"Could not release a lock on the resource \'{_resource}\': Check inner exception for details.", ex);
            }
        }

        private void StartHeartBeat() {
            Logger.InfoFormat(".Starting heartbeat for resource: {0}", _resource);

            _heartbeatTimer = new Timer(state => {
                lock (_lockObject) {
                    try {
                        Logger.InfoFormat("..Heartbeat for resource {0}", _resource);

                        using (var session = _storage.Repository.OpenSession()) {
                            session.SetExpiry(_distributedLock.Id, _options.DistributedLockLifetime);
                            session.SaveChanges();
                        }
                    } catch (Exception ex) {
                        Logger.ErrorFormat("...Unable to update heartbeat on the resource '{0}'. {1}", _resource, ex);
                        Release();
                    }
                }
            }, null, KeepAliveInterval, KeepAliveInterval);
        }
    }
}