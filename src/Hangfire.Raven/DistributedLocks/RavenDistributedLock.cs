using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;

namespace Hangfire.Raven.DistributedLocks
{
    public class RavenDistributedLock : IDisposable
    {
        private const int CommandTimeoutAdditionSeconds = 1;
        private readonly object _lock = new object();

        private RavenStorage _storage;
        private string _resource;
        private DistributedLock _distributedLock;
        private Timer _heartbeatTimer;
        private TimeSpan _timeout;

        private readonly List<string> _skipLocks = new List<string>()
        {
            "HangFire/job:"
        };

        public RavenDistributedLock([NotNull] RavenStorage storage, [NotNull] string resource, TimeSpan timeout)
        {
            storage.ThrowIfNull("storage");
            resource.ThrowIfNull("resource");

            _timeout = timeout;
            _storage = storage;
            _resource = resource;

            // -- Skip some locks
            if (!_skipLocks.Any(a => _resource.StartsWith(a)))
                Lock();
        }

        public void Dispose()
        {
            Release();
        }

        private void Lock()
        {
            using (var session = _storage.Repository.OpenSession()) {
                _distributedLock = new DistributedLock() {
                    ClientId = _storage.Options.ClientId,
                    Resource = _resource
                };

                session.Store(_distributedLock);
                session.Advanced.AddExpire(_distributedLock, DateTime.UtcNow + _timeout);

                try {
                    // Blocking session!
                    session.Advanced.UseOptimisticConcurrency = true;
                    session.SaveChanges();
                } catch (Exception e) {
                    _distributedLock = null;
                    throw new RavenDistributedLockException("Lock already given.", e);
                }
            }

            Heartbeat();
        }

        private void Release()
        {
            lock (_lock) {
                if (_distributedLock != null) {
                    // Non blocking session!
                    try {
                        using (var session = _storage.Repository.OpenSession()) {
                            session.Delete(_distributedLock.Id);
                            session.SaveChanges();
                        }
                    } catch {
                        Console.WriteLine("Unable to delete lock: {0}", _resource);
                    }

                    _distributedLock = null;
                }

                // Stop timer
                if (_heartbeatTimer != null) {
                    _heartbeatTimer.Dispose();
                    _heartbeatTimer = null;
                }
            }
        }

        private void Heartbeat()
        {
            Console.WriteLine(".Starting heartbeat for resource: {0}", _resource);
            TimeSpan timerInterval = TimeSpan.FromMilliseconds(_timeout.TotalMilliseconds / 3);

            _heartbeatTimer = new Timer(state => {
                try {
                    Console.WriteLine("..Heartbeat for resource {0}", _resource);
                    using (var session = _storage.Repository.OpenSession()) {
                        var distributedLock = session.Load<DistributedLock>(_distributedLock.Id);

                        session.Advanced.AddExpire(distributedLock, DateTime.UtcNow + _timeout);
                        session.SaveChanges();
                    }
                } catch (Exception ex) {
                    Console.WriteLine("...Unable to update heartbeat on the resource '{0}'", ex, _resource);
                    Release();
                }
            }, null, timerInterval, timerInterval);
        }
    }
}