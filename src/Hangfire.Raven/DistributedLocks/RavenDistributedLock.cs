// This file is part of Hangfire.
// Copyright � 2013-2014 Sergey Odinokov.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using Hangfire.Annotations;
using HangFire.Raven;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;

namespace Hangfire.Raven.DistributedLock
{
    public class RavenDistributedLock : IDisposable
    {
        private const string LockMode = "Exclusive";
        private const string LockOwner = "Session";
        private const int CommandTimeoutAdditionSeconds = 1;

        private static readonly IDictionary<int, string> LockErrorMessages = new Dictionary<int, string>
        {
            { -1, "The lock request timed out" },
            { -2, "The lock request was canceled" },
            { -3, "The lock request was chosen as a deadlock victim" },
            { -999, "Indicates a parameter validation or other call error" }
        };

        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());

        private RavenStorage _storage;
        private string _resource;
        private RavenStorageOptions _options;

        private Timer _heartbeatTimer = null;

        private bool _completed;

        public RavenDistributedLock([NotNull] RavenStorage storage, [NotNull] string resource, TimeSpan timeout, RavenStorageOptions options)
        {
            storage.ThrowIfNull("storage");
            resource.ThrowIfNull("resource");

            if ((timeout.TotalSeconds + CommandTimeoutAdditionSeconds) > int.MaxValue) {
                throw new ArgumentException(string.Format("The timeout specified is too large. Please supply a timeout equal to or less than {0} seconds",
                                                          int.MaxValue - CommandTimeoutAdditionSeconds),
                                                          "timeout");
            }

            _storage = storage;
            _resource = resource;
            _options = options;

            if (!AcquiredLocks.Value.ContainsKey(_resource)) {
                Acquire(_resource, timeout);
                AcquiredLocks.Value[_resource] = 1;
            } else {
                AcquiredLocks.Value[_resource]++;
            }
        }

        public void Dispose()
        {
            if (_completed)
                return;

            _completed = true;

            if (!AcquiredLocks.Value.ContainsKey(_resource))
                return;

            AcquiredLocks.Value[_resource]--;

            if (AcquiredLocks.Value[_resource] != 0)
                return;

            Release(_resource);
            AcquiredLocks.Value.Remove(_resource);
        }

        internal void Acquire(string resource, TimeSpan timeout)
        {
            try {
                RemoveDeadLocks(resource);

                // Check lock
                DateTime lockTimeoutTime = DateTime.Now.Add(timeout);
                bool isLockedBySomeoneElse;
                bool isFirstAttempt = true;
                do {
                    using (var repository = new Repository()) {
                        isLockedBySomeoneElse = repository.Session.Query<DistributedLocks>()
                            .FirstOrDefault(t => t.Resource == resource && t.ClientId != _options.ClientId) != null;
                    }

                    if (isFirstAttempt == true) {
                        isFirstAttempt = false;
                    } else {
                        Thread.Sleep((int)timeout.TotalMilliseconds / 10);
                    }
                }
                while ((isLockedBySomeoneElse == true) && (lockTimeoutTime >= DateTime.Now));

                // Set lock
                if (isLockedBySomeoneElse == false) {
                    using (var repository = new Repository()) {
                        var distributedLocks = repository.Session.Query<DistributedLocks>().Where(t => t.Resource == resource).ToList();

                        if (!distributedLocks.Any()) {
                            distributedLocks.Add(new DistributedLocks
                            {
                                Resource = resource,
                            });
                        }

                        foreach (var distributedLock in distributedLocks) {
                            distributedLock.ClientId = _options.ClientId;
                            distributedLock.LockCount = 1;
                            distributedLock.Heartbeat = DateTime.UtcNow;

                            repository.Save(distributedLock);
                        }
                    }

                    StartHeartBeat(_resource);
                } else {
                    throw new RavenDistributedLockException(string.Format("Could not place a lock on the resource '{0}': {1}.", resource, "The lock request timed out"));
                }
            } catch (Exception ex) {
                if (ex is RavenDistributedLockException) {
                    throw;
                } else {
                    throw new RavenDistributedLockException(string.Format("Could not place a lock on the resource '{0}': {1}.", resource, "Check inner exception for details"), ex);
                }
            }
        }

        internal void Release(string resource)
        {
            try {
                RemoveDeadLocks(resource);

                // Remove resource lock
                using (var repository = new Repository()) {
                    var distributedLocks = repository.Session.Query<DistributedLocks>().Where(t => t.Resource == _resource && t.ClientId == _options.ClientId).ToList();

                    foreach (var distributedLock in distributedLocks) {
                        repository.Delete(distributedLock);
                    }
                }

                if (_heartbeatTimer != null) {
                    _heartbeatTimer.Dispose();
                    _heartbeatTimer = null;
                }
            } catch (Exception ex) {
                throw new RavenDistributedLockException(string.Format("Could not release a lock on the resource '{0}': {1}.", _resource, "Check inner exception for details"), ex);
            }
        }

        private void StartHeartBeat(string resource)
        {
            TimeSpan timerInterval = TimeSpan.FromMilliseconds(_options.DistributedLockLifetime.TotalMilliseconds / 5);

            _heartbeatTimer = new Timer(state =>
            {
                try {
                    using (var repository = new Repository()) {
                        var distributedLocks = repository.Session.Query<DistributedLocks>().Where(t => t.Resource == resource && t.ClientId == _options.ClientId).ToList();

                        foreach (var distributedLock in distributedLocks) {
                            distributedLock.Heartbeat = DateTime.UtcNow;
                            repository.Save(distributedLock);
                        }
                    }
                } catch (Exception ex) {
                    Console.WriteLine("Unable to update heartbeat on the resource '{0}'", ex, resource);
                }
            }, null, timerInterval, timerInterval);
        }

        private void RemoveDeadLocks(string resource)
        {
            using (var repository = new Repository()) {
                var heartBeat = DateTime.UtcNow.Subtract(_options.DistributedLockLifetime);
                var deadLocks = repository.Session.Query<DistributedLocks>().Where(t => t.Resource == resource && t.Heartbeat < heartBeat).ToList();

                foreach (var deadlock in deadLocks) {
                    repository.Delete(deadlock);
                }
            }
        }
    }
}