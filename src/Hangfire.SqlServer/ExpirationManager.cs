// This file is part of Hangfire.
// Copyright © 2013-2014 Sergey Odinokov.
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
using System.Threading;
using Hangfire.Logging;
using Hangfire.Server;
using System.Linq;
using HangFire.Raven;
using Hangfire.Raven.Entities;

namespace Hangfire.Raven
{
    public class ExpirationManager : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private readonly TimeSpan _checkInterval;

        public ExpirationManager()
            : this(TimeSpan.FromHours(1))
        {
        }

        public ExpirationManager(TimeSpan checkInterval)
        {
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        { 
            using (var repository = new Repository()) {
                var session = repository.Session;

                var now = DateTime.UtcNow;

                var counters = session.Query<Counter>().Where(t => t.ExpireAt <= now).ToList();
                counters.ForEach(t => session.Delete(t));

                var jobs = session.Query<RavenJob>().Where(t => t.ExpireAt <= now).ToList();
                jobs.ForEach(t => session.Delete(t));

                var hashes = session.Query<RavenHash>().Where(t => t.ExpireAt <= now).ToList();
                hashes.ForEach(t => session.Delete(t));

                var lists = session.Query<RavenList>().Where(t => t.ExpireAt <= now).ToList();
                lists.ForEach(t => session.Delete(t));

                var sets = session.Query<RavenSet>().Where(t => t.ExpireAt <= now).ToList();
                sets.ForEach(t => session.Delete(t));
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        public override string ToString()
        {
            return "Raven Expiration Manager";
        }
    }
}