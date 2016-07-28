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
using Hangfire.Annotations;
using Hangfire.Raven.Storage;

namespace Hangfire.Raven
{
    public class ExpirationManager : IBackgroundProcess, IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private readonly TimeSpan _checkInterval;
        private RavenStorage _storage;

        public ExpirationManager(RavenStorage storage, TimeSpan checkInterval)
        {
            _checkInterval = checkInterval;
            _storage = storage;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            using (var repository = _storage.Repository.OpenSession())
            {
                var now = DateTime.UtcNow;

                var counters = repository.Query<Counter>().Where(t => t.ExpireAt <= now).ToList();
                counters.ForEach(t => repository.Delete(t));

                var jobs = repository.Query<RavenJob>().Where(t => t.ExpireAt <= now).ToList();
                jobs.ForEach(t => repository.Delete(t));

                var hashes = repository.Query<RavenHash>().Where(t => t.ExpireAt <= now).ToList();
                hashes.ForEach(t => repository.Delete(t));

                var lists = repository.Query<RavenList>().Where(t => t.ExpireAt <= now).ToList();
                lists.ForEach(t => repository.Delete(t));

                var sets = repository.Query<RavenSet>().Where(t => t.ExpireAt <= now).ToList();
                sets.ForEach(t => repository.Delete(t));

                repository.SaveChanges();
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        public void Execute([NotNull] BackgroundProcessContext context)
        {
            this.Execute(context.CancellationToken);
        }

        public override string ToString()
        {
            return "Raven Expiration Manager";
        }
    }
}