using System;
using System.Collections;
using System.Collections.Generic;

namespace Hangfire.Raven.JobQueues
{
    public class PersistentJobQueueProviderCollection : IEnumerable<IPersistentJobQueueProvider>
    {
        private readonly List<IPersistentJobQueueProvider> _providers
            = new List<IPersistentJobQueueProvider>();
        private readonly Dictionary<string, IPersistentJobQueueProvider> _providersByQueue
            = new Dictionary<string, IPersistentJobQueueProvider>(StringComparer.OrdinalIgnoreCase);

        private readonly IPersistentJobQueueProvider _defaultProvider;

        public PersistentJobQueueProviderCollection(IPersistentJobQueueProvider defaultProvider)
        {
            defaultProvider.ThrowIfNull("defaultProvider");

            _defaultProvider = defaultProvider;

            _providers.Add(_defaultProvider);
        }

        public void Add(IPersistentJobQueueProvider provider, IEnumerable<string> queues)
        {
            provider.ThrowIfNull("provider");
            queues.ThrowIfNull("queues");

            _providers.Add(provider);

            foreach (var queue in queues) {
                _providersByQueue.Add(queue, provider);
            }
        }

        public IPersistentJobQueueProvider GetProvider(string queue)
        {
            return _providersByQueue.ContainsKey(queue)
                ? _providersByQueue[queue]
                : _defaultProvider;
        }

        public IEnumerator<IPersistentJobQueueProvider> GetEnumerator()
        {
            return _providers.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}