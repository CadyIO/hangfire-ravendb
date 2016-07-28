using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Raven
{
    public class BlockingQueue<T>
    {
        Queue<T> _queue = new Queue<T>();
        SemaphoreSlim _sem = new SemaphoreSlim(0, Int32.MaxValue);

        public void Enqueue(T item)
        {
            lock (_queue)
            {
                _queue.Enqueue(item);
            }

            _sem.Release();
        }

        public T Dequeue(CancellationToken token)
        {
            _sem.Wait(token);

            lock (_queue)
            {
                return _queue.Dequeue();
            }
        }
    }
}
