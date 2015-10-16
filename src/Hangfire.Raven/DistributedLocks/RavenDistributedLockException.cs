using System;

namespace Hangfire.Raven.DistributedLock
{
    /// <summary>
    /// Represents exceptions for distributed lock implementation for MongoDB
    /// </summary>
    [Serializable]
    public class RavenDistributedLockException : Exception
    {
        /// <summary>
        /// Creates exception
        /// </summary>
        /// <param name="message">Exception message</param>
        public RavenDistributedLockException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Creates exception with inner exception
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner exception</param>
        public RavenDistributedLockException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}