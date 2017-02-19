namespace Hangfire.Raven.Entities
{
    public class DistributedLock
    {
        public string Id {
            get {
                return string.Format("DistributedLocks/{0}", Resource);
            }
            set {

            }
        }

        public string Resource { get; set; }
        public string ClientId { get; set; }
    }
}
