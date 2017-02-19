using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hangfire.Raven.Entities
{
    public class RavenJobQueueConfig
    {
        public RavenJobQueueConfig()
        {
            this.Subscriptions = new Dictionary<string, long>();
        }

        public string Id {
            get
            {
                return "Config/RavenJobQueue";
            }
        }

        public Dictionary<string, long> Subscriptions { get; set; }
    }
}
