using System.Collections.Generic;

namespace Hangfire.Raven.Entities
{
    public class RavenHash
    {
        public RavenHash()
        {
            this.Fields = new Dictionary<string, string>();
        }

        public string Id { get; set; }
        public Dictionary<string, string> Fields { get; set; }
    }
}
