using System.Collections.Generic;

namespace Hangfire.Raven.Entities
{
    public class RavenList
    {
        public RavenList()
        {
            Values = new List<string>();
        }

        public string Id { get; set; }
        public List<string> Values { get; set; }
    }
}
