using Hangfire.Raven.Entities.Identity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hangfire.Raven.Entities
{
    public class Counter
    {
        public string Id { get; set; }

        public int Value { get; set; }

        public DateTime? ExpireAt { get; set; }
    }
}
