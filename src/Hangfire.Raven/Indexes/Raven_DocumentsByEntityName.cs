using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hangfire.Raven.Indexes
{
    public class Raven_DocumentsByEntityName
        : AbstractIndexCreationTask
    {
        public override IndexDefinition CreateIndexDefinition()
        {
            return null;
        }

        public class Mapping
        {
            public string Tag { get; set; }
            public DateTime LastModified { get; set; }
        }
    }
}
