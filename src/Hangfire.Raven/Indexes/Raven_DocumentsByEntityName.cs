using System;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

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
