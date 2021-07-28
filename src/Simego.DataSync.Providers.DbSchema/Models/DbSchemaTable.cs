using System.Collections.Generic;

namespace Simego.DataSync.Providers.DbSchema.Models
{
    public class DbSchemaTable
    {
        public string Schema { get; set; }
        public string Name { get; set; }
        public IList<DbSchemaTableColumn> Columns { get; set; } = new List<DbSchemaTableColumn>();
        public IList<DbSchemaTableColumnIndex> Indexes { get; set; } = new List<DbSchemaTableColumnIndex>();
    }
}
