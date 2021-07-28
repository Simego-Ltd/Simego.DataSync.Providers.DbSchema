using Simego.DataSync.Helpers;
using System.Collections.Generic;
using System.Linq;

namespace Simego.DataSync.Providers.DbSchema.Models
{
    public class DbSchemaTableColumnIndex
    {
        public string Name { get; set; }
        public IList<string> Columns { get; set; } = new List<string>();
        public IList<string> Include { get; set; } = new List<string>();
        public bool PrimaryKey { get; set; }
        public bool Clustered { get; set; }
        public bool Unique { get; set; }
        public DbSchemaTableColumnIndexType Type { get; set; } = DbSchemaTableColumnIndexType.Index;    
        
        public string GetName(HashHelper h, DbSchemaTable t)
        {
            var hash = h.GetHashAsString(DataSchemaTypeConverter.ConvertTo<string>(Columns.ToArray())).Substring(0, 6);
            return $"{(PrimaryKey ? "PK" : "IX")}_{t.Schema}_{t.Name}_{hash}";
        }
    }
}
