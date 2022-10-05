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
        
        public string GetName(HashHelper h, DbSchemaTable t, string indexNameFormat)
        {
            // Allow for no template when IndexNameFormat is blank
            if (string.IsNullOrWhiteSpace(indexNameFormat)) return Name;
            // Create a name for the Index using the IndexNameFormat template string.
            var hash = h.GetHashAsString(DataSchemaTypeConverter.ConvertTo<string>(Columns.ToArray())).Substring(0, 6);
            var name = indexNameFormat.Replace("Schema", t.Schema).Replace("Name", t.Name);
            return $"{(PrimaryKey ? "PK" : "IX")}_{name}_{hash}";
        }
    }
}
