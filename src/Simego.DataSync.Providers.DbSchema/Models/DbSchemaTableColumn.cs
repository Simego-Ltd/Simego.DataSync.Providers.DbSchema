using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Simego.DataSync.Providers.DbSchema.Models
{

    public class DbSchemaTableColumn
    {
        public string Name { get; set; }
        public DbSchemaColumnDataType Type { get; set; }
        public bool Identity { get; set; }
        public bool PrimaryKey { get; set; }
        public bool NotNull { get; set; }
        public int Length { get; set; } = -1;
        public DbSchemaColumnDefault Default { get; set; }
    }
}
