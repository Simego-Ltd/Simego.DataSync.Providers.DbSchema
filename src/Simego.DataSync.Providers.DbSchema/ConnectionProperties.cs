using Simego.DataSync.Providers.DbSchema.TypeConverters;
using System.ComponentModel;

namespace Simego.DataSync.Providers.DbSchema
{
    class ConnectionProperties
    {
        private readonly DbSchemaDatasourceReader _reader;

        [Category("Connection")]
        [TypeConverter(typeof(DbProviderTypeConverter))]
        public string DbProvider { get { return _reader.DbProvider.Name; } set { _reader.SetProvider(value); } }
        
        [Category("Connection")]
        public string ConnectionString { get { return _reader.ConnectionString; } set { _reader.ConnectionString = value; } }

        [Category("Connection")]
        public string CommandWhere { get { return _reader.CommandWhere; } set { _reader.CommandWhere = value; } }

        [Category("Settings")]
        public bool OutputSqlTrace { get { return _reader.OutputSqlTrace; } set { _reader.OutputSqlTrace = value; } }

        [Category("Settings")]
        public bool DoNotExecute { get { return _reader.DoNotExecute; } set { _reader.DoNotExecute = value; } }

        public ConnectionProperties(DbSchemaDatasourceReader reader)
        {
            _reader = reader;
            DbProvider = _reader.DbProvider.Name;
        }        
    }
}
