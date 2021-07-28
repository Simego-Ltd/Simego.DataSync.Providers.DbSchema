using System.ComponentModel;

namespace Simego.DataSync.Providers.DbSchema
{
    class ConnectionProperties
    {
        private readonly DbSchemaDatasourceReader _reader;
        
        [Category("Settings")]
        public string ConnectionString { get { return _reader.ConnectionString; } set { _reader.ConnectionString = value; } }

        public ConnectionProperties(DbSchemaDatasourceReader reader)
        {
            _reader = reader;
        }        
    }
}
