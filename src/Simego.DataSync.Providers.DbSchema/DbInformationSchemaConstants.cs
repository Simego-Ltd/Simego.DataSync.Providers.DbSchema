namespace Simego.DataSync.Providers.DbSchema
{    
    class DbInformationSchemaConstants
    {
        public const string Q_TABLE_COUNT = "select count(*) from information_schema.tables WHERE table_schema = @p0 AND table_name = @p1";
        public const string Q_COLUMNS_COUNT = "select count(*) from information_schema.columns WHERE table_schema = @p0 AND table_name = @p1";
        public const string Q_COLUMNS = "select columns.* from information_schema.columns left join information_schema.views ON views.table_catalog = columns.table_catalog AND views.table_schema = columns.table_schema AND views.table_name = columns.table_name where views.table_name IS NULL";
        public const string Q_COLUMNS_SQLSERVER = "select columns.*, columnproperty(object_id('[' + columns.table_schema + '].[' + columns.table_name + ']'), [column_name], 'IsIdentity') AS is_identity from information_schema.columns left join information_schema.views ON views.table_catalog = columns.table_catalog AND views.table_schema = columns.table_schema AND views.table_name = columns.table_name where views.table_name IS NULL";
        
        public const string C_TABLE_SCHEMA = "table_schema";
        public const string C_TABLE_NAME = "table_name";
        public const string C_COLUMN_NAME = "column_name";
        public const string C_INDEX_NAME = "index_name";
        public const string C_COLUMN_DEFAULT = "column_default";
        public const string C_DATA_TYPE = "data_type";
        public const string C_IS_NULLABLE = "is_nullable";
        public const string C_CHARACTER_LENGTH = "character_maximum_length";
        public const string C_IS_IDENTITY = "is_identity";
        public const string C_PRECISION = "numeric_precision";
        public const string C_SCALE = "numeric_scale";

    }
}
