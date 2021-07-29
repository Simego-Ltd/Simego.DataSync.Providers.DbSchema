namespace Simego.DataSync.Providers.DbSchema
{    
    class DbInformationSchemaConstants
    {
        public const string Q_TABLE_COUNT = "select count(*) from information_schema.tables WHERE table_schema = @p0 AND table_name = @p1";
        public const string Q_COLUMNS_COUNT = "select count(*) from information_schema.columns WHERE table_schema = @p0 AND table_name = @p1";
        public const string Q_COLUMNS = "select * from information_schema.columns";
        public const string Q_COLUMNS_SQLSERVER = "select *, columnproperty(object_id('[' + table_schema + '].[' + table_name + ']'), [column_name], 'IsIdentity') AS is_identity from information_schema.columns";

        public const string C_TABLE_SCHEMA = "table_schema";
        public const string C_TABLE_NAME = "table_name";
        public const string C_COLUMN_NAME = "column_name";
        public const string C_COLUMN_DEFAULT = "column_default";
        public const string C_DATA_TYPE = "data_type";
        public const string C_IS_NULLABLE = "is_nullable";
        public const string C_CHARACTER_LENGTH = "character_maximum_length";
        public const string C_IS_IDENTITY = "is_identity";

    }
}
