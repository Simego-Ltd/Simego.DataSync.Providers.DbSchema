using Simego.DataSync.Providers.DbSchema.Interfaces;
using Simego.DataSync.Providers.DbSchema.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Simego.DataSync.Providers.DbSchema
{
    class MySqlDbProvider : IDbSchemaProvider
    {
        private DbSchemaDatasourceReader Reader { get; set; }
        private Lazy<DbProviderFactory> Factory = new Lazy<DbProviderFactory>(() => MySql.Data.MySqlClient.MySqlClientFactory.Instance);

        public string Name => "MySql";

        public MySqlDbProvider(DbSchemaDatasourceReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader));
        }

        public DbConnection GetConnection()
        {
            var connection = Factory.Value.CreateConnection();
            connection.ConnectionString = Reader.ConnectionString;
            connection.Open();
            return connection;
        }

        public void Initialize(DbConnection connection)
        {

        }

        public void GetColumns(IDictionary<string, DbSchemaTable> tables)
        {
            using (var connection = GetConnection())
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = DbInformationSchemaConstants.Q_COLUMNS;
                    if (!string.IsNullOrEmpty(Reader.CommandWhere))
                    {
                        cmd.CommandText = $"SELECT * FROM ({cmd.CommandText}) T1 WHERE ({Reader.CommandWhere})";
                    }

                    using (var reader = cmd.ExecuteReader())
                    {

                        while (reader.Read())
                        {

                            var schemaName = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_TABLE_SCHEMA]);
                            var tableName = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_TABLE_NAME]);

                            var key = $"{schemaName}.{tableName}";

                            if (!tables.ContainsKey(key))
                                tables[key] = new DbSchemaTable { Schema = schemaName, Name = tableName };

                            var t = tables[key];

                            var dataType = ToDataType(reader);

                            // Length is bigger than an int for longtext.
                            var length = DataSchemaTypeConverter.ConvertTo<long?>(reader[DbInformationSchemaConstants.C_CHARACTER_LENGTH]) ?? -1;
                            if (length >= 16777215)
                                length = -1;

                            // Ensure UniqueIdentifiers are of length -1
                            if (dataType == DbSchemaColumnDataType.UniqueIdentifier)
                                length = -1;

                            t.Columns.Add(
                                new DbSchemaTableColumn
                                {
                                    Name = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_COLUMN_NAME]),
                                    Type = dataType,
                                    PrimaryKey = false,
                                    Identity = ToColumnIdentity(reader),
                                    Length = (int)length,
                                    Precision = DataSchemaTypeConverter.ConvertTo<int?>(reader[DbInformationSchemaConstants.C_PRECISION]) ?? 0,
                                    Scale = DataSchemaTypeConverter.ConvertTo<int?>(reader[DbInformationSchemaConstants.C_SCALE]) ?? 0,
                                    NotNull = !DataSchemaTypeConverter.ConvertTo<bool>(reader[DbInformationSchemaConstants.C_IS_NULLABLE]),
                                    Default = ToColumnDefault(reader)
                                });
                        }
                    }
                }
            }
        }

        public void GetIndexes(IDictionary<string, DbSchemaTable> tables)
        {
            using (var connection = GetConnection())
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "SELECT * FROM INFORMATION_SCHEMA.STATISTICS";
                    
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var schemaName = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_TABLE_SCHEMA]);
                            var tableName = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_TABLE_NAME]);

                            var columnName = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_COLUMN_NAME]);
                            var indexName = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_INDEX_NAME]);
                            var isUnique = !DataSchemaTypeConverter.ConvertTo<bool>(reader["NON_UNIQUE"]);
                            var isPrimaryKey = string.Equals(indexName, "PRIMARY", StringComparison.OrdinalIgnoreCase);
                            var nonClustred = string.Equals(indexName, "NONCLUSTERED", StringComparison.OrdinalIgnoreCase);

                            var key = $"{schemaName}.{tableName}";

                            if (tables.TryGetValue(key, out DbSchemaTable t))
                            {
                                var existingIndex = t.Indexes.FirstOrDefault(p => p.Name == indexName);
                                if (existingIndex != null)
                                {
                                    existingIndex.Columns.Add(columnName);
                                }
                                else
                                {
                                    var index = new DbSchemaTableColumnIndex
                                    {
                                        Name = indexName,
                                        PrimaryKey = isPrimaryKey,
                                        Unique = isUnique,
                                        Clustered = isPrimaryKey || !nonClustred,
                                        Type = isPrimaryKey || isUnique || nonClustred ? DbSchemaTableColumnIndexType.Constraint : DbSchemaTableColumnIndexType.Index,
                                        Columns = new List<string>(new[] { columnName })
                                    };

                                    t.Indexes.Add(index);
                                }                                
                            }

                        }
                    }
                }
            }            
        }        

        public string GenerateAddTableColumn(string schema, string table, DbSchemaTableColumn column)
        {
            return $"ALTER TABLE `{schema}`.`{table}` ADD COLUMN `{column.Name}` {ToSqlType(column)} {ToSqlNotNull(column)} {ToSqlDefault(column)};";
        }

        public string GenerateAlterColumnDefault(string schema, string table, DbSchemaTableColumn column)
        {
            if (column.Default == DbSchemaColumnDefault.None)
            {
                return $"ALTER TABLE `{schema}`.`{table}` ALTER COLUMN `{column.Name}` DROP DEFAULT";
            }

            return $"ALTER TABLE `{schema}`.`{table}` ALTER COLUMN `{column.Name}` SET {ToSqlDefault(column)};";
        }

        public string GenerateAlterIndex(string schema, string table, DbSchemaTableColumnIndex index, string name)
        {
            var sb = new StringBuilder();
            var columns = string.Join(",", index.Columns.Select(c => $"`{c}`"));

            //Drop Existing
            if (index.Type == DbSchemaTableColumnIndexType.Constraint)
            {
                sb.AppendLine($"ALTER TABLE `{schema}`.`{table}` DROP CONSTRAINT `{name}`;");
            }
            else
            {
                sb.AppendLine($"ALTER TABLE `{schema}`.`{table}` DROP INDEX `{name}`;");
            }
            
            if (index.Type == DbSchemaTableColumnIndexType.Constraint)
            {
                sb.Append($"ALTER TABLE `{schema}`.`{table}` ADD CONSTRAINT `{index.Name}` ");
                if (index.PrimaryKey)
                {
                    sb.Append($"PRIMARY KEY ");
                }
                else if (index.Unique)
                {
                    sb.Append("UNIQUE ");
                }
                sb.Append($"({columns});");
            }
            else
            {
                sb.Append("CREATE ");
                if (index.Unique)
                {
                    sb.Append("UNIQUE ");
                }
                sb.Append($"INDEX `{index.Name}` ON `{schema}`.`{table}` ({columns});");
            }

            return sb.ToString();
        }

        public string GenerateAlterTableColumn(string schema, string table, DbSchemaTableColumn column)
        {
            var sb = new StringBuilder();

            var nullOption = column.NotNull ? "NOT NULL" : "NULL";
            var defaultOption = column.Default == DbSchemaColumnDefault.None ? "" : $"{ToSqlDefault(column)}";

            sb.Append($"ALTER TABLE `{schema}`.`{table}` ");
            sb.AppendLine($"MODIFY COLUMN `{column.Name}` {ToSqlType(column)} {nullOption} {defaultOption}");

            return sb.ToString();
        }

        public string GenerateCreateIndex(string schema, string table, DbSchemaTableColumnIndex index)
        {
            var sb = new StringBuilder();
            var columns = string.Join(",", index.Columns.Select(c => $"`{c}`"));

            if (index.Type == DbSchemaTableColumnIndexType.Constraint)
            {
                sb.Append($"ALTER TABLE {CombineParts(schema, table)} ADD CONSTRAINT `{index.Name}` ");
                if (index.PrimaryKey)
                {
                    sb.Append($"PRIMARY KEY ");
                }
                else if (index.Unique)
                {
                    sb.Append("UNIQUE ");
                }
                sb.Append($"({columns});");
            }
            else
            {
                sb.Append("CREATE ");
                if (index.Unique)
                {
                    sb.Append("UNIQUE ");
                }
                sb.Append($"INDEX `{index.Name}` ON {CombineParts(schema, table)} ({columns});");
            }

            return sb.ToString();
        }

        public string GenerateDropIndex(string schema, string table, DbSchemaTableColumnIndex index, string name)
        {
            var sb = new StringBuilder();

            //Drop Existing
            if (index.Type == DbSchemaTableColumnIndexType.Constraint)
            {
                sb.AppendLine($"ALTER TABLE `{schema}`.`{table}` DROP CONSTRAINT `{name}`;");
            }
            else
            {
                sb.AppendLine($"ALTER TABLE `{schema}`.`{table}` DROP INDEX `{name}`;");
            }

            return sb.ToString();
        }

        public string GenerateDropTableColumn(string schema, string table, DbSchemaTableColumn column)
        {
            return $"ALTER TABLE `{schema}`.`{table}` DROP COLUMN `{column.Name}`";
        }

        public string GenerateCreateTableObjects(DbSchemaTable table)
        {
            var sb = new StringBuilder();
            if (table.Columns.Count > 0)
            {
                // Create Columns

                // Check if the Table already exists and if not create the table
                using (var connection = GetConnection())
                {
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = DbInformationSchemaConstants.Q_TABLE_COUNT;

                        cmd.Parameters.Add(CreateParameter(0, table.Schema));
                        cmd.Parameters.Add(CreateParameter(1, table.Name));

                        var tableCount = DataSchemaTypeConverter.ConvertTo<int>(cmd.ExecuteScalar());

                        if (tableCount == 0)
                        {
                            //We need to create the Table
                            sb.AppendLine($"CREATE TABLE `{table.Schema}`.`{table.Name}` (");

                            for (int i = 0; i < table.Columns.Count; i++)
                            {
                                DbSchemaTableColumn column = table.Columns[i];
                                if (i > 0) sb.AppendLine(",");
                                sb.Append($"\t`{column.Name}` {ToSqlType(column)} {ToSqlNotNull(column)} {ToAutoIncrement(column)} {ToSqlDefault(column)}");
                            }
                            sb.AppendLine(");");
                        }
                        else
                        {
                            //We need to Add Columns to the Table
                            sb.AppendLine($"ALTER TABLE `{table.Schema}`.`{table.Name}` ");

                            for (int i = 0; i < table.Columns.Count; i++)
                            {
                                DbSchemaTableColumn column = table.Columns[i];
                                if (i > 0) sb.AppendLine(",");
                                sb.Append($"\tADD COLUMN `{column.Name}` {ToSqlType(column)} {ToSqlNotNull(column)} {ToSqlDefault(column)}");
                            }
                            sb.AppendLine(";");
                        }
                    }
                }

            }
            if (table.Indexes.Count > 0)
            {
                // Add Indexes
                foreach (var index in table.Indexes)
                {
                    // Do not add Primary Key index when table has identity.
                    if (table.Columns.Any(p => p.Identity) && index.PrimaryKey)
                        continue;

                    //Add to the script the Create Index
                    sb.AppendLine(GenerateCreateIndex(table.Schema, table.Name, index));
                }
            }

            return sb.ToString();
        }

        public string GenerateCreateTableObject(DbSchemaTable table)
        {
            var sb = new StringBuilder();
            if (table.Columns.Count > 0)
            {
                //We need to create the Table
                sb.AppendLine($"CREATE TABLE {CombineParts(table.Schema, table.Name)}");
                sb.AppendLine("(");
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    DbSchemaTableColumn column = table.Columns[i];
                    if (i > 0) sb.AppendLine(",");
                    var col = $"`{column.Name}` {ToSqlType(column)} {ToSqlNotNull(column)} {ToAutoIncrement(column)} {ToSqlDefault(column)}".Trim();
                    sb.Append($"\t{col}");
                }
                sb.AppendLine();
                sb.AppendLine(");");

                if (table.Indexes.Count > 0)
                {
                    // Add Indexes
                    foreach (var index in table.Indexes)
                    {
                        // Do not add Primary Key index when table has identity.
                        if (table.Columns.Any(p => p.Identity) && index.PrimaryKey)
                            continue;

                        //Add to the script the Create Index
                        sb.AppendLine(GenerateCreateIndex(table.Schema, table.Name, index));
                    }
                }
            }

            return sb.ToString();
        }

        public string GenerateDeleteTableObjects(DbSchemaTable table)
        {
            var sb = new StringBuilder();
            if (table.Columns.Count > 0)
            {
                // Drop Columns

                // Check if the number of columns to drop match the number of table columns then drop the table.
                using (var connection = GetConnection())
                {
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = DbInformationSchemaConstants.Q_COLUMNS_COUNT;

                        cmd.Parameters.Add(CreateParameter(0, table.Schema));
                        cmd.Parameters.Add(CreateParameter(1, table.Name));

                        var tableColumnsCount = DataSchemaTypeConverter.ConvertTo<int>(cmd.ExecuteScalar());

                        if (table.Columns.Count == tableColumnsCount)
                        {
                            //We just drop the table and return at this point.
                            sb.AppendLine($"DROP TABLE `{table.Schema}`.`{table.Name}`;");
                            return sb.ToString();
                        }
                        else
                        {
                            sb.AppendLine($"ALTER TABLE `{table.Schema}`.`{table.Name}` ");
                            for (int i = 0; i < table.Columns.Count; i++)
                            {
                                if (i > 0) sb.AppendLine(",");
                                sb.Append($"\tDROP COLUMN `{table.Columns[i].Name}`");
                            }
                            sb.Append(";");
                        }
                    }
                }

            }
            if (table.Indexes.Count > 0)
            {
                // Drop Indexes
                foreach (var index in table.Indexes)
                {
                    //Ignore dropping Primary keys as this just seems wrong.
                    if (index.PrimaryKey) continue;
                    //Add to the script the Drop Index
                    sb.AppendLine(GenerateDropIndex(table.Schema, table.Name, index, index.Name));
                }
            }

            return sb.ToString();
        }

        private DbSchemaColumnDefault ToColumnDefault(DbDataReader reader)
        {
            var defaultString = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_COLUMN_DEFAULT]);
            if (!string.IsNullOrEmpty(defaultString))
            {
                if (defaultString.Equals("current_timestamp()", StringComparison.OrdinalIgnoreCase) ||
                    defaultString.Equals("current_timestamp", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.CurrentDateTime;
                }
                if (defaultString.Equals("uuid()", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.NewUniqueIdentifier;
                }
                if (defaultString.Equals("0", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.Zero;
                }
                if (defaultString.Equals("1", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.One;
                }
                if (defaultString.Equals("2", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.Two;
                }
                if (defaultString.Equals("3", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.Three;
                }
                if (defaultString.Equals("b'0'", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.Zero;
                }
                if (defaultString.Equals("b'1'", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.One;
                }
                if(int.TryParse(defaultString, ConversionNumberStyles.Integer, CultureInfo.InvariantCulture, out var val))
                {
                    switch (val)
                    {
                        case 0: return DbSchemaColumnDefault.Zero;
                        case 1: return DbSchemaColumnDefault.One;
                        case 2: return DbSchemaColumnDefault.Two;
                        case 3: return DbSchemaColumnDefault.Three;
                    }
                }
            }
            return DbSchemaColumnDefault.None;
        }

        private bool ToColumnIdentity(DbDataReader reader)
        {
            var defaultString = DataSchemaTypeConverter.ConvertTo<string>(reader["Extra"]);
            if (!string.IsNullOrEmpty(defaultString))
            {
                if (defaultString.StartsWith("auto_increment", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            return false;
        }
        private DbSchemaColumnDataType ToDataType(DbDataReader reader)
        {
            var dataTypeString = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_DATA_TYPE]);
            var dataTypeLength = DataSchemaTypeConverter.ConvertTo<long?>(reader[DbInformationSchemaConstants.C_CHARACTER_LENGTH]) ?? -1;
            switch (dataTypeString)
            {
                case "bigint": return DbSchemaColumnDataType.BigInteger;
                case "int": return DbSchemaColumnDataType.Integer;
                case "tinyint": return DbSchemaColumnDataType.TinyInt;

                case "real":
                case "float": return DbSchemaColumnDataType.Double;

                case "numeric":
                case "decimal": return DbSchemaColumnDataType.Decimal;
                                
                case "bit": return DbSchemaColumnDataType.Boolean;


                case "timestamp":
                case "datetime": return DbSchemaColumnDataType.DateTime;
                
                case "longblob":
                case "mediumblob": return DbSchemaColumnDataType.Blob;
                
                case "varchar":
                    {
                        return DbSchemaColumnDataType.VarString;
                    }
                case "char":
                    {
                        return dataTypeLength == 38 ? DbSchemaColumnDataType.UniqueIdentifier : DbSchemaColumnDataType.VarString;
                    }
                case "longtext":
                case "mediumtext":
                case "text":
                    {
                        return DbSchemaColumnDataType.Text;
                    }
                case "binary":
                    {
                        return dataTypeLength == 16 ? DbSchemaColumnDataType.UniqueIdentifier : DbSchemaColumnDataType.Blob;
                    }
                default:
                    {
                        return DbSchemaColumnDataType.Text;
                    }
            }
        }

        private DbParameter CreateParameter(int index, string value)
        {
            var p = Factory.Value.CreateParameter();
            p.ParameterName = $"@p{index}";
            p.DbType = DbType.String;
            p.Value = value;
            return p;
        }

        private string ToSqlType(DbSchemaTableColumn column)
        {
            switch (column.Type)
            {
                case DbSchemaColumnDataType.BigInteger: return "bigint";
                case DbSchemaColumnDataType.Integer: return "int";
                case DbSchemaColumnDataType.Double: return "float";
                case DbSchemaColumnDataType.Decimal: return $"decimal({column.Precision}, {column.Scale})";
                case DbSchemaColumnDataType.Boolean: return "bit";
                case DbSchemaColumnDataType.DateTime: return "datetime";
                case DbSchemaColumnDataType.UniqueIdentifier: return "char(38)";
                case DbSchemaColumnDataType.VarString: return $"varchar({column.Length})";
                case DbSchemaColumnDataType.Text: return "mediumtext";
                case DbSchemaColumnDataType.Blob: return "mediumblob";
                case DbSchemaColumnDataType.TinyInt: return "tinyint";
            }

            throw new ArgumentOutOfRangeException(nameof(column), $"Invalid Sql Type: {column}");
        }

        private string ToSqlDefault(DbSchemaTableColumn column)
        {
            switch (column.Default)
            {
                case DbSchemaColumnDefault.CurrentDateTime: return "DEFAULT CURRENT_TIMESTAMP()";
                case DbSchemaColumnDefault.NewUniqueIdentifier: return "DEFAULT (UUID())";
                case DbSchemaColumnDefault.Zero: return "DEFAULT 0";
                case DbSchemaColumnDefault.One: return "DEFAULT 1";
                case DbSchemaColumnDefault.Two: return "DEFAULT 2";
                case DbSchemaColumnDefault.Three: return "DEFAULT 3";
                default: return string.Empty;
            }
        }

        private string ToSqlNotNull(DbSchemaTableColumn column) => column.NotNull ? "NOT NULL" : "NULL";

        private string ToAutoIncrement(DbSchemaTableColumn column) => column.Identity ? "auto_increment primary key" : "";

        private static string CombineParts(string schema, string table)
        {
            if (string.IsNullOrEmpty(schema)) return $"`{table}`";
            return $"`{schema}`.`{table}`";
        }
    }
}

