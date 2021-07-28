using Simego.DataSync.Providers.DbSchema.Interfaces;
using Simego.DataSync.Providers.DbSchema.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Simego.DataSync.Providers.DbSchema
{
    class PostgreSqlDbProvider : IDbSchemaProvider
    {
        private DbSchemaDatasourceReader Reader { get; set; }
        private DbProviderFactory Factory = DbProviderFactories.GetFactory("Npgsql");

        public PostgreSqlDbProvider(DbSchemaDatasourceReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader));
        }

        public DbConnection GetConnection()
        {
            var connection = Factory.CreateConnection();
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
                    cmd.CommandText = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS";
                    if (!string.IsNullOrEmpty(Reader.CommandWhere))
                    {
                        cmd.CommandText = $"{cmd.CommandText} WHERE {Reader.CommandWhere}";
                    }

                    var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

                    while (reader.Read())
                    {
                        var schemaName = DataSchemaTypeConverter.ConvertTo<string>(reader["TABLE_SCHEMA"]);
                        var tableName = DataSchemaTypeConverter.ConvertTo<string>(reader["TABLE_NAME"]);

                        var key = $"{schemaName}.{tableName}";

                        if (!tables.ContainsKey(key))
                            tables[key] = new DbSchemaTable { Schema = schemaName, Name = tableName };

                        var t = tables[key];

                        t.Columns.Add(
                            new DbSchemaTableColumn
                            {
                                Name = DataSchemaTypeConverter.ConvertTo<string>(reader["COLUMN_NAME"]),
                                Type = ToDataType(reader),
                                PrimaryKey = false,
                                Identity = false,
                                Length = DataSchemaTypeConverter.ConvertTo<int?>(reader["CHARACTER_MAXIMUM_LENGTH"]) ?? -1,
                                NotNull = !DataSchemaTypeConverter.ConvertTo<bool>(reader["IS_NULLABLE"]),
                                Default = ToColumnDefault(reader)
                            });
                    }
                }
            }
        }
    
        public void GetIndexes(IDictionary<string, DbSchemaTable> tables)
        {
            //select * from "information_schema"."key_column_usage"

            var sb = new StringBuilder();

            sb.AppendLine("SELECT s.*, t.*, c.constraint_type FROM pg_indexes s ");
            sb.AppendLine("INNER JOIN (");
            sb.AppendLine("SELECT  i.relname AS index_name, indisunique AS is_unique, indisprimary AS is_primary, indisclustered AS is_clustered ");
            sb.AppendLine("FROM pg_class c JOIN pg_index x ON c.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid ");
            sb.AppendLine("LEFT JOIN pg_namespace n ON n.oid = c.relnamespace ");
            sb.AppendLine("WHERE c.relkind = ANY(ARRAY['r', 't']) AND i.relname NOT LIKE 'pg%'");
            sb.AppendLine(") t ON s.indexname = t.index_name ");
            sb.AppendLine("LEFT JOIN information_schema.table_constraints c ON c.constraint_name = t.index_name");
            sb.AppendLine("WHERE t.index_name IS NOT null");

            using (var connection = GetConnection())
            {                
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = sb.ToString();

                    var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

                    while (reader.Read())
                    {
                        var schemaName = DataSchemaTypeConverter.ConvertTo<string>(reader["schemaname"]);
                        var tableName = DataSchemaTypeConverter.ConvertTo<string>(reader["tablename"]);

                        var key = $"{schemaName}.{tableName}";

                        if (tables.TryGetValue(key, out DbSchemaTable t))
                        {
                            var indexName = DataSchemaTypeConverter.ConvertTo<string>(reader["indexname"]);

                            var index =
                                new DbSchemaTableColumnIndex
                                {
                                    Name = DataSchemaTypeConverter.ConvertTo<string>(indexName),
                                    PrimaryKey = DataSchemaTypeConverter.ConvertTo<bool>(reader["is_primary"]),
                                    Unique = DataSchemaTypeConverter.ConvertTo<bool>(reader["is_unique"]),
                                    Clustered = DataSchemaTypeConverter.ConvertTo<bool>(reader["is_clustered"])
                                };

                            var constraintType = DataSchemaTypeConverter.ConvertTo<string>(reader["constraint_type"]);
                            index.Type = constraintType != null ? DbSchemaTableColumnIndexType.Constraint : DbSchemaTableColumnIndexType.Index;                           
                            index.Columns = ParseIndexDefColumns(DataSchemaTypeConverter.ConvertTo<string>(reader["indexdef"])).ToList();

                            t.Indexes.Add(index);
                        }
                    }
                }
            }
        }

        private IEnumerable<string> ParseIndexDefColumns(string value)
        {
            int indexOfStart = value.IndexOf("(");
            int indexOfEnd = value.IndexOf(")");

            if(indexOfEnd > indexOfStart)
            {
                var columns = value.Substring(indexOfStart + 1, indexOfEnd - indexOfStart- 1);
                foreach(var s in columns.Split(','))
                {
                    yield return s.Trim();
                }
            }
        }

        public string GenerateAddTableColumn(string schema, string table, DbSchemaTableColumn column)
        {
            var sb = new StringBuilder();

            sb.Append($"ALTER TABLE \"{schema.ToLower()}\".\"{table.ToLower()}\" ADD {column.Name.ToLower()} ");

            switch (column.Type)
            {
                case DbSchemaColumnDataType.BigInteger:
                    {
                        sb.Append(column.Identity ? "bigserial " : "bigint ");
                        break;
                    }
                case DbSchemaColumnDataType.Integer:
                    {
                        sb.Append(column.Identity ? "serial " : "int ");
                        break;
                    }
                case DbSchemaColumnDataType.Boolean:
                    {
                        sb.Append("int ");
                        break;
                    }
                case DbSchemaColumnDataType.DateTime:
                    {
                        sb.Append("timestamp ");
                        break;
                    }
                case DbSchemaColumnDataType.UniqueIdentifier:
                    {
                        sb.Append("uuid ");
                        break;
                    }
                case DbSchemaColumnDataType.VarString:
                    {
                        sb.Append($"varchar({column.Length}) ");
                        break;
                    }
                case DbSchemaColumnDataType.Text:
                    {
                        sb.Append("text ");
                        break;
                    }
                case DbSchemaColumnDataType.Blob:
                    {
                        sb.Append("bytea ");
                        break;
                    }
            }


            switch (column.Default)
            {
                case DbSchemaColumnDefault.CurrentDateTime:
                    {
                        sb.Append("DEFAULT(CURRENT_TIMESTAMP) ");
                        break;
                    }
                //case DbSchemaColumnDefault.NewUniqueIdentifier:
                //    {
                //        sb.Append("DEFAULT(UUID_TO_BIN(UUID(), true)) ");
                //        break;
                //    }
                case DbSchemaColumnDefault.Zero:
                    {
                        sb.Append("DEFAULT(0) ");
                        break;
                    }
                case DbSchemaColumnDefault.One:
                    {
                        sb.Append("DEFAULT(1) ");
                        break;
                    }
                case DbSchemaColumnDefault.Two:
                    {
                        sb.Append("DEFAULT(2) ");
                        break;
                    }
                case DbSchemaColumnDefault.Three:
                    {
                        sb.Append("DEFAULT(3) ");
                        break;
                    }
            }

            sb.Append(column.NotNull ? "NOT NULL" : "NULL");

            return sb.ToString();
        }

        public string GenerateAlterColumnDefault(string schema, string table, DbSchemaTableColumn column)
        {
            var sb = new StringBuilder();
            //ALTER TABLE ONLY users ALTER COLUMN lang SET DEFAULT 'en_GB';

            sb.Append($"ALTER TABLE ONLY \"{schema.ToLower()}\".\"{table.ToLower()}\" ALTER COLUMN {column.Name.ToLower()} SET ");

            
            switch (column.Default)
            {
                case DbSchemaColumnDefault.None:
                    {
                        sb.Append("DEFAULT(NULL) ");
                        break;
                    }
                case DbSchemaColumnDefault.CurrentDateTime:
                    {
                        sb.Append("DEFAULT(CURRENT_TIMESTAMP) ");
                        break;
                    }
                //case DbSchemaColumnDefault.NewUniqueIdentifier:
                //    {
                //        sb.Append("DEFAULT(UUID_TO_BIN(UUID(), true)) ");
                //        break;
                //    }
                case DbSchemaColumnDefault.Zero:
                    {
                        sb.Append("DEFAULT(0) ");
                        break;
                    }
                case DbSchemaColumnDefault.One:
                    {
                        sb.Append("DEFAULT(1) ");
                        break;
                    }
                case DbSchemaColumnDefault.Two:
                    {
                        sb.Append("DEFAULT(2) ");
                        break;
                    }
                case DbSchemaColumnDefault.Three:
                    {
                        sb.Append("DEFAULT(3) ");
                        break;
                    }
            }
            
            return sb.ToString();
        }

        public string GenerateAlterIndex(string schema, string table, DbSchemaTableColumnIndex index, string name)
        {
            var sb = new StringBuilder();
            var columns = string.Join(",", index.Columns.Select(c => $"{c.ToLower()}"));

            //Drop Existing
            sb.AppendLine($"ALTER TABLE \"{schema.ToLower()}\".\"{table.ToLower()}\" DROP CONSTRAINT IF EXISTS \"{name}\";");
            sb.AppendLine($"DROP INDEX IF EXISTS \"{schema.ToLower()}\".\"{name}\";");
            
            if (index.Type == DbSchemaTableColumnIndexType.Constraint)
            {
                sb.Append($"ALTER TABLE \"{schema.ToLower()}\".\"{table.ToLower()}\" ADD CONSTRAINT \"{index.Name}\" ");
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
                sb.Append($"INDEX \"{index.Name}\" ON \"{schema.ToLower()}\".\"{table.ToLower()}\" ({columns});");
            }

            return sb.ToString();
        }

        public string GenerateAlterTableColumn(string schema, string table, DbSchemaTableColumn column)
        {
            var sb = new StringBuilder();

            sb.Append($"ALTER TABLE ONLY \"{schema.ToLower()}\".\"{table.ToLower()}\" ");

            sb.Append($"ALTER COLUMN {column.Name.ToLower()} TYPE ");
            switch (column.Type)
            {
                case DbSchemaColumnDataType.BigInteger:
                    {
                        sb.Append(column.Identity ? "bigserial " : "bigint ");
                        break;
                    }
                case DbSchemaColumnDataType.Integer:
                    {
                        sb.Append(column.Identity ? "serial " : "int ");
                        break;
                    }
                case DbSchemaColumnDataType.Boolean:
                    {
                        sb.Append("int ");
                        break;
                    }
                case DbSchemaColumnDataType.DateTime:
                    {
                        sb.Append("timestamp ");
                        break;
                    }
                case DbSchemaColumnDataType.UniqueIdentifier:
                    {
                        sb.Append("uuid ");
                        break;
                    }
                case DbSchemaColumnDataType.VarString:
                    {
                        sb.Append($"varchar({column.Length}) ");
                        break;
                    }
                case DbSchemaColumnDataType.Text:
                    {
                        sb.Append("text ");
                        break;
                    }
                case DbSchemaColumnDataType.Blob:
                    {
                        sb.Append("bytea ");
                        break;
                    }
            }

            sb.Append(", ");
            sb.Append($"ALTER COLUMN {column.Name.ToLower()} ");

            switch (column.Default)
            {
                case DbSchemaColumnDefault.None:
                    {
                        sb.Append("DROP DEFAULT ");
                        break;
                    }
                case DbSchemaColumnDefault.CurrentDateTime:
                    {
                        sb.Append("SET DEFAULT(CURRENT_TIMESTAMP) ");
                        break;
                    }
                //case DbSchemaColumnDefault.NewUniqueIdentifier:
                //    {
                //        sb.Append("DEFAULT(UUID_TO_BIN(UUID(), true)) ");
                //        break;
                //    }
                case DbSchemaColumnDefault.Zero:
                    {
                        sb.Append("SET DEFAULT(0) ");
                        break;
                    }
                case DbSchemaColumnDefault.One:
                    {
                        sb.Append("SET DEFAULT(1) ");
                        break;
                    }
                case DbSchemaColumnDefault.Two:
                    {
                        sb.Append("SET DEFAULT(2) ");
                        break;
                    }
                case DbSchemaColumnDefault.Three:
                    {
                        sb.Append("SET DEFAULT(3) ");
                        break;
                    }
            }

            sb.Append(", ");
            sb.Append($"ALTER COLUMN {column.Name.ToLower()} ");
            sb.Append(column.NotNull ? "SET NOT NULL" : "DROP NOT NULL");

            return sb.ToString();
        }

        public string GenerateCreateIndex(string schema, string table, DbSchemaTableColumnIndex index)
        {
            var sb = new StringBuilder();
            var columns = string.Join(",", index.Columns.Select(c => $"{c.ToLower()}"));
            
            if (index.Type == DbSchemaTableColumnIndexType.Constraint)
            {
                sb.Append($"ALTER TABLE \"{schema.ToLower()}\".\"{table.ToLower()}\" ADD CONSTRAINT \"{index.Name}\" ");
                if (index.PrimaryKey)
                {
                    sb.Append($"PRIMARY KEY ");
                }
                else if (index.Unique)
                {
                    sb.Append("UNIQUE ");
                }
                sb.Append($"({columns})");
            }
            else
            {
                sb.Append("CREATE ");
                if (index.Unique)
                {
                    sb.Append("UNIQUE ");
                }
                sb.Append($"INDEX \"{index.Name.ToLower()}\" ON \"{schema.ToLower()}\".\"{table.ToLower()}\" ({columns}) ");                
            }

            return sb.ToString();
        }

        public string GenerateDropIndex(string schema, string table, DbSchemaTableColumnIndex index, string name)
        {
            var sb = new StringBuilder();

            //Drop Existing
            sb.AppendLine($"ALTER TABLE \"{schema.ToLower()}\".\"{table.ToLower()}\" DROP CONSTRAINT IF EXISTS \"{name}\";");
            sb.AppendLine($"DROP INDEX IF EXISTS \"{schema.ToLower()}\".\"{name}\";");

            return sb.ToString();
        }

        public string GenerateDropTableColumn(string schema, string table, DbSchemaTableColumn column)
        {
            return $"ALTER TABLE \"{schema.ToLower()}\".\"{table.ToLower()}\" DROP COLUMN \"{column.Name}\"";
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
                        cmd.CommandText = "select count(*) from information_schema.columns WHERE table_schema = @p0 AND table_name = @p1";

                        cmd.Parameters.Add(CreateParameter(0, table.Schema.ToLower()));
                        cmd.Parameters.Add(CreateParameter(1, table.Name.ToLower()));

                        var tableColumnsCount = DataSchemaTypeConverter.ConvertTo<int>(cmd.ExecuteScalar());
                        
                        if (table.Columns.Count == tableColumnsCount)
                        {
                            //We just drop the table and return at this point.
                            sb.AppendLine($"DROP TABLE \"{table.Schema.ToLower()}\".\"{table.Name.ToLower()}\";");
                            return sb.ToString();
                        }
                        else
                        {
                            sb.AppendLine($"ALTER TABLE \"{table.Schema.ToLower()}\".\"{table.Name.ToLower()}\" ");
                            for (int i = 0; i < table.Columns.Count; i++)
                            {
                                if (i > 0) sb.AppendLine(",");
                                sb.Append($"\tDROP COLUMN \"{table.Columns[i].Name}\"");                                                                
                            }
                            sb.Append(";");
                        }
                    }
                }

            }
            if(table.Indexes.Count > 0)
            {
                // Drop Indexes
                foreach(var index in table.Indexes)
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
            var defaultString = DataSchemaTypeConverter.ConvertTo<string>(reader["COLUMN_DEFAULT"]);
            if (!string.IsNullOrEmpty(defaultString))
            {
                if (defaultString.Equals("CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.CurrentDateTime;
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
            }
            return DbSchemaColumnDefault.None;
        }
        private DbSchemaColumnDataType ToDataType(DbDataReader reader)
        {
            var dataTypeString = DataSchemaTypeConverter.ConvertTo<string>(reader["DATA_TYPE"]);
            switch (dataTypeString)
            {
                case "bigint": return DbSchemaColumnDataType.BigInteger;
                case "int":
                case "integer": return DbSchemaColumnDataType.Integer;
                case "uuid": return DbSchemaColumnDataType.UniqueIdentifier;
                case "bit": return DbSchemaColumnDataType.Boolean;
                case "timestamp":
                case "timestamp without time zone": return DbSchemaColumnDataType.DateTime;
                case "bytea": return DbSchemaColumnDataType.Blob;
                case "varchar":
                case "character varying":
                    {                        
                        return DbSchemaColumnDataType.VarString;
                    }
                case "text":
                    {                   
                        return DbSchemaColumnDataType.Text;
                    }
                default:
                    {
                        return DbSchemaColumnDataType.Text;
                    }
            }
        }

        private DbParameter CreateParameter(int index, string value)
        {
            var p = Factory.CreateParameter();
            p.ParameterName = $"@p{index}";
            p.DbType = DbType.String;
            p.Value = value;
            return p;
        }
    }
}
