using Simego.DataSync.Providers.DbSchema.Interfaces;
using Simego.DataSync.Providers.DbSchema.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Simego.DataSync.Providers.DbSchema
{
    class SqlClientDbProvider : IDbSchemaProvider
    {
        private DbSchemaDatasourceReader Reader { get; set; }
        private IDictionary<string, string> ColumnDefaultNames { get; set; } = new Dictionary<string, string>();

        public SqlClientDbProvider(DbSchemaDatasourceReader reader)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader));
        }

        public DbConnection GetConnection()
        {
            var connection = new SqlConnection(Reader.ConnectionString);
            connection.Open();
            return connection;
        }

        public void Initialize(DbConnection connection)
        {
            var sb = new StringBuilder();

            sb.AppendLine("select con.[name] as constraint_name, schema_name(t.schema_id) AS [schema], t.[name] as [table], col.[name] as column_name");
            sb.AppendLine("from sys.default_constraints con ");
            sb.AppendLine("left outer join sys.objects t on con.parent_object_id = t.object_id ");
            sb.AppendLine("left outer join sys.all_columns col on con.parent_column_id = col.column_id and con.parent_object_id = col.object_id");
            sb.AppendLine("order by con.name");

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = sb.ToString();

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var key = $"{DataSchemaTypeConverter.ConvertTo<string>(reader["schema"])}.{DataSchemaTypeConverter.ConvertTo<string>(reader["table"])}.{DataSchemaTypeConverter.ConvertTo<string>(reader["column_name"])}";
                        ColumnDefaultNames[key] = DataSchemaTypeConverter.ConvertTo<string>(reader["constraint_name"]);
                    }
                }
            }            
        }

        public void GetColumns(IDictionary<string, DbSchemaTable> tables)
        {
            using (var connection = new SqlConnection(Reader.ConnectionString))
            {
                connection.Open();

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
            var sb = new StringBuilder();

            sb.AppendLine("SELECT OBJECT_SCHEMA_NAME(T.[object_id],DB_ID()) AS [TABLE_SCHEMA],  ");
            sb.AppendLine(" T.[name] AS [TABLE_NAME], I.[name] AS [INDEX_NAME], AC.[name] AS [column_name],  ");
            sb.AppendLine("I.[type_desc], I.[is_unique], I.[data_space_id], I.[ignore_dup_key], I.[is_primary_key], ");
            sb.AppendLine("I.[is_unique_constraint], I.[fill_factor],    I.[is_padded], I.[is_disabled], I.[is_hypothetical],");
            sb.AppendLine("I.[allow_row_locks], I.[allow_page_locks], IC.[is_descending_key], IC.[is_included_column] ");
            sb.AppendLine("FROM sys.[tables] AS T ");
            sb.AppendLine("INNER JOIN sys.[indexes] I ON T.[object_id] = I.[object_id]  ");
            sb.AppendLine("INNER JOIN sys.[index_columns] IC ON I.[object_id] = IC.[object_id] AND  I.[index_id] = IC.[index_id]");
            sb.AppendLine("INNER JOIN sys.[all_columns] AC ON T.[object_id] = AC.[object_id] AND IC.[column_id] = AC.[column_id] ");
            sb.AppendLine("WHERE T.[is_ms_shipped] = 0 AND I.[type_desc] <> 'HEAP' ");
            sb.AppendLine("ORDER BY T.[name], I.[index_id], IC.[key_ordinal]   ");

            using (var connection = new SqlConnection(Reader.ConnectionString))
            {
                connection.Open();

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = sb.ToString();

                    var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

                    while (reader.Read())
                    {
                        var schemaName = DataSchemaTypeConverter.ConvertTo<string>(reader["TABLE_SCHEMA"]);
                        var tableName = DataSchemaTypeConverter.ConvertTo<string>(reader["TABLE_NAME"]);

                        var key = $"{schemaName}.{tableName}";

                        if (tables.TryGetValue(key, out DbSchemaTable t))
                        {
                            var indexName = DataSchemaTypeConverter.ConvertTo<string>(reader["INDEX_NAME"]);
                            var isIncludedColumn = DataSchemaTypeConverter.ConvertTo<bool>(reader["is_included_column"]);

                            var existingIndex = t.Indexes.FirstOrDefault(p => p.Name == indexName);
                            if (existingIndex != null)
                            {
                                if (isIncludedColumn)
                                {
                                    existingIndex.Include.Add(DataSchemaTypeConverter.ConvertTo<string>(reader["column_name"]));
                                }
                                else
                                {
                                    existingIndex.Columns.Add(DataSchemaTypeConverter.ConvertTo<string>(reader["column_name"]));
                                }
                            }
                            else
                            {
                                var index =
                                    new DbSchemaTableColumnIndex
                                    {
                                        Name = DataSchemaTypeConverter.ConvertTo<string>(indexName),
                                        PrimaryKey = DataSchemaTypeConverter.ConvertTo<bool>(reader["is_primary_key"]),
                                        Unique = DataSchemaTypeConverter.ConvertTo<bool>(reader["is_unique"]),
                                        Clustered = string.Equals(DataSchemaTypeConverter.ConvertTo<string>(reader["type_desc"]), "CLUSTERED", StringComparison.OrdinalIgnoreCase),
                                    };

                                if (index.PrimaryKey)
                                {
                                    index.Type = DbSchemaTableColumnIndexType.Constraint;
                                }
                                else if (DataSchemaTypeConverter.ConvertTo<bool>(reader["is_unique_constraint"]))
                                {
                                    index.Type = DbSchemaTableColumnIndexType.Constraint;
                                }
                                else
                                {
                                    index.Type = DbSchemaTableColumnIndexType.Index;
                                }

                                if (isIncludedColumn)
                                {
                                    index.Include.Add(DataSchemaTypeConverter.ConvertTo<string>(reader["column_name"]));
                                }
                                else
                                {
                                    index.Columns.Add(DataSchemaTypeConverter.ConvertTo<string>(reader["column_name"]));
                                }

                                t.Indexes.Add(index);
                            }
                        }
                    }
                }
            }
        }

        public string GenerateAddTableColumn(string schema, string table, DbSchemaTableColumn column)
        {
            var sb = new StringBuilder();

            sb.Append($"ALTER TABLE [{schema}].[{table}] ADD [{column.Name}] ");

            switch (column.Type)
            {
                case DbSchemaColumnDataType.BigInteger:
                    {
                        sb.Append("bigint ");
                        break;
                    }
                case DbSchemaColumnDataType.Integer:
                    {
                        sb.Append("int ");
                        break;
                    }
                case DbSchemaColumnDataType.Boolean:
                    {
                        sb.Append("bit ");
                        break;
                    }
                case DbSchemaColumnDataType.DateTime:
                    {
                        sb.Append("datetime ");
                        break;
                    }
                case DbSchemaColumnDataType.UniqueIdentifier:
                    {
                        sb.Append("uniqueidentifier ");
                        break;
                    }
                case DbSchemaColumnDataType.VarString:
                    {
                        sb.Append($"nvarchar({column.Length}) ");
                        break;
                    }
                case DbSchemaColumnDataType.Text:
                    {
                        sb.Append("nvarchar(MAX) ");
                        break;
                    }
                case DbSchemaColumnDataType.Blob:
                    {
                        sb.Append("varbinary(MAX) ");
                        break;
                    }
            }
            if (column.Identity)
            {
                sb.Append("IDENTITY(1, 1) ");
            }
            switch (column.Default)
            {
                case DbSchemaColumnDefault.CurrentDateTime:
                    {
                        sb.Append("DEFAULT(getutcdate()) ");
                        break;
                    }
                case DbSchemaColumnDefault.NewUniqueIdentifier:
                    {
                        sb.Append("DEFAULT(newid()) ");
                        break;
                    }
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

        public string GenerateAlterTableColumn(string schema, string table, DbSchemaTableColumn column)
        {
            var sb = new StringBuilder();

            sb.Append($"ALTER TABLE [{schema}].[{table}] ALTER COLUMN [{column.Name}] ");

            switch (column.Type)
            {
                case DbSchemaColumnDataType.BigInteger:
                    {
                        sb.Append("bigint ");
                        break;
                    }
                case DbSchemaColumnDataType.Integer:
                    {
                        sb.Append("int ");
                        break;
                    }
                case DbSchemaColumnDataType.Boolean:
                    {
                        sb.Append("bit ");
                        break;
                    }
                case DbSchemaColumnDataType.DateTime:
                    {
                        sb.Append("datetime ");
                        break;
                    }
                case DbSchemaColumnDataType.UniqueIdentifier:
                    {
                        sb.Append("uniqueidentifier ");
                        break;
                    }
                case DbSchemaColumnDataType.VarString:
                    {
                        sb.Append($"nvarchar({column.Length}) ");
                        break;
                    }
                case DbSchemaColumnDataType.Text:
                    {
                        sb.Append("nvarchar(MAX) ");
                        break;
                    }
                case DbSchemaColumnDataType.Blob:
                    {
                        sb.Append("varbinary(MAX) ");
                        break;
                    }
            }

            sb.Append(column.NotNull ? "NOT NULL" : "NULL");

            return sb.ToString();
        }

        public string GenerateAlterColumnDefault(string schema, string table, DbSchemaTableColumn column)
        {
            var sb = new StringBuilder();

            if (ColumnDefaultNames.TryGetValue($"{schema}.{table}.{column.Name}", out string name))
            {
                sb.Append($"ALTER TABLE [{schema}].[{table}] DROP CONSTRAINT [{name}] ");
            }

            if (column.Default != DbSchemaColumnDefault.None)
            {
                sb.Append($"ALTER TABLE [{schema}].[{table}] ADD CONSTRAINT ");

                //Name the default.
                sb.Append($"DF_{schema}_{table}_{column.Name} ");

                switch (column.Default)
                {
                    case DbSchemaColumnDefault.CurrentDateTime:
                        {
                            sb.Append("DEFAULT(getutcdate()) ");
                            break;
                        }
                    case DbSchemaColumnDefault.NewUniqueIdentifier:
                        {
                            sb.Append("DEFAULT(newid()) ");
                            break;
                        }
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

                sb.Append($"FOR [{column.Name}]");
            }

            return sb.ToString();
        }

        public string GenerateDropTableColumn(string schema, string table, DbSchemaTableColumn column)
        {
            return $"ALTER TABLE [{schema}].[{table}] DROP COLUMN [{column.Name}]";
        }

        public string GenerateCreateIndex(string schema, string table, DbSchemaTableColumnIndex index)
        {
            var sb = new StringBuilder();
            var columns = string.Join(",", index.Columns.Select(c => $"[{c}]"));
            var include = string.Join(",", index.Include.Select(c => $"[{c}]"));
            
            if (index.Type == DbSchemaTableColumnIndexType.Constraint)
            {
                sb.Append($"ALTER TABLE [{schema}].[{table}] ADD CONSTRAINT [{index.Name}] ");
                if (index.PrimaryKey)
                {
                    sb.Append($"PRIMARY KEY ");
                }
                else if (index.Unique)
                {
                    sb.Append("UNIQUE ");
                }
                sb.Append(index.Clustered ? "CLUSTERED " : "NONCLUSTERED ");
                sb.Append($"({columns})");
            }
            else
            {
                sb.Append("CREATE ");
                if (index.Unique)
                {
                    sb.Append("UNIQUE ");
                }
                sb.Append(index.Clustered ? "CLUSTERED " : "NONCLUSTERED ");
                sb.Append($"INDEX [{index.Name}] ON [{schema}].[{table}] ({columns}) ");
                if (index.Include.Any())
                {
                    sb.Append($"INCLUDE ({include})");
                }
            }

            return sb.ToString();
        }

        public string GenerateAlterIndex(string schema, string table, DbSchemaTableColumnIndex index, string name)
        {
            var sb = new StringBuilder();
            var columns = string.Join(",", index.Columns.Select(c => $"[{c}]"));
            var include = string.Join(",", index.Include.Select(c => $"[{c}]"));

            //Drop Existing
            sb.AppendLine($"ALTER TABLE [{schema}].[{table}] DROP CONSTRAINT IF EXISTS [{name}]");
            sb.AppendLine($"DROP INDEX IF EXISTS [{name}] ON [{schema}].[{table}]");

            if (index.Type == DbSchemaTableColumnIndexType.Constraint)
            {
                sb.Append($"ALTER TABLE [{schema}].[{table}] ADD CONSTRAINT [{index.Name}] ");
                if (index.PrimaryKey)
                {
                    sb.Append($"PRIMARY KEY ");
                }
                else if (index.Unique)
                {
                    sb.Append("UNIQUE ");
                }
                sb.Append(index.Clustered ? "CLUSTERED " : "NONCLUSTERED ");
                sb.Append($"({columns})");
            }
            else
            {
                sb.Append("CREATE ");
                if (index.Unique)
                {
                    sb.Append("UNIQUE ");
                }
                sb.Append(index.Clustered ? "CLUSTERED " : "NONCLUSTERED ");
                sb.Append($"INDEX [{index.Name}] ON [{schema}].[{table}] ({columns}) ");
                if (index.Include.Any())
                {
                    sb.Append($"INCLUDE ({include})");
                }
            }

            return sb.ToString();
        }

        public string GenerateDropIndex(string schema, string table, DbSchemaTableColumnIndex index, string name)
        {
            var sb = new StringBuilder();
            
            //Drop Existing
            sb.AppendLine($"ALTER TABLE [{schema}].[{table}] DROP CONSTRAINT IF EXISTS [{name}]");
            sb.AppendLine($"DROP INDEX IF EXISTS [{name}] ON [{schema}].[{table}]");

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
                        cmd.CommandText = "select count(*) from information_schema.columns WHERE table_schema=@p0 AND table_name=@p1";

                        cmd.Parameters.Add(CreateParameter(0, table.Schema.ToLower()));
                        cmd.Parameters.Add(CreateParameter(1, table.Name.ToLower()));

                        var tableColumnsCount = DataSchemaTypeConverter.ConvertTo<int>(cmd.ExecuteScalar());

                        if (table.Columns.Count == tableColumnsCount)
                        {
                            //We just drop the table and return at this point.
                            sb.AppendLine($"DROP TABLE [{table.Schema}[.[{table.Name}]");
                            return sb.ToString();
                        }
                        else
                        {
                            sb.AppendLine($"ALTER TABLE [{table.Schema}].[{table.Name}] ");
                            for (int i = 0; i < table.Columns.Count; i++)
                            {
                                if (i > 0) sb.AppendLine(",");
                                sb.Append($"\tDROP COLUMN [{table.Columns[i].Name}]");
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
            var defaultString = DataSchemaTypeConverter.ConvertTo<string>(reader["COLUMN_DEFAULT"]);
            if (!string.IsNullOrEmpty(defaultString))
            {
                if (defaultString.Equals("(getutcdate())", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.CurrentDateTime;
                }
                if (defaultString.Equals("(newid())", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.NewUniqueIdentifier;
                }
                if (defaultString.Equals("((0))", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.Zero;
                }
                if (defaultString.Equals("((1))", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.One;
                }
                if (defaultString.Equals("((2))", StringComparison.OrdinalIgnoreCase))
                {
                    return DbSchemaColumnDefault.Two;
                }
                if (defaultString.Equals("((3))", StringComparison.OrdinalIgnoreCase))
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
                case "int": return DbSchemaColumnDataType.Integer;
                case "uniqueidentifier": return DbSchemaColumnDataType.UniqueIdentifier;
                case "bit": return DbSchemaColumnDataType.Boolean;
                case "datetime": return DbSchemaColumnDataType.DateTime;
                case "varbinary": return DbSchemaColumnDataType.Blob;
                case "nvarchar":
                    {
                        var length = DataSchemaTypeConverter.ConvertTo<int?>(reader["CHARACTER_MAXIMUM_LENGTH"]) ?? -1;
                        if (length > 0)
                        {
                            return DbSchemaColumnDataType.VarString;
                        }

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
            var p = new SqlParameter();
            p.ParameterName = $"@p{index}";
            p.DbType = DbType.String;
            p.Value = value;
            return p;
        }
    }
}
