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
        private IDictionary<string, string> ColumnDefaultNames { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public string Name => "SqlClient";

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
                    cmd.CommandText = DbInformationSchemaConstants.Q_COLUMNS_SQLSERVER;
                    if (!string.IsNullOrEmpty(Reader.CommandWhere))
                    {
                        cmd.CommandText = $"{cmd.CommandText} WHERE {Reader.CommandWhere}";
                    }

                    var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

                    while (reader.Read())
                    {
                        var schemaName = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_TABLE_SCHEMA]);
                        var tableName = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_TABLE_NAME]);

                        var key = $"{schemaName}.{tableName}";

                        if (!tables.ContainsKey(key))
                            tables[key] = new DbSchemaTable { Schema = schemaName, Name = tableName };

                        var t = tables[key];

                        t.Columns.Add(
                            new DbSchemaTableColumn
                            {
                                Name = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_COLUMN_NAME]),
                                Type = ToDataType(reader),
                                PrimaryKey = false,
                                Identity = DataSchemaTypeConverter.ConvertTo<bool>(reader[DbInformationSchemaConstants.C_IS_IDENTITY]),
                                Length = DataSchemaTypeConverter.ConvertTo<int?>(reader[DbInformationSchemaConstants.C_CHARACTER_LENGTH]) ?? -1,
                                NotNull = !DataSchemaTypeConverter.ConvertTo<bool>(reader[DbInformationSchemaConstants.C_IS_NULLABLE]),
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
                        var schemaName = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_TABLE_SCHEMA]);
                        var tableName = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_TABLE_NAME]);

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
                                    existingIndex.Include.Add(DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_COLUMN_NAME]));
                                }
                                else
                                {
                                    existingIndex.Columns.Add(DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_COLUMN_NAME]));
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
                                    index.Include.Add(DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_COLUMN_NAME]));
                                }
                                else
                                {
                                    index.Columns.Add(DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_COLUMN_NAME]));
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
            return $"ALTER TABLE [{schema}].[{table}] ADD [{column.Name}] {ToSqlType(column)} {ToSqlNotNull(column)} {ToSqlIdentity(column)} {ToSqlDefault(column)}";
        }

        public string GenerateAlterTableColumn(string schema, string table, DbSchemaTableColumn column)
        {
            return $"ALTER TABLE [{schema}].[{table}] ALTER COLUMN [{column.Name}] {ToSqlType(column)} {ToSqlNotNull(column)}";
        }

        public string GenerateAlterColumnDefault(string schema, string table, DbSchemaTableColumn column)
        {
            var sb = new StringBuilder();

            if (ColumnDefaultNames.TryGetValue($"{schema}.{table}.{column.Name}", out string name))
            {
                sb.AppendLine($"ALTER TABLE [{schema}].[{table}] DROP CONSTRAINT [{name}] ");
            }

            if (column.Default != DbSchemaColumnDefault.None)
            {
                sb.AppendLine($"ALTER TABLE [{schema}].[{table}] ADD CONSTRAINT DF_{schema}_{table}_{column.Name} {ToSqlDefault(column)} FOR [{column.Name}]");
            }

            // Return a NOP operation
            if (sb.Length == 0) return "SELECT 1";

            return sb.ToString();
        }

        public string GenerateDropTableColumn(string schema, string table, DbSchemaTableColumn column)
        {
            var sb = new StringBuilder();

            // If this Column has a Default we need to drop it first.
            if (ColumnDefaultNames.TryGetValue($"{schema}.{table}.{column.Name}", out string name))
            {
                sb.AppendLine($"ALTER TABLE [{schema}].[{table}] DROP CONSTRAINT [{name}] ");
            }

            sb.AppendLine($"ALTER TABLE [{schema}].[{table}] DROP COLUMN [{column.Name}]");

            return sb.ToString();
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
                            sb.AppendLine($"CREATE TABLE [{table.Schema}].[{table.Name}] (");
                            for (int i = 0; i < table.Columns.Count; i++)
                            {
                                DbSchemaTableColumn column = table.Columns[i];
                                if (i > 0) sb.AppendLine(",");
                                sb.Append($"\t[{column.Name}] {ToSqlType(column)} {ToSqlNotNull(column)} {ToSqlIdentity(column)} {ToSqlDefault(column)}");
                            }
                            sb.AppendLine(")");
                        }
                        else
                        {
                            //We need to Add Columns to the Table
                            var columnsToAdd = string.Join(",", table.Columns.Select(p => $"[{p.Name}] {ToSqlType(p)} {ToSqlNotNull(p)} {ToSqlDefault(p)}"));
                            sb.AppendLine($"ALTER TABLE [{table.Schema}].[{table.Name}] ADD {columnsToAdd}");                            
                        }
                    }
                }

            }
            if (table.Indexes.Count > 0)
            {
                // Add Indexes
                foreach (var index in table.Indexes)
                {
                    //Add to the script the Create Index
                    sb.AppendLine(GenerateCreateIndex(table.Schema, table.Name, index));
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
                        cmd.CommandText = DbInformationSchemaConstants.Q_COLUMNS_COUNT;

                        cmd.Parameters.Add(CreateParameter(0, table.Schema));
                        cmd.Parameters.Add(CreateParameter(1, table.Name));

                        var tableColumnsCount = DataSchemaTypeConverter.ConvertTo<int>(cmd.ExecuteScalar());

                        if (table.Columns.Count == tableColumnsCount)
                        {
                            //We just drop the table and return at this point.
                            sb.AppendLine($"DROP TABLE [{table.Schema}].[{table.Name}]");
                            return sb.ToString();
                        }
                        else
                        {
                            var defaultsToDrop = new List<string>();
                            foreach(var column in table.Columns)
                            {
                                if (ColumnDefaultNames.TryGetValue($"{table.Schema}.{table.Name}.{column.Name}", out string constraintName))
                                {
                                    defaultsToDrop.Add(constraintName);
                                }
                            }
                            if (defaultsToDrop.Any())
                            {
                                sb.AppendLine($"ALTER TABLE [{table.Schema}].[{table.Name}] DROP CONSTRAINT {string.Join(",", defaultsToDrop.Select(p => $"[{p}]"))}");
                            }
                            
                            var columnsToDrop = string.Join(",", table.Columns.Select(p => $"[{p.Name}]"));
                            sb.AppendLine($"ALTER TABLE [{table.Schema}].[{table.Name}] DROP COLUMN {columnsToDrop}");                            
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
            var dataTypeString = DataSchemaTypeConverter.ConvertTo<string>(reader[DbInformationSchemaConstants.C_DATA_TYPE]);
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
                        var length = DataSchemaTypeConverter.ConvertTo<int?>(reader[DbInformationSchemaConstants.C_CHARACTER_LENGTH]) ?? -1;
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

        private string ToSqlType(DbSchemaTableColumn column)
        {
            switch (column.Type)
            {
                case DbSchemaColumnDataType.BigInteger: return "bigint";
                case DbSchemaColumnDataType.Integer: return "int";
                case DbSchemaColumnDataType.Boolean: return "bit";
                case DbSchemaColumnDataType.DateTime: return "datetime";
                case DbSchemaColumnDataType.UniqueIdentifier: return "uniqueidentifier";
                case DbSchemaColumnDataType.VarString: return $"nvarchar({column.Length})";
                case DbSchemaColumnDataType.Text: return "nvarchar(MAX)";
                case DbSchemaColumnDataType.Blob: return "varbinary(MAX)";
            }

            throw new ArgumentOutOfRangeException(nameof(column), $"Invalid Sql Type: {column}");
        }

        private string ToSqlDefault(DbSchemaTableColumn column)
        {
            switch (column.Default)
            {
                case DbSchemaColumnDefault.CurrentDateTime: return "DEFAULT(getutcdate())";
                case DbSchemaColumnDefault.NewUniqueIdentifier: return "DEFAULT(newid())";
                case DbSchemaColumnDefault.Zero: return "DEFAULT(0)";
                case DbSchemaColumnDefault.One: return "DEFAULT(1)";
                case DbSchemaColumnDefault.Two: return "DEFAULT(2)";
                case DbSchemaColumnDefault.Three: return "DEFAULT(3)";
                default: return string.Empty;
            }
        }

        private string ToSqlNotNull(DbSchemaTableColumn column) => column.NotNull ? "NOT NULL" : "NULL";

        private string ToSqlIdentity(DbSchemaTableColumn column) => column.Identity ? "IDENTITY(1, 1)" : string.Empty;
    }
}
