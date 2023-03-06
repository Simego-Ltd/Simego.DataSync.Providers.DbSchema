using Simego.DataSync.Helpers;
using Simego.DataSync.Interfaces;
using Simego.DataSync.Providers.DbSchema.Interfaces;
using Simego.DataSync.Providers.DbSchema.Models;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Design;
using System.Drawing;
using System.Drawing.Design;
using System.Linq;
using System.Windows.Forms;

namespace Simego.DataSync.Providers.DbSchema
{
    [ProviderInfo(Name = "DbSchema Synchronisation", Description = "Simego Database Schema Synchronisation (SQL Server, PostgreSQL, MySQL)", Group = "SQL")]
    public partial class DbSchemaDatasourceReader : DataReaderProviderBase, IDataSourceSetup
    {
        private ConnectionInterface _connectionIf;
        
        [Browsable(false)]
        public IDbSchemaProvider DbProvider { get; private set; }

        [Category("Connection")]
        public string DbProviderType => DbProvider.Name;
        
        [Category("Connection")]
        [Editor(typeof(MultilineStringEditor), typeof(UITypeEditor))] 
        public string ConnectionString { get; set; }

        [Category("Settings")]
        [Editor(typeof(MultilineStringEditor), typeof(UITypeEditor))] 
        public string CommandWhere { get; set; }

        [Category("Settings")]
        public bool OutputSqlTrace { get; set; } = true;

        [Category("Settings")]
        public bool DoNotExecute { get; set; } = true;

        [Category("Settings")]
        public string IndexNameFormat { get; set; } = "Schema_Name";

        [Category("Filter")]
        public string FilterBySchema { get; set; }
        
        [Category("Filter")]
        public string FilterByTableName { get; set; }

        public DbSchemaDatasourceReader()
        {
            DbProvider = new SqlClientDbProvider(this);
        }

        public override DataTableStore GetDataTable(DataTableStore dt)
        {
            // Store the actual object name in the identifier
            dt.AddIdentifierColumn(typeof(string));

            var mapping = new DataSchemaMapping(SchemaMap, Side);
            var columns = SchemaMap.GetIncludedColumns();

            var tables = new Dictionary<string, DbSchemaTable>();
            var hashHelper = new HashHelper(HashHelper.HashType.MD5);

            //Read Columns
            DbProvider.GetColumns(tables);
            //Read Indexes
            DbProvider.GetIndexes(tables);

            foreach (var table in tables.Values)
            {
                if (FilterObject(table, FilterBySchema, FilterByTableName))
                    continue;

                foreach (var column in table.Columns)
                {
                    dt.Rows.AddWithIdentifier(mapping, columns,
                                    (item, columnName) =>
                                    {
                                        switch (columnName)
                                        {
                                            case "Schema": return table.Schema;
                                            case "ObjectType": return "TABLE_COLUMN";
                                            case "TableName": return table.Name;
                                            case "Name": return column.Name;
                                            case "DataType": return column.Type;
                                            case "Length": return column.Length;
                                            case "Precision": return column.IsPrecisionScaleType ? column.Precision : 0;
                                            case "Scale": return column.IsPrecisionScaleType ? column.Scale : 0;
                                            case "NotNull": return column.NotNull;
                                            case "ColumnDefault": return column.Default;
                                            case "IsIdentity": return column.Identity;
                                            case "IsPrimaryKey": return table.Indexes.Any(p => p.PrimaryKey && p.Columns.Contains(column.Name));
                                            case "IsClustered": return table.Indexes.Any(p => p.Clustered && p.Columns.Contains(column.Name));
                                            case "IsUnique": return table.Indexes.Any(p => p.Unique && p.Columns.Contains(column.Name));
                                            default: return null;
                                        }
                                    }, column.Name);
                }

                foreach (var index in table.Indexes)
                {
                    dt.Rows.AddWithIdentifier(mapping, columns,
                                    (item, columnName) =>
                                    {
                                        switch (columnName)
                                        {
                                            case "Schema": return table.Schema;
                                            case "ObjectType": return index.Type == DbSchemaTableColumnIndexType.Index ? "TABLE_INDEX" : "TABLE_CONSTRAINT";
                                            case "TableName": return table.Name;
                                            case "Name": return index.GetName(hashHelper, table, IndexNameFormat);
                                            case "IsIdentity": return false;
                                            case "IsPrimaryKey": return index.PrimaryKey;
                                            case "IsClustered": return index.Clustered;
                                            case "IsUnique": return index.Unique;
                                            case "Columns": return index.Columns.ToArray();
                                            case "Include": return index.Include.ToArray();
                                            case "Length": return 0;
                                            case "Precision": return 0;
                                            case "Scale": return 0;
                                            case "NotNull": return false;
                                            default: return null;
                                        }
                                    }, index.Name);
                }
            }

            return dt;
        }

        public override DataSchema GetDefaultDataSchema()
        {
            //Return the Data source default Schema.

            DataSchema schema = new DataSchema();

            schema.Map.Add(new DataSchemaItem("Schema", typeof(string), true, false, false, -1));
            schema.Map.Add(new DataSchemaItem("ObjectType", typeof(string), true, false, true, -1));
            schema.Map.Add(new DataSchemaItem("TableName", typeof(string), true, false, true, -1));
            schema.Map.Add(new DataSchemaItem("Name", typeof(string), true, false, true, -1));
            schema.Map.Add(new DataSchemaItem("DataType", typeof(string), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("Length", typeof(int), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("Precision", typeof(int), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("Scale", typeof(int), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("NotNull", typeof(bool), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("ColumnDefault", typeof(string), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("IsIdentity", typeof(bool), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("IsPrimaryKey", typeof(bool), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("IsClustered", typeof(bool), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("IsUnique", typeof(bool), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("Include", typeof(string[]), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("Columns", typeof(string[]), false, false, true, -1));

            return schema;

        }

        private bool FilterObject(DbSchemaTable table, string schemaFilter, string tableFilter)
        {
            if(!string.IsNullOrEmpty(schemaFilter) && !string.IsNullOrEmpty(tableFilter))
            {
                if(string.Equals(table.Schema, schemaFilter, StringComparison.OrdinalIgnoreCase) && string.Equals(table.Name, tableFilter, StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }
            else if (!string.IsNullOrEmpty(schemaFilter))
            {
                if (string.Equals(table.Schema, schemaFilter, StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }
            else if (!string.IsNullOrEmpty(tableFilter))
            {
                if (string.Equals(table.Name, tableFilter, StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
            
            return true;
        }

        public override List<ProviderParameter> GetInitializationParameters()
        {
            //Return the Provider Settings so we can save the Project File.
            return new List<ProviderParameter>
                       {
                            new ProviderParameter("DbProvider", DbProvider.Name),
                            new ProviderParameter("ConnectionString", ConnectionString),
                            new ProviderParameter("CommandWhere", CommandWhere),
                            new ProviderParameter("OutputSqlTrace", OutputSqlTrace.ToString()),
                            new ProviderParameter("DoNotExecute", DoNotExecute.ToString()),
                            new ProviderParameter("FilterBySchema", FilterBySchema),
                            new ProviderParameter("FilterByTableName", FilterByTableName),
                            new ProviderParameter("IndexNameFormat", IndexNameFormat)
                       };
        }

        public override void Initialize(List<ProviderParameter> parameters)
        {
            //Load the Provider Settings from the File.
            foreach (ProviderParameter p in parameters)
            {
                AddConfigKey(p.Name, p.ConfigKey);

                switch (p.Name)
                {
                    case "DbProvider":
                        {
                            SetProvider(p.Value);
                            break;
                        }
                    case "ConnectionString":
                        {
                            ConnectionString = p.Value;
                            break;
                        }
                    case "CommandWhere":
                        {
                            CommandWhere = p.Value;
                            break;
                        }
                    case "FilterBySchema":
                        {
                            FilterBySchema = p.Value;
                            break;
                        }
                    case "FilterByTableName":
                        {
                            FilterByTableName = p.Value;
                            break;
                        }
                    case "IndexNameFormat":
                        {
                            IndexNameFormat = p.Value;
                            break;
                        }
                    case "OutputSqlTrace":
                        {
                            if (bool.TryParse(p.Value, out bool val))
                            {
                                OutputSqlTrace = val;
                            }
                            break;
                        }
                    case "DoNotExecute":
                        {
                            if (bool.TryParse(p.Value, out bool val))
                            {
                                DoNotExecute = val;
                            }
                            break;
                        }
                    default:
                        {
                            break;
                        }
                }
            }            
        }

        public override IDataSourceWriter GetWriter()
        {
            return new DbSchemaDataSourceWriter { SchemaMap = SchemaMap };
        }

        #region IDataSourceSetup - Render Custom Configuration UI
        
        public void DisplayConfigurationUI(IntPtr parent)
        {
            var parentControl = Control.FromHandle(parent);
            
            if (_connectionIf == null)
            {
                _connectionIf = new ConnectionInterface();
                _connectionIf.PropertyGrid.SelectedObject = new ConnectionProperties(this);
            }

            _connectionIf.Font = parentControl.Font;
            _connectionIf.Size = new Size(parentControl.Width, parentControl.Height);
            _connectionIf.Location = new Point(0, 0);
            _connectionIf.Dock = System.Windows.Forms.DockStyle.Fill;

            parentControl.Controls.Add(_connectionIf);
        }

        public bool Validate()
        {
            try
            {
                if (string.IsNullOrEmpty(ConnectionString))
                {
                    throw new ArgumentException("You must specify a valid ConnectionString.");
                }

                return true;
            }
            catch (Exception e)
            {
                MessageBox.Show(e.Message, "DbSchemaDatasourceReader", MessageBoxButtons.OK, MessageBoxIcon.Information);
            }

            return false;

        }

        public IDataSourceReader GetReader()
        {
            return this;
        }

        #endregion

        public IList<string> QueryTenants(string sql)
        {
            var result = new List<string>();

            using(var connection = DbProvider.GetConnection())
            {
                using(var cmd = connection.CreateCommand())
                {
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = sql;

                    var reader = cmd.ExecuteReader(System.Data.CommandBehavior.CloseConnection);
                    while (reader.Read())
                    {
                        result.Add(reader.GetString(0));
                    }
                }
            }

            return result;
        }

        public void SetProvider(string name)
        {
            switch (name)
            {
                case "SqlClient":
                    {
                        DbProvider = new SqlClientDbProvider(this);
                        break;
                    }
                case "Npgsql":
                    {
                        DbProvider = new PostgreSqlDbProvider(this);
                        break;
                    }
                case "MySql":
                    {
                        DbProvider = new MySqlDbProvider(this);
                        break;
                    }
                default:
                    {
                        throw new ArgumentException(nameof(name), $"Unknown provider type: {name}");
                    }
            }
        }
    }
}
