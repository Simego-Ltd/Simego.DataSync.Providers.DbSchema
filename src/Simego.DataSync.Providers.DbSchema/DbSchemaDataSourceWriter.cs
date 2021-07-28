using Simego.DataSync.Engine;
using Simego.DataSync.Interfaces;
using Simego.DataSync.Providers.DbSchema.Interfaces;
using Simego.DataSync.Providers.DbSchema.Models;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Simego.DataSync.Providers.DbSchema
{
    public class DbSchemaDataSourceWriter : DataWriterProviderBase
    {
        private DbSchemaDatasourceReader DataSourceReader { get; set; }
        private DataSchemaMapping Mapping { get; set; }
        private DbConnection Connection { get; set; }
        private IDbSchemaProvider DbProvider { get; set; }
        
        public override void AddItems(List<DataCompareItem> items, IDataSynchronizationStatus status)
        {
            if (items != null && items.Count > 0)
            {
                int currentItem = 0;

                foreach (var item in items)
                {
                    if (!status.ContinueProcessing)
                        break;

                    try
                    {
                        var itemInvariant = new DataCompareItemInvariant(item);

                        //Call the Automation BeforeAddItem (Optional only required if your supporting Automation Item Events)
                        Automation?.BeforeAddItem(this, itemInvariant, null);

                        if (itemInvariant.Sync)
                        {
                            #region Add Item
                            //Get the Target Item Data
                            Dictionary<string, object> targetItem = AddItemToDictionary(Mapping, itemInvariant);

                            var schema = DataSchemaTypeConverter.ConvertTo<string>(targetItem["Schema"]);
                            var table = DataSchemaTypeConverter.ConvertTo<string>(targetItem["TableName"]);                           
                            var objectType = DataSchemaTypeConverter.ConvertTo<string>(targetItem["ObjectType"]);

                            // TABLE_COLUMN
                            if (objectType == "TABLE_COLUMN")
                            {
                                var column = ToTableColumn(targetItem);
                                var sql = DbProvider.GenerateAddTableColumn(schema, table, column);

                                if(DataSourceReader.OutputSqlTrace)
                                    status.Message(sql);

                                if (!DataSourceReader.DoNotExecute)
                                {
                                    using (var cmd = Connection.CreateCommand())
                                    {
                                        cmd.CommandType = System.Data.CommandType.Text;
                                        cmd.CommandText = sql;
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                            }
                            // TABLE_INDEX
                            if (objectType == "TABLE_INDEX" || objectType == "TABLE_CONSTRAINT")
                            {
                                var index = ToTableIndex(targetItem, null);
                                var sql = DbProvider.GenerateCreateIndex(schema, table, index);

                                if (DataSourceReader.OutputSqlTrace)
                                    status.Message(sql);

                                if (!DataSourceReader.DoNotExecute)
                                {
                                    using (var cmd = Connection.CreateCommand())
                                    {
                                        cmd.CommandType = System.Data.CommandType.Text;
                                        cmd.CommandText = sql;
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                            }
                            //Call the Automation AfterAddItem (pass the created item identifier if possible)
                            Automation?.AfterAddItem(this, itemInvariant, null);
                        }

                        #endregion

                        ClearSyncStatus(item); //Clear the Sync Flag on Processed Rows

                    }
                    catch (SystemException e)
                    {
                        HandleError(status, e);
                    }
                    finally
                    {
                        status.Progress(items.Count, ++currentItem); //Update the Sync Progress
                    }
                }
            }
        }

        public override void UpdateItems(List<DataCompareItem> items, IDataSynchronizationStatus status)
        {
            if (items != null && items.Count > 0)
            {
                int currentItem = 0;

                foreach (var item in items)
                {
                    if (!status.ContinueProcessing)
                        break;

                    try
                    {
                        var itemInvariant = new DataCompareItemInvariant(item);
                        var itemIdentifier = itemInvariant.GetTargetIdentifier<string>();

                        //Call the Automation BeforeUpdateItem (Optional only required if your supporting Automation Item Events)
                        Automation?.BeforeUpdateItem(this, itemInvariant, itemIdentifier);

                        if (itemInvariant.Sync)
                        {
                            #region Update Item
                            
                            //Get the Target Item Data
                            Dictionary<string, object> targetItem = AddItemToDictionary(Mapping, itemInvariant);
                            Dictionary<string, object> targetChanges = UpdateItemToDictionary(Mapping, itemInvariant);

                            var schema = DataSchemaTypeConverter.ConvertTo<string>(targetItem["Schema"]);
                            var table = DataSchemaTypeConverter.ConvertTo<string>(targetItem["TableName"]);                           
                            var objectType = DataSchemaTypeConverter.ConvertTo<string>(targetItem["ObjectType"]);

                            // TABLE_COLUMN
                            if (objectType == "TABLE_COLUMN")
                            {
                                var column = ToTableColumn(targetItem);

                                //TODO: Write the code to Update the Item in the Target using item_id as the Key to the item.
                                if (targetChanges.ContainsKey("Length") || targetChanges.ContainsKey("NotNull"))
                                {
                                    var sql = DbProvider.GenerateAlterTableColumn(schema, table, column);
                                    
                                    if (DataSourceReader.OutputSqlTrace)
                                        status.Message(sql);

                                    if (!DataSourceReader.DoNotExecute)
                                    {
                                        using (var cmd = Connection.CreateCommand())
                                        {
                                            cmd.CommandType = System.Data.CommandType.Text;
                                            cmd.CommandText = sql;
                                            cmd.ExecuteNonQuery();
                                        }
                                    }
                                }
                                if (targetChanges.ContainsKey("ColumnDefault"))
                                {
                                    var sql = DbProvider.GenerateAlterColumnDefault(schema, table, column);
                                    
                                    if (DataSourceReader.OutputSqlTrace)
                                        status.Message(sql);

                                    if (!DataSourceReader.DoNotExecute)
                                    {
                                        using (var cmd = Connection.CreateCommand())
                                        {
                                            cmd.CommandType = System.Data.CommandType.Text;
                                            cmd.CommandText = sql;
                                            cmd.ExecuteNonQuery();
                                        }
                                    }
                                }
                            }
                            // TABLE_INDEX
                            if (objectType == "TABLE_INDEX" || objectType == "TABLE_CONSTRAINT")
                            {
                                var index = ToTableIndex(targetItem, null);
                                var sql = DbProvider.GenerateAlterIndex(schema, table, index, itemIdentifier);
                                
                                if (DataSourceReader.OutputSqlTrace)
                                    status.Message(sql);

                                if (!DataSourceReader.DoNotExecute)
                                {
                                    using (var cmd = Connection.CreateCommand())
                                    {
                                        cmd.CommandType = System.Data.CommandType.Text;
                                        cmd.CommandText = sql;
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                            }

                            //Call the Automation AfterUpdateItem 
                            Automation?.AfterUpdateItem(this, itemInvariant, itemIdentifier);


                            #endregion
                        }

                        ClearSyncStatus(item); //Clear the Sync Flag on Processed Rows
                    }
                    catch (SystemException e)
                    {
                        HandleError(status, e);
                    }
                    finally
                    {
                        status.Progress(items.Count, ++currentItem); //Update the Sync Progress
                    }

                }
            }
        }

        public override void DeleteItems(List<DataCompareItem> items, IDataSynchronizationStatus status)
        {
            if (items != null && items.Count > 0)
            {
                int currentItem = 0;

                var tables = new List<DbSchemaTable>();

                // Convert source data to a Table List.
                foreach (var item in items)
                {
                    var targetItem = AddItemToDictionary(Mapping, item);

                    var schema = DataSchemaTypeConverter.ConvertTo<string>(targetItem["Schema"]);
                    var name = DataSchemaTypeConverter.ConvertTo<string>(targetItem["TableName"]);
                    var objectType = DataSchemaTypeConverter.ConvertTo<string>(targetItem["ObjectType"]);

                    var table = GetTable(tables, schema, name);
                    if (table != null)
                    {
                        // TABLE_COLUMN
                        if (objectType == "TABLE_COLUMN")
                        {
                            table.Columns.Add(ToTableColumn(targetItem));
                        }
                        // TABLE_INDEX
                        if (objectType == "TABLE_INDEX" || objectType == "TABLE_CONSTRAINT")
                        {
                            table.Indexes.Add(ToTableIndex(targetItem, item.GetTargetIdentifier<string>()));
                        }
                    }
                }

                // Process the Tables
                foreach (var item in tables)
                {
                    if (!status.ContinueProcessing)
                        break;

                    try
                    {                        
                        var sql = DbProvider.GenerateDeleteTableObjects(item);

                        if (DataSourceReader.OutputSqlTrace)
                            status.Message(sql);

                        if (!DataSourceReader.DoNotExecute)
                        {
                            using (var cmd = Connection.CreateCommand())
                            {
                                cmd.CommandType = System.Data.CommandType.Text;
                                cmd.CommandText = sql;
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    catch (SystemException e)
                    {
                        HandleError(status, e);
                    }
                    finally
                    {
                        status.Progress(tables.Count, ++currentItem); //Update the Sync Progress
                    }
                }
            }
        }

        public override void Execute(List<DataCompareItem> addItems, List<DataCompareItem> updateItems, List<DataCompareItem> deleteItems, IDataSourceReader reader, IDataSynchronizationStatus status)
        {
            DataSourceReader = reader as DbSchemaDatasourceReader;

            if (DataSourceReader != null)
            {
                Mapping = new DataSchemaMapping(SchemaMap, DataCompare);
                DbProvider = DataSourceReader.DbProvider ?? throw new ArgumentNullException(nameof(DataSourceReader.DbProvider));

                using (Connection = DbProvider.GetConnection())
                {
                    // Load anything this provider specifcally needs.
                    DbProvider.Initialize(Connection);

                    //Process the Changed Items
                    if (addItems != null && status.ContinueProcessing) AddItems(addItems, status);
                    if (updateItems != null && status.ContinueProcessing) UpdateItems(updateItems, status);
                    if (deleteItems != null && status.ContinueProcessing) DeleteItems(deleteItems, status);
                }
            }
        }

        private static void HandleError(IDataSynchronizationStatus status, Exception e)
        {
            if (!status.FailOnError)
            {
                status.LogMessage(e.Message);
            }
            if (status.FailOnError)
            {
                throw e;
            }
        }

        private DbSchemaTableColumn ToTableColumn(IDictionary<string, object> targetItem)
        {
            var column = new DbSchemaTableColumn();

            foreach (var key in targetItem.Keys)
            {
                switch (key)
                {
                    case "Name":
                        {
                            column.Name = DataSchemaTypeConverter.ConvertTo<string>(targetItem[key]);
                            break;
                        }
                    case "Length":
                        {
                            column.Length = DataSchemaTypeConverter.ConvertTo<int>(targetItem[key]);
                            break;
                        }
                    case "NotNull":
                        {
                            column.NotNull = DataSchemaTypeConverter.ConvertTo<bool>(targetItem[key]);
                            break;
                        }
                    case "IsPrimaryKey":
                        {
                            column.PrimaryKey = DataSchemaTypeConverter.ConvertTo<bool>(targetItem[key]);
                            break;
                        }
                    case "IsIdentity":
                        {
                            column.Identity = DataSchemaTypeConverter.ConvertTo<bool>(targetItem[key]);
                            break;
                        }
                    case "DataType":
                        {
                            column.Type = (DbSchemaColumnDataType)Enum.Parse(typeof(DbSchemaColumnDataType), DataSchemaTypeConverter.ConvertTo<string>(targetItem[key]), true);
                            break;
                        }
                    case "ColumnDefault":
                        {
                            column.Default = (DbSchemaColumnDefault)Enum.Parse(typeof(DbSchemaColumnDefault), DataSchemaTypeConverter.ConvertTo<string>(targetItem[key]), true);
                            break;
                        }
                }

            }
            return column;
        }

        /// <summary>
        /// Returns a DbSchemaTableColumnIndex from a Dictionary.
        /// </summary>
        /// <param name="targetItem">Dictionary of name value pairs to build DbSchemaTableColumnIndex from</param>
        /// <param name="name">Name to use inplace of the name value in the dictionary.</param>
        /// <returns></returns>
        private DbSchemaTableColumnIndex ToTableIndex(IDictionary<string, object> targetItem, string name)
        {
            var index = new DbSchemaTableColumnIndex();

            foreach (var key in targetItem.Keys)
            {
                switch (key)
                {
                    case "Name":
                        {
                            index.Name = DataSchemaTypeConverter.ConvertTo<string>(name ?? targetItem[key]);
                            break;
                        }
                    case "Columns":
                        {
                            index.Columns = DataSchemaTypeConverter.ConvertTo<string[]>(targetItem[key]);
                            break;
                        }
                    case "Include":
                        {
                            index.Include = DataSchemaTypeConverter.ConvertTo<string[]>(targetItem[key]);
                            break;
                        }
                    case "IsPrimaryKey":
                        {
                            index.PrimaryKey = DataSchemaTypeConverter.ConvertTo<bool>(targetItem[key]);
                            break;
                        }
                    case "IsClustered":
                        {
                            index.Clustered = DataSchemaTypeConverter.ConvertTo<bool>(targetItem[key]);
                            break;
                        }
                    case "IsUnique":
                        {
                            index.Unique = DataSchemaTypeConverter.ConvertTo<bool>(targetItem[key]);
                            break;
                        }
                    case "ObjectType":
                        {
                            index.Type = DataSchemaTypeConverter.ConvertTo<string>(targetItem[key]) == "TABLE_INDEX" ? DbSchemaTableColumnIndexType.Index : DbSchemaTableColumnIndexType.Constraint;
                            break;
                        }                    
                }

            }
            return index;
        }

        private DbSchemaTable GetTable(List<DbSchemaTable> tables, string schema, string name)
        {
            var table = tables.FirstOrDefault(p => p.Schema == schema && p.Name == name);
            if(table != null)
            {
                return table;
            }

            table = new DbSchemaTable { Schema = schema, Name = name };
            tables.Add(table);
            return table;
        }
    }
}
