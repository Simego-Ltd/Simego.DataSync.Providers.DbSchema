using Simego.DataSync.Providers.DbSchema.Models;
using System.Collections.Generic;
using System.Data.Common;

namespace Simego.DataSync.Providers.DbSchema.Interfaces
{
    public interface IDbSchemaProvider
    {
        DbConnection GetConnection();
        void Initialize(DbConnection connection);

        //Get Schema
        void GetColumns(IDictionary<string, DbSchemaTable> tables);
        void GetIndexes(IDictionary<string, DbSchemaTable> tables);

        string GenerateCreateTableObjects(DbSchemaTable table);
        string GenerateDeleteTableObjects(DbSchemaTable table);

        //Columns
        string GenerateAddTableColumn(string schema, string table, DbSchemaTableColumn column);
        string GenerateAlterTableColumn(string schema, string table, DbSchemaTableColumn column);
        string GenerateAlterColumnDefault(string schema, string table, DbSchemaTableColumn column);
        string GenerateDropTableColumn(string schema, string table, DbSchemaTableColumn column);
        
        //Indexes
        string GenerateCreateIndex(string schema, string table, DbSchemaTableColumnIndex index);
        string GenerateAlterIndex(string schema, string table, DbSchemaTableColumnIndex index, string name);
        string GenerateDropIndex(string schema, string table, DbSchemaTableColumnIndex index, string name);
    }
}
