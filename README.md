# DbSchemaSync

A Data Sync connector for Synchronization of Db Schemas across multiple Tenants.

The idea of this connector is to facilitate Database Schema changes with Simego Online platform.

The Online Platform uses either a Microsoft SQL Server Database or PostgreSQL Database and for each Tenant on the platform we create a new Db Schema for the Tenant data.
Since applying Database changes across multiple Tenants can be painful this connector can be used to compare each tenant schema against a reference schema and then apply the changes necessary to ensure all Tenants have the same Database Schema.

Anyway this is more of any internal tool, however if you have a similar challenge then this might be a solution for you.

There are limitations because we target either SQL Server or PostgreSQL with our Online Solution we decided to only support a few Database features and Data Types this way we didn’t need to cater for too many differences.
So this Provider only works with Tables, Columns, Column Defaults, Indexes, Constraints, Identity, Primary Key

Also the Data Types are limited to

Integer,
BigInteger,
Boolean,
DateTime,
VarString,
Text,
UniqueIdentifier,
Blob

Column Defaults are Limited to

None,
NewUniqueIdentifier,
CurrentDateTime,
Zero,
One,
Two,
Three

It would be relatively easy to extend the Data Types and Column Defaults. However adding Views, Procedures, Foreign Keys, Check Constraints etc would involve more complex work.

## Installing the Connector

Data Sync has a quick install function to allow you to download and install connectors from GitHub. To access this open the File menu and select **Install Data Connector**.

![install-data-connector](https://github.com/Simego-Ltd/Simego.DataSync.Providers.DbSchema/assets/63856275/9e4801f1-2d99-4ea4-89f0-b38541e48d79)

This will open the Connector Installer Window, where you need to select the connector you want to install from the drop down and click **OK**. In this case we select **DbSchema** from the dropdown list.

![install-db-schema](https://github.com/Simego-Ltd/Simego.DataSync.Providers.DbSchema/assets/63856275/31277926-c389-42c3-9f9a-100d8cd2ced1)


If it was successful you should get a confirmation popup and you now need to close all instances of Data Sync and then re-start the program. 
![installed-connector-successfully](https://github.com/Simego-Ltd/Simego.DataSync.Providers.DbSchema/assets/63856275/d96f56a4-dca8-488b-9aad-df73e17c563e)

You can then access the connector by expanding the SQL Database folder and selecting **DbSchema Synchronisation**. 

![db-schema-sync-location](https://github.com/Simego-Ltd/Simego.DataSync.Providers.DbSchema/assets/63856275/a5a966ca-7b13-4e6d-a7f0-82178a3fd0e1)

## Using the Connector
To connect you need to enter in the connection string to your database and set the database provider to be the right provider for the database you are connecting to.

For SQL databases you would use the SqlClient provider, for MySQL you would use the MySql provider and for PostgreSQL you would use the Npgsql provider.

If you have already saved the connection to this database in the connection library, you can get the connection string by opening the connection in notepad and copying the connection string.

![db-schema-sync-connection](https://github.com/Simego-Ltd/Simego.DataSync.Providers.DbSchema/assets/63856275/d1626136-9120-4e72-b707-e9c912d7d144)

Once this has been entered you can click Connect to load the database schema into the datasource window.

![db-schema-sync-schemamap](https://github.com/Simego-Ltd/Simego.DataSync.Providers.DbSchema/assets/63856275/24434697-c5de-4ca3-b064-c3a1137d2764)

