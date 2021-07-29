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
