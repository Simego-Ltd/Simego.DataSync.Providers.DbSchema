# DbSchemaSync
A Data Sync connector for Synchronization of Db Schemas across multiple Tenants.

The idea of this connector is to facilitate Database Schema changes with Simego Online platform.

The Online Platform uses either a Microsoft SQL Server Database or PostgreSql Database and for each Tenant on the platform we create a new Db Schema for the Tenant data.
Since applying Database changes across multiple Tenants can be painful this connector can be used to compare each tenant schema against a reference schema and then apply the changes necessary to ensure all Tenants have the same Database Schema.

Anyway this is more of any internal tool, however if you have a similar challenge then this might be a solution for you.
