using System.ComponentModel;

namespace Simego.DataSync.Providers.DbSchema.TypeConverters
{
    class DbProviderTypeConverter : StringConverter
    {
        public override bool GetStandardValuesSupported(ITypeDescriptorContext context)
        {
            //true means show a combo-box
            return true;
        }

        public override bool GetStandardValuesExclusive(ITypeDescriptorContext context)
        {
            //true will limit to list. false will show the list, 
            //but allow free-form entry
            return true;
        }

        public override StandardValuesCollection GetStandardValues(ITypeDescriptorContext context)
        {
            return new StandardValuesCollection(new [] { "SqlClient", "Npgsql", "MySql" });
        }
    }
}
