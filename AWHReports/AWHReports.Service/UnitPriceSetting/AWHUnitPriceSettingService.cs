using AWHReports.Infrastructure.Configuration;
using SqlServerDataAdapter;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AWHReports.Service.UnitPriceSetting
{
    public class AWHUnitPriceSettingService
    {
        public static DataTable AWHUnitPriceDefineTable()
        {
            string connectionString = ConnectionStringFactory.JCDSConnectionString;
            ISqlServerDataFactory dataFactory = new SqlServerDataFactory(connectionString);
            DataTable result = new DataTable();
            string mySql = @"select * from [dbo].[system_AWHUnitPrice]";
            DataTable table = dataFactory.Query(mySql);
            return table;      
        }
        public static int SaveAWHUnitPriceDefineValues(DataTable tableUpdated) 
        {
            string connectionString = ConnectionStringFactory.JCDSConnectionString;
            ISqlServerDataFactory dataFactory = new SqlServerDataFactory(connectionString);        
            int m_UpdateRowCount = 0;
            string repDate = DateTime.Now.ToString();
            for (int i = 0; i < tableUpdated.Rows.Count; i++)
            {
                tableUpdated.Rows[i]["UpdateTime"] = repDate;
            }
            if (tableUpdated.Rows.Count >= 1)
            {
                m_UpdateRowCount = dataFactory.Update("system_AWHUnitPrice", tableUpdated, new string[] { "PriceID" });
            }
            return m_UpdateRowCount;   
        }
    }
}
