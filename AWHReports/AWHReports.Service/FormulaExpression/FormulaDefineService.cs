using AWHReports.Infrastructure.Configuration;
using SqlServerDataAdapter;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AWHReports.Service.FormulaExpression
{
    public class FormulaDefineService
    {
        public static DataTable GetFormulaDataTable(string meterType)
        {
            string connectionString = ConnectionStringFactory.JCDSConnectionString;
            ISqlServerDataFactory dataFactory = new SqlServerDataFactory(connectionString);
            DataTable result = new DataTable();
            string mySql = @"select * from [dbo].[formula_Formula{0}Detail] order by FormulaCode";
            mySql=string.Format(mySql, meterType);
            DataTable table = dataFactory.Query(mySql);
            return table;
        }
        public static int SaveFormulaDefineValues(string meterType, DataTable tableUpdated, DataTable tableInserted) 
        {
            string connectionString = ConnectionStringFactory.JCDSConnectionString;
            ISqlServerDataFactory dataFactory = new SqlServerDataFactory(connectionString);
            string formulaTable = "formula_Formula" + meterType + "Detail";
            int m_UpdateRowCount = 0;
            int m_InserteRowCount = 0;
            string repDate=DateTime.Now.ToString();
            for (int i=0; i < tableUpdated.Rows.Count;i++ ) {
                tableUpdated.Rows[i]["SaveToHistory"] = repDate;
            }
            for (int i = 0; i < tableInserted.Rows.Count; i++)
            {
                tableInserted.Rows[i]["SaveToHistory"] = repDate;
            }
            if (tableUpdated.Rows.Count >=1 )
            {    
                m_UpdateRowCount = dataFactory.Update(formulaTable, tableUpdated, new string[] {"ID"});
           
            }
            if (tableInserted.Rows.Count >= 1)
            {
                m_InserteRowCount=dataFactory.Insert(formulaTable, tableInserted, new string[] { "Name", "FormulaCode", "Formula","Type", "Creator", "SaveToHistory", "Visible", "Remarks" });
            }
            return m_UpdateRowCount + m_InserteRowCount;   
        }
        public static int CancleFormulaDefineLine(string meterType, string mCancelID)
        {
            string connectionString = ConnectionStringFactory.JCDSConnectionString;
            ISqlServerDataFactory dataFactory = new SqlServerDataFactory(connectionString);
            string formulaTable = "formula_Formula" + meterType + "Detail";
            string mySql = @"delete from [dbo].[{0}] where ID='{1}'";
            mySql = string.Format(mySql, formulaTable, mCancelID);
            int m_Return=dataFactory.ExecuteSQL(mySql);
            return m_Return;
        }
    }
}
