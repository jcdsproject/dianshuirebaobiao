using AWHReports.Infrastructure.Configuration;
using SqlServerDataAdapter;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AWHReports.Service.StatisticalReport
{
    public class DataGridJsonParser
    {
        public static string GetRowsAndColumnsJson(DataTable table)
        {
            string connectionString = ConnectionStringFactory.JCDSConnectionString;
            ISqlServerDataFactory dataFactory = new SqlServerDataFactory(connectionString);
            DataTable columnsModelDataTable = new DataTable();
            StringBuilder sbuilder = new StringBuilder(@"SELECT A.[columnName],A.[fieldName],A.[fieldWidth],A.[fieldAlign]
                          FROM (");
            string[] strParm = new string[table.Columns.Count];
            for (int i = 0; i < table.Columns.Count;i++ )
            {
                strParm[i] = table.Columns[i].ColumnName.ToString().Trim();
                
                sbuilder.Append(@"SELECT *
                          FROM [dbo].[system_FieldContrast] where Enabled=1 and columnName='" + strParm[i] + "' union ");
            }       
            string mySql = sbuilder.ToString();
            mySql = mySql.Remove(mySql.Length - 6, 6) + ")A order by A.[ID]";
            columnsModelDataTable = dataFactory.Query(mySql);    //得到模版表
            string RowsJson = EasyUIJsonParser.DataGridJsonParser.GetDataRowJson(table, strParm);
            string ColumnsJson = GetColumnsJson(columnsModelDataTable);

            string m_TotalJson = "\"total\":" + table.Rows.Count;
            string json = "{" + m_TotalJson + "," + RowsJson + "," + ColumnsJson + "}";
            return json;
            //return "{" + m_TotalJson + "," + RowsJson + "," + ColumnsJson + "}";
        }
        private static string GetColumnsJson(DataTable fieldModel_DataTable) 
        {
            if (fieldModel_DataTable.Rows.Count == 0)
            {
                return "\"columns\":[]";
            }
            StringBuilder m_Columns = new StringBuilder();
            m_Columns.Append("\"columns\":[");
            for (int i = 0; i < fieldModel_DataTable.Rows.Count; i++)
            {
                if (i == 0)
                {
                    m_Columns.Append("{");
                }
                else
                {
                    m_Columns.Append(",{");
                }
                
                m_Columns.Append("\"field\":\"" +fieldModel_DataTable.Rows[i]["columnName"].ToString().Trim() + "\"");
                m_Columns.Append(",\"title\":\"" + fieldModel_DataTable.Rows[i]["fieldName"].ToString().Trim() + "\"");
                m_Columns.Append(",\"width\": " + fieldModel_DataTable.Rows[i]["fieldWidth"].ToString().Trim());
              //  m_Columns.Append(",\"headeralign\":\"" + fieldModel_DataTable.Rows[i]["fieldName"] + "\"");
                m_Columns.Append(",\"align\":\"" + fieldModel_DataTable.Rows[i]["fieldAlign"].ToString().Trim() + "\"");
                m_Columns.Append("}");
            }
            m_Columns.Append("]");
            return m_Columns.ToString();   
        }
    }
}
