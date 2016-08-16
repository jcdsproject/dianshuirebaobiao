using AWHReports.Infrastructure.Configuration;
using SqlServerDataAdapter;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AWHReports.Service.StatisticalReport
{
    public class tableFormulaService
    {
        public static DataTable GettzMeterTable(string meterType, string dateType, string yearDate) 
        {
            string connectionString = ConnectionStringFactory.JCDSConnectionString;
            ISqlServerDataFactory dataFactory = new SqlServerDataFactory(connectionString);
            string mySql = @"select A.Name,A.MeterType,A.ReportType,A.StoreName,B.[Date],B.[CreationDate],B.[KeyID] from [dbo].[system_ReportDescription] A,[dbo].[tz_" + meterType + @"] B
                                where A.Isformula=1 
                                and A.Enabled=1
                                and B.ReportID=A.ID
                                and A.MeterType=@meterType
                                and A.ReportType=@dateType
                                and B.[Date]=@yearDate";
            SqlParameter[] para ={
                                    new SqlParameter("@meterType",meterType),
                                    new SqlParameter("@dateType",dateType),
                                    new SqlParameter("@yearDate",yearDate)
                                  };
            DataTable table = dataFactory.Query(mySql, para);
            return table;     
        }
        public static DataTable GettzMeterTable(string meterType, string dateType, string startTime, string endTime)
        {
            string connectionString = ConnectionStringFactory.JCDSConnectionString;
            ISqlServerDataFactory dataFactory = new SqlServerDataFactory(connectionString);
            //string sTime = Convert.ToDateTime(startTime.Trim()).ToString().Replace('/','-');
            //string eTime = Convert.ToDateTime(endTime.Trim()).ToString().Replace('/', '-');
            //DateTime sTime = DateTime.ParseExact(startTime.Trim(), "yyyy-MM-dd HH:mm:ss", null);
            //DateTime eTime = DateTime.ParseExact(endTime.Trim(), "yyyy-MM-dd HH:mm:ss", null);

            string mySql = @"select A.Name,A.MeterType,A.ReportType,A.StoreName,B.[Date],B.[CreationDate],B.[KeyID] from [dbo].[system_ReportDescription] A,[dbo].[tz_" + meterType + @"] B
                                where A.Isformula=1 
                                and A.Enabled=1
                                and B.ReportID=A.ID
                                and A.MeterType=@meterType
                                and A.ReportType=@dateType
                                and B.[Date]>=@startTime
                                and B.[Date]<=@endTime
                                order by B.[Date] desc";
//            string mySql = @"select A.Name,A.MeterType,A.ReportType,A.StoreName,B.[Date],B.[CreationDate],B.[KeyID] from [dbo].[system_ReportDescription] A,[dbo].[tz_" + meterType + @"] B
//                                            where  A.Enabled=1
//                                            and B.ReportID=A.ID
//                                            and A.MeterType=@meterType
//                                            and A.ReportType=@dateType
//                                            and B.[Date]>=@startTime
//                                            and B.[Date]<=@endTime
//                                            order by A.[Isformula],B.[Date] desc";
            SqlParameter[] para ={
                                    new SqlParameter("@meterType",meterType),
                                    new SqlParameter("@dateType",dateType),
                                    new SqlParameter("@startTime",startTime),
                                    new SqlParameter("@endTime",endTime)
                                  };
            DataTable table = dataFactory.Query(mySql, para);
            return table;
        }
        public static DataTable GetformulaDataTable(string tableName, string mKeyID, string meterType) 
        {
            DataTable table = new DataTable();
            if (meterType == "Ammeter")
            {
                table = getAmmeterformulaDataTable(tableName, mKeyID);
            }
            else if (meterType=="WaterMeter") {

                table = getWaterMeterformulaDataTable(tableName, mKeyID);
            }
            else if (meterType == "HeatMeter")
            {
                table = getHeatMeterformulaDataTable(tableName, mKeyID);
            }               
            return table;     
        }
        private static DataTable getAmmeterformulaDataTable(string tableName, string mKeyID)
        {
            string connectionString = ConnectionStringFactory.JCDSConnectionString;
            ISqlServerDataFactory dataFactory = new SqlServerDataFactory(connectionString);
            string mySql = @"select A.*,B.PowerPeakPrice,B.PowerValleyPrice,B.PowerFlatPrice,'' as TotalCost from [dbo].[{0}] A,[dbo].[system_AWHUnitPrice] B
                            where A.KeyID='{1}'
                            and B.Enabled=1
                            order by A.FormulaCode asc";
            mySql = string.Format(mySql, tableName, mKeyID);
            DataTable table = dataFactory.Query(mySql);
            foreach(DataRow dr in table.Rows){
                dr["TotalCost"] = (Convert.ToDecimal(dr["Peak"]) * Convert.ToDecimal(dr["PowerPeakPrice"]) + Convert.ToDecimal(dr["Valley"]) * Convert.ToDecimal(dr["PowerValleyPrice"]) + Convert.ToDecimal(dr["Flat"]) * Convert.ToDecimal(dr["PowerFlatPrice"])).ToString("0.00");       
            }
            return table;     
        }
        private static DataTable getWaterMeterformulaDataTable(string tableName, string mKeyID)
        {
            string connectionString = ConnectionStringFactory.JCDSConnectionString;
            ISqlServerDataFactory dataFactory = new SqlServerDataFactory(connectionString);
            string mySql = @"select A.[ID]
                          ,A.[KeyID]
                          ,A.[FormulaCode]
                          ,A.[ProcessName]
                          ,A.[Amountto] as WAmountto,B.WaterPrice,'' as TotalCost from [dbo].[{0}] A,[dbo].[system_AWHUnitPrice] B
                            where A.KeyID='{1}'
                            and B.Enabled=1
                            order by A.FormulaCode asc";
            mySql = string.Format(mySql, tableName, mKeyID);
            DataTable table = dataFactory.Query(mySql);
            foreach (DataRow dr in table.Rows)
            {
                dr["TotalCost"] = (Convert.ToDecimal(dr["WAmountto"]) * Convert.ToDecimal(dr["WaterPrice"])).ToString("0.00");
            }
            return table;    
        }
        private static DataTable getHeatMeterformulaDataTable(string tableName, string mKeyID)
        {
            string connectionString = ConnectionStringFactory.JCDSConnectionString;
            ISqlServerDataFactory dataFactory = new SqlServerDataFactory(connectionString);
            string mySql = @"select A.[ID]
                          ,A.[KeyID]
                          ,A.[FormulaCode]
                          ,A.[ProcessName]
                          ,A.[Amountto] as HAmountto,B.HeatPrice,'' as TotalCost from [dbo].[{0}] A,[dbo].[system_AWHUnitPrice] B
                            where A.KeyID='{1}'
                            and B.Enabled=1
                            order by A.FormulaCode asc";
            mySql = string.Format(mySql, tableName, mKeyID);
            DataTable table = dataFactory.Query(mySql);
            foreach (DataRow dr in table.Rows)
            {
                dr["TotalCost"] = (Convert.ToDecimal(dr["HAmountto"]) * Convert.ToDecimal(dr["HeatPrice"])).ToString("0.00");
            }
            return table;
        }

    }
}
