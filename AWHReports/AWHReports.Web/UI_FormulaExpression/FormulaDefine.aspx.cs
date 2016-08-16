using AWHReports.Service.FormulaExpression;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Web;
using System.Web.Services;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace AWHReports.Web.UI_FormulaExpression
{
    public partial class FormulaDefine : WebStyleBaseForEnergy.webStyleBase
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            base.InitComponts();
            if (!IsPostBack)
            {
#if DEBUG
                // 调试用,自定义的数据授权
                mPageOpPermission = "1000";
#endif
            }
        }
        [WebMethod]
        public static char[] AuthorityControl()
        {
            return mPageOpPermission.ToArray();
        }
        [WebMethod]
        public static string GetFormulaJsonData(string meterType) 
        {
            DataTable table = FormulaDefineService.GetFormulaDataTable(meterType);
            string json = EasyUIJsonParser.DataGridJsonParser.DataTableToJson(table);
            return json;
        }
        [WebMethod]
        public static int SaveFormulaDefineValues(string meterType, string datagridDataUpdated, string datagridDataInserted) 
        {

            DataTable tableUpdated = EasyUIJsonParser.TreeGridJsonParser.JsonToDataTable(datagridDataUpdated);
            DataTable tableInserted =AddTypeToInsertedTable(meterType,EasyUIJsonParser.TreeGridJsonParser.JsonToDataTable(datagridDataInserted));
            int m_Result = FormulaDefineService.SaveFormulaDefineValues(meterType, tableUpdated, tableInserted);
            m_Result = m_Result > 0 ? 1 : m_Result;
            return m_Result;       
        }
        [WebMethod]
        public static int CancleFormulaDefineLine(string meterType,string mCancelID) 
        {
            int m_Result = FormulaDefineService.CancleFormulaDefineLine(meterType,mCancelID);
            m_Result = m_Result > 0 ? 1 : m_Result;
            return m_Result;
        }
        private static DataTable AddTypeToInsertedTable(string meterType,DataTable insertedTable) 
        {
            string mtype="";
            if(meterType=="Ammeter"){
                mtype="A";

            }else if (meterType == "Watermeter") { 
                  mtype="W";

            } else if (meterType == "Heatmeter") {
                  mtype="H";
            }
            foreach(DataRow dr in insertedTable.Rows){
            dr["Type"]=mtype;     
            }
            return insertedTable;
        }
    }
}