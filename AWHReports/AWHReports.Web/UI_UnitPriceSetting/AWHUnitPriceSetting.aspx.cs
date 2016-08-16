using AWHReports.Service.UnitPriceSetting;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Web;
using System.Web.Services;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace AWHReports.Web.UI_UnitPriceSetting
{
    public partial class AWHUnitPriceSetting : WebStyleBaseForEnergy.webStyleBase
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            base.InitComponts();
            if (!IsPostBack)
            {
#if DEBUG
                // 调试用,自定义的数据授权
                mPageOpPermission = "1111";
#endif
            }
        }
        [WebMethod]
        public static char[] AuthorityControl()
        {
            return mPageOpPermission.ToArray();
        }
        [WebMethod]
        public static string GetAWHUnitPriceDefine()
        {
                DataTable table = AWHUnitPriceSettingService.AWHUnitPriceDefineTable();
                string json = EasyUIJsonParser.DataGridJsonParser.DataTableToJson(table);
                return json;                
        }
        [WebMethod]
        public static int SaveAWHUnitPriceSettingValues(string datagridDataUpdated)
        {
            if (mPageOpPermission.ToArray()[2] == '1')
            {
                DataTable tableUpdated = EasyUIJsonParser.TreeGridJsonParser.JsonToDataTable(datagridDataUpdated);
                int m_Result = AWHUnitPriceSettingService.SaveAWHUnitPriceDefineValues(tableUpdated);
                m_Result = m_Result > 0 ? 1 : m_Result;
                return m_Result;
            }
            else 
            {
                return -1;
            }
        }
    }
}