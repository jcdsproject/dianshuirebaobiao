<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="table_FormulaHeatmeter.aspx.cs" Inherits="AWHReports.Web.UI_StatisticalReport.table_FormulaHeatmeter" %>

<!DOCTYPE html>

<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
<meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
    <title></title>
  <link rel="stylesheet" type="text/css" href="/lib/ealib/themes/gray/easyui.css" />
    <link rel="stylesheet" type="text/css" href="/lib/ealib/themes/icon.css" />
    <link rel="stylesheet" type="text/css" href="/lib/extlib/themes/syExtIcon.css" />
    <link rel="stylesheet" type="text/css" href="/lib/extlib/themes/syExtCss.css" />

    <script type="text/javascript" src="/lib/ealib/jquery.min.js" charset="utf-8"></script>
    <script type="text/javascript" src="/lib/ealib/jquery.easyui.min.js" charset="utf-8"></script>
    <script type="text/javascript" src="/lib/ealib/easyui-lang-zh_CN.js" charset="utf-8"></script>
    <script type="text/javascript" src="/lib/pllib/jquery.jqplot.min.js"></script>

    <script type="text/javascript" src="/js/common/format/DateTimeFormat.js" charset="utf-8"></script>
    <script type="text/javascript" src="js/common/MergeCell.js" charset="utf-8"></script>
      <!--[if lt IE 8 ]><script type="text/javascript" src="/js/common/json2.min.js"></script><![endif]-->
    <script src="../lib/dplib/WdatePicker.js" type="text/javascript" defer="defer" ></script>

    <script type="text/javascript" src="js/page/table_FormulaHeatmeter.js" charset="utf-8"></script>
</head>
<body>
  <div class="easyui-layout" data-options="fit:'true'">
               
               <div class="easyui-panel" data-options="region:'north',border:true"style="display:normal;height:45px;">   
                   <%--id="toolbar_tzReportDatagrid"--%> 
                    <%--<div class="easyui-panel" data-options="region:'west',border:true" style="height:300px;width:500px">--%>         
                    <table style="padding-top:10px">
                                <tr>                                  
                            <td style="width:60px;text-align:center">报表类型：</td>
                            <td>
                                <input id="reportType" class="easyui-combobox" required="required" style="width:80px"/>
                            </td>                         
                         
       
                            <%--年份--%>
                            <td class="myear" style="width:60px;text-align:center">年份：</td>
                            <td class="myear">
                                <input id="date_year" type="text" class="Wdate myear" style="width:80px" onFocus="WdatePicker({lang:'zh-cn',dateFmt:'yyyy'})"/>
                            </td>
                            <%--月份--%>
                            <td class="mmonth">开始月份：</td>
                            <td class="mmonth">
                                    <input id="date_smonth" type="text" class="Wdate mmonth"style="width:80px" onFocus="WdatePicker({lang:'zh-cn',dateFmt:'yyyy-MM'})"/>
                            </td>
                            <td class="mmonth">结束月份：</td>
                            <td class="mmonth">
                                    <input id="date_emonth" type="text" class="Wdate mmonth" style="width:80px" onFocus="WdatePicker({lang:'zh-cn',dateFmt:'yyyy-MM'})"/>  
                            </td>
                            <%--日期--%>
                                <td class="mday">开始日期：</td>
                            <td class="mday">
                                <input id="date_sday" type="text" class="Wdate mday"style="width:80px" onFocus="WdatePicker({lang:'zh-cn',dateFmt:'yyyy-MM-dd'})"/>
                            </td>
                            <td class="mday">结束日期：</td>
                            <td class="mday">
                                <input id="date_eday" type="text" class="Wdate mday" style="width:80px"onFocus="WdatePicker({lang:'zh-cn',dateFmt:'yyyy-MM-dd'})"/>
                            </td>
                

                            <td><a href="javascript:void(0);" class="easyui-linkbutton" data-options="iconCls:'icon-search',plain:true"
                                onclick="researchFun();">查询</a>
                            </td>                                                               
                            </tr>
                    </table>                                                                           
                 </div>    
                 <div id="toolbar_formulaDatagrid" style="display: none;height:35px;">              
                        <table style="padding-left:20px">
                            <tr>      
                                <td style="width:60px;text-align:center">报表名称：</td> 
                                <td>
                                   <input id="reportName" class="easyui-textbox" required="required"readonly="readonly" style="width:120px"/>
                                </td>                                     
                                <td style="width:60px;text-align:center">报表时间：</td>
                                 <td>
                                    <input id="reportTime" class="easyui-textbox" required="required"readonly="readonly" style="width:80px"/>
                                </td>                                                                               
                                <td>
                                    <a href="#"class="easyui-linkbutton" data-options="iconCls:'ext-icon-page_white_excel',plain:true"onclick="ExportFileFun();">导出</a>
                                </td> 
                                 <td>
                                    <a href="#" class="easyui-linkbutton" data-options="iconCls:'ext-icon-printer',plain:true" onclick="PrintFileFun();">打印</a>
                                </td>                                                              
                            </tr>
                        </table>                                                                         
                 </div>    
                <div class="easyui-panel" data-options="region:'west',border:true" style="height:300px;width:250px">    
                    <table id="grid_tzReportDatagrid" class="easyui-edatagrid"></table>                
                </div>
                <div id="formulaTable" class="easyui-panel" data-options="region:'center', border:true, collapsible:true, split:false">
                    <table id="grid_formulaDatagrid" class="easyui-edatagrid"></table>
                </div>   
        </div>
</body>
</html>

