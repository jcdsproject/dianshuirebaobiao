<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="FormulaDefine.aspx.cs" Inherits="AWHReports.Web.UI_FormulaExpression.FormulaDefine" %>

<!DOCTYPE html>

<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
<meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
      <title>计算公式定义</title>
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
    <script type="text/javascript" src="../lib/ealib/plugins/jquery.edatagrid.js" charset="utf-8"></script>

    <script type="text/javascript" src="js/page/FormulaDefine.js" charset="utf-8"></script>
</head>
<body>
  <div class="easyui-layout" data-options="fit:'true'">
                <div data-options="region:'north',border:false" style="height:48px">
                    <table>
                        <tr>
                            <td>
                                <table style="padding-top:10px">
                                    <tr>                                  
                                    <td style="width:60px;text-align:center">表类型：</td>
                                    <td>
                                        <input id="MeterType" class="easyui-combobox" required="required" style="width:80px"/>
                                    </td>
                                   <td><a href="javascript:void(0);" class="easyui-linkbutton" data-options="iconCls:'icon-reload',plain:true"
                                        onclick="RefreshFun();">刷新</a>
                                    </td>
                                    <td>
                                        <a id="add" href="javascript:void(0)" class="easyui-linkbutton" data-options="iconCls:'icon-add',plain:true" onclick="addFun()">添加</a>
                                    </td>
                             
                                     <td>
                                        <a href="#" id="edit" class="easyui-linkbutton" data-options="iconCls:'icon-save',plain:true" onclick="saveFun();">保存</a>
                                    </td>
                                     <td>
                                        <a href="javascript:void(0)" class="easyui-linkbutton" data-options="iconCls:'icon-undo',plain:true" onclick="reject()">撤销</a>
                                     </td>
                                     <td>
                                        <a href="#" id="delete" class="easyui-linkbutton" data-options="iconCls:'icon-cancel',plain:true" onclick="cancelFun();">删除</a>
                                    </td>
                                    </tr>
                                </table>                      
                           </td>
                          </tr>
                    </table>                            
                </div>
                <div id="formulaTable" class="easyui-panel" data-options="region:'center', border:true, collapsible:true, split:false">
                    <table id="grid_formulaDatagrid" class="easyui-edatagrid"></table>
                </div>   
        </div>
</body>
</html>
