$(function () {
    initPageAuthority();
    LoadUnitPriceDefine("first");
    queryUnitPriceDefine();

});
//初始化页面的增删改查权限
var authArray = new Array;
function initPageAuthority() {
    $.ajax({
        type: "POST",
        url: "AWHUnitPriceSetting.aspx/AuthorityControl",
        data: "",
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        async: false,//同步执行
        success: function (msg) {
             authArray = msg.d;
            //增加
            //if (authArray[1] == '0') {
            //    $("#add").linkbutton('disable');
            //}
            //修改
            if (authArray[2] == '0') {
                $("#edit").linkbutton('disable');
            }
            //删除
            if (authArray[3] == '0') {
                $("#delete").linkbutton('disable');
            }
        }
    });
}
var mCancelID = "";
function LoadUnitPriceDefine(type, myData) {
    if (type == "first") {
        $('#grid_UnitPriceDefine').datagrid({
            columns:
                    [[
                        //{ field: 'PriceID', title: '编号', rowspan: 2, width: 120, align: 'center', editor: 'text' },
                      { title: '电表',colspan:3, width: 120 },
                      { title: '水表', width: 120},
                      { title: '热量表', width: 120 },
                       { field: 'Enabled', title: '是否执行', rowspan: 2, width: 120, align: 'center', editor: 'text' },
                      { field: 'Creator', title: '创建人', rowspan: 2, width: 120, align: 'center', editor: 'text' },
                      { field: 'UpdateTime', title: '创建时间', rowspan: 2, width: 120, align: 'center' }
                     
                    ]
                      , [{ field: 'PowerPeakPrice', title: '峰期电价', width: 120, align: 'center', editor: 'text' },
                      { field: 'PowerValleyPrice', title: '谷期电价', width: 120, align: 'center', editor: 'text' },
                      { field: 'PowerFlatPrice', title: '平期电价', width: 120, align: 'center', editor: 'text' },
                      { field: 'WaterPrice', title: '单价', width: 120, align: 'center', editor: 'text' },
                      { field: 'HeatPrice', title: '单价', width: 120, align: 'center', editor: 'text' }
                    ]],
           // rownumbers: true,
            striped: true,
            singleSelect: true,
            data: [],
            onDblClickRow: function (index, row) {
                if (authArray[2] == '1') {
                    SelectheRow(index);
                } else { $.messager.alert('提示', '该用户没有修改权限！'); }
            },
            onClickRow: function (index, row) {
                mCancelID = row.PriceID;
            },
            fit: true
        });
    }
    else if (type == "last") {
        $("#grid_UnitPriceDefine").datagrid('loadData', myData);
    }
}
var mger;
function queryUnitPriceDefine() {
    $.ajax({
        type: "POST",
        url: "AWHUnitPriceSetting.aspx/GetAWHUnitPriceDefine",
        data: "",
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        success: function (msg) {
            mger.window('close');
            var myData = jQuery.parseJSON(msg.d);
            if (myData.total == 0) {
                LoadUnitPriceDefine("last", []);
                $.messager.alert('提示', "没有查询的数据");
                return;
            } else {
                LoadUnitPriceDefine("last", myData);
            }
        },
        beforeSend: function (XMLHttpRequest) {
            //alert('远程调用开始...');
            mger = $.messager.alert('提示', "加载中...");
        },
        error: handleError
    });
}
function handleError() {
    $('#grid_UnitPriceDefine').datagrid('loadData', []);
    $.messager.alert('失败', '获取数据失败');
}
var editingId = undefined;
var myDataGridObject = $('#grid_UnitPriceDefine');
function endEditing() {
    if (editingId == undefined) {
        return true;
    }
    if (myDataGridObject.datagrid('selectRow', editingId)) {
        $('#grid_UnitPriceDefine').datagrid('endEdit', editingId);
        editingId = undefined;
        return true;
    } else {
        return false;
    }
}
//行单击事件
function SelectheRow(row) {
    if (editingId != row) {
        if (endEditing()) {
            editingId = row;
            $('#grid_UnitPriceDefine').datagrid('selectRow', editingId)
                    .datagrid('beginEdit', editingId);
        } else {
            $('#grid_UnitPriceDefine').datagrid('selectRow', editingId);
        }
    }
}
//保存修改
function saveFun() {
    endEditing();           //关闭正在编辑
    var m_DataGridData_updated = $('#grid_UnitPriceDefine').datagrid('getChanges', 'updated');
  //  var m_DataGridData_inserted = $('#grid_UnitPriceDefine').datagrid('getChanges', 'inserted');

    for (var i = 0; i < m_DataGridData_updated.length; i++) {
        m_DataGridData_updated[i]['children'] = [];
    }
    //for (var i = 0; i < m_DataGridData_inserted.length; i++) {
    //    m_DataGridData_inserted[i]['children'] = [];
    //}
    if (m_DataGridData_updated.length > 0) {
        var m_DataGridDataJson_updated = JSON.stringify(m_DataGridData_updated);
        //var m_DataGridDataJson_inserted = JSON.stringify(m_DataGridData_inserted);
        $.ajax({
            type: "POST",
            url: "AWHUnitPriceSetting.aspx/SaveAWHUnitPriceSettingValues",
            data: "{datagridDataUpdated:'" + m_DataGridDataJson_updated  + "'}",
            contentType: "application/json; charset=utf-8",
            dataType: "json",
            success: function (msg) {
                var m_Msg = msg.d;
                if (m_Msg == '1') {
                    $.messager.alert('提示', '修改成功！');
                    RefreshFun();
                }
                else if (m_Msg == '0') {
                    $.messager.alert('提示', '修改失败！');
                }
                else if (m_Msg == '-1') {
                    $.messager.alert('提示', '用户没有保存权限！');
                }
                else {
                    $.messager.alert('提示', m_Msg);
                }
            }
        });
    }
    else {
        $.messager.alert('提示', '没有任何数据需要保存');
    }
}
//刷新操作
function RefreshFun() {
    queryUnitPriceDefine();
}

//撤销修改
function reject() {
    if (editingId != undefined) {
        $('#grid_UnitPriceDefine').datagrid('cancelEdit', editingId);
        editingId = undefined;
    }
}
//增加操作
//function addFun() {
//    $('#grid_UnitPriceDefine').datagrid('appendRow', {});
//}
//删除操作
//function cancelFun() {
//    $.ajax({
//        type: "POST",
//        url: "AWHUnitPriceSetting.aspx/CancleAWHUnitPriceSettingLine",
//        data: "{mCancelID:'" + mCancelID + "'}",
//        contentType: "application/json; charset=utf-8",
//        dataType: "json",
//        success: function (msg) {
//            //var m_Msg = jQuery.parseJSON(msg.d);
//            var m_Msg = msg.d;
//            if (m_Msg == '1') {
//                $.messager.alert('提示', '删除成功！');
//                RefreshFun();
//            }
//            else if (m_Msg == '0') {
//                $.messager.alert('提示', '删除失败！');
//            }
//                //else if (m_Msg == '-1') {
//                //    $.messager.alert('提示', '用户没有删除权限！');
//                //}
//            else {
//                $.messager.alert('提示', m_Msg);
//            }
//        }
//    });
//}

