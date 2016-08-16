$(function () {
    initPageAuthority();
    InitialLoad();
    LoadMeterData("first");
 
});

//初始化页面的增删改查权限
var authArray = new Array();
function initPageAuthority() {
    $.ajax({
        type: "POST",
        url: "FormulaDefine.aspx/AuthorityControl",
        data: "",
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        async: false,//同步执行
        success: function (msg) {
            authArray = msg.d;
            //增加
            if (authArray[1] == '0') {
                $("#add").linkbutton('disable');
            }
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
var mValue = "";
function InitialLoad() {
    $("#MeterType").combobox({
        valueField: 'value',
        textField: 'name',
        data: [{
            name: '电表',
            value:'Ammeter'
        }, {
            name: '水表',
            value: 'Watermeter'
        }, {
            name: '热表',
            value: 'Heatmeter'
        }],
        panelHeight: 'auto',
        onSelect: function (node) {
            mValue = node.value;
            if (authArray[0] == '1') {
                queryFormulaData(mValue);
            } else {
                $.messager.alert('提示','该用户没有查询权限！');
            }        
        }
    });
}
var mCancelID = "";
function LoadMeterData(type,myData) {
    if (type == "first") {
        $('#grid_formulaDatagrid').datagrid({
            columns:
                    [[{ field: 'Name', title: '业主名称', width: 120, align: 'left', editor: 'text' },
                      { field: 'FormulaCode', title: '编码', width: 80, align: 'left', editor: 'text' },
                      { field: 'Formula', title: '统计公式', width: 240, align: 'left', editor: 'text' },
                      { field: 'Creator', title: '创建人', width: 80, align: 'left', editor: 'text' },
                      { field: 'SaveToHistory', title: '创建时间', width: 120, align: 'left', editor: 'text' },
                      { field: 'Visible', title: '是否可用', width: 80, align: 'left', editor: 'text' },
                       { field: 'Type', title: '类别', width: 120, align: 'left', editor: 'text' ,hidden:true},
                      { field: 'Remarks', title: '备注', width: 120, align: 'left', editor: 'text' }
                    ]],
            rownumbers: true,
            striped: true,
            singleSelect: true,
            data: [],
            onDblClickRow: function (index, row) {
                if (authArray[2] == '1') {
                    SelectheRow(index);
                } else { $.messager.alert('提示', '该用户没有修改权限！'); }
            },
            onClickRow: function (index, row) {
                mCancelID = row.ID;
            },
            fit: true
        });
    }
    else if (type == "last") {
        $("#grid_formulaDatagrid").datagrid('loadData', myData);
    }
}
var editingId = undefined;
var myDataGridObject = $('#grid_formulaDatagrid');
function endEditing() {
    if (editingId == undefined) {
        return true;
    }
    if (myDataGridObject.datagrid('selectRow', editingId)) {
        $('#grid_formulaDatagrid').datagrid('endEdit', editingId);
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
            $('#grid_formulaDatagrid').datagrid('selectRow', editingId)
                    .datagrid('beginEdit', editingId);
        } else {
            $('#grid_formulaDatagrid').datagrid('selectRow', editingId);
        }
    }
}
//保存修改
function saveFun() {
    endEditing();           //关闭正在编辑
    var m_DataGridData_updated = $('#grid_formulaDatagrid').datagrid('getChanges', 'updated');
    var m_DataGridData_inserted = $('#grid_formulaDatagrid').datagrid('getChanges', 'inserted');

    for (var i = 0; i < m_DataGridData_updated.length; i++) {
        m_DataGridData_updated[i]['children'] = [];
    }
    for (var i = 0; i < m_DataGridData_inserted.length; i++) {
        m_DataGridData_inserted[i]['children'] = [];
    }
    if (m_DataGridData_updated.length + m_DataGridData_inserted.length > 0) {
        var m_DataGridDataJson_updated = JSON.stringify(m_DataGridData_updated);
        var m_DataGridDataJson_inserted = JSON.stringify(m_DataGridData_inserted);
        $.ajax({
            type: "POST",
            url: "FormulaDefine.aspx/SaveFormulaDefineValues",
            data: "{meterType:'" + mValue + "',datagridDataUpdated:'" + m_DataGridDataJson_updated + "',datagridDataInserted:'" + m_DataGridDataJson_inserted + "'}",
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
    queryFormulaData(mValue);
}

//撤销修改
function reject() {
    if (editingId != undefined) {
        $('#grid_formulaDatagrid').datagrid('cancelEdit', editingId);
        editingId = undefined;
    }
}
//增加操作
function addFun() {
    $('#grid_formulaDatagrid').datagrid('appendRow', {});
}
//删除操作
function cancelFun() {
    $.ajax({
        type: "POST",
        url: "FormulaDefine.aspx/CancleFormulaDefineLine",
        data: "{meterType:'" + mValue +"',mCancelID:'" + mCancelID  + "'}",
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        success: function (msg) {
            //var m_Msg = jQuery.parseJSON(msg.d);
            var m_Msg = msg.d;
            if (m_Msg == '1') {
                $.messager.alert('提示', '删除成功！');
                RefreshFun();
            }
            else if (m_Msg == '0') {
                $.messager.alert('提示', '删除失败！');
            }
            //else if (m_Msg == '-1') {
            //    $.messager.alert('提示', '用户没有删除权限！');
            //}
            else {
                $.messager.alert('提示', m_Msg);
            }
        }
    });
}

var mger;
function queryFormulaData(myValue) {
    $.ajax({
        type: "POST",
        url: "FormulaDefine.aspx/GetFormulaJsonData",
        data: "{meterType:'" + myValue + "'}",
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        success: function (msg) {         
            mger.window('close');
            var myData = jQuery.parseJSON(msg.d);
            if (myData.total == 0) {
                LoadMeterData("last", []);
                $.messager.alert('提示', "没有查询的数据");             
                return;
            } else {             
                LoadMeterData("last", myData);
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
    $('#grid_formulaDatagrid').datagrid('loadData', []);
    $.messager.alert('失败', '获取数据失败');
}