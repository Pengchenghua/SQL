// 帆软导出控件商品明细

var aa =  _g().parameterEl.getWidgetByName("prov").getText(); //获取控件显示值
var params = {prov:aa}; //参数值
var paramStr = encodeURIComponent(JSON.stringify(params)); // 加密转换字符串
var colNames = encodeURIComponent("订单时间,要求送货日期,接单时间,省区,订单类型,订单号,子号,子名称,站点,编码,名称,订单金额")
//指定导出的数据列，导出字段按此顺序排列，为空默认导出所有
var fileName = aa + '订单明细' + new Date().getTime(); //省区+订单明细+时间
_g().directExportToExcel("ds6",fileName, paramStr, colNames) //导出文件
;
