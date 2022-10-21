
-- 帆软日期控件
var form = this.options.form;//定义一个变量 form ，将获取到的当前表单对象赋值给form
var smon = form.getWidgetByName("smon"); //获取class控件
var emon = form.getWidgetByName("emon"); //获取class控件
var sdt = form.getWidgetByName("sdt"); //获取class控件
var edt = form.getWidgetByName("edt"); //获取class控件
var week = form.getWidgetByName("week"); //获取week
// var labl1 = this.options.form.getWidgetByName("lable1"); //获取supplierID控件
var date =new Date();
var y = date.getFullYear()  ; 
var m = date.getMonth()+1;
var d = date.getDate()-1;
if (m< 10) m = '0'+m;
if (d< 10) d = '0'+d;
var smon = y +"-"+ m;
var emon = y +"-"+ m;
var edt = y+"-"+m+"-"+d;
var sdt =  y+"-"+m+"-"+"01";
var value = this.getValue(); // 获取控件值
if (value == "周") {
    //隐藏class控件
    form.visible(["week"]);
    form.invisible(["smon"]);
    form.invisible(["emon"]);
    week.setEnable(true);      // 周可用
    form.invisible(["sdt"]);
    form.invisible(["edt"]);
    // 重置控件
    this.options.form.getWidgetByName("smon").options.allowBlank = true; 
    this.options.form.getWidgetByName("smon").setValue("");
    this.options.form.getWidgetByName("emon").setText("");
    this.options.form.getWidgetByName("edt").reset();
    this.options.form.getWidgetByName("sdt").reset();
    this.options.form.getWidgetByName("smon").reset();
    this.options.form.getWidgetByName("emon").reset();
    this.options.form.getWidgetByName("sdt").setValue("");
    this.options.form.getWidgetByName("edt").setText("");
    week.options.allowBlank = false;    //周日期不为空    
}else if (value == "月") {
//隐藏class控件
    form.visible(["smon"]);
    form.visible(["emon"]);
    this.options.form.getWidgetByName("smon").setValue(smon);
    this.options.form.getWidgetByName("emon").setValue(emon);
    form.invisible(["week"]);    
    this.options.form.getWidgetByName("week").reset();    
    this.options.form.getWidgetByName("week").setEnable(false);      //周日期不可用
    this.options.form.getWidgetByName("week").options.allowBlank = true;    
    form.invisible(["sdt"]);
    form.invisible(["edt"]);
    //this.options.form.getWidgetByName("sdt").setEnable(false);      //周日期不可用
    //this.options.form.getWidgetByName("edt").setEnable(false);      //周日期不可用
    this.options.form.getWidgetByName("edt").reset();
    this.options.form.getWidgetByName("sdt").reset();
    this.options.form.getWidgetByName("sdt").setValue("");
    this.options.form.getWidgetByName("edt").setText("");
}else if (value == "日") {
    form.visible(["edt"]);
    form.visible(["sdt"]);
    form.invisible(["week"]); 
    form.invisible(["smon"]);
    form.invisible(["emon"]);   
    week.reset();
    week.setEnable(false);      //周日期不可用
    week.options.allowBlank = true; // 可为空
    this.options.form.getWidgetByName("smon").reset();
    this.options.form.getWidgetByName("emon").reset();
    this.options.form.getWidgetByName("smon").options.allowBlank = true;   //月不为空  
    this.options.form.getWidgetByName("emon").options.allowBlank = true; 
    this.options.form.getWidgetByName("smon").setValue("");
    this.options.form.getWidgetByName("emon").setText("");
    this.options.form.getWidgetByName("sdt").setValue(sdt);
    this.options.form.getWidgetByName("edt").setValue(edt);    
}