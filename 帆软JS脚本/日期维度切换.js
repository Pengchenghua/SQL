// 日期维度js代码
//获取supplierID控件
var mon = this.options.form.getWidgetByName("mon"); //获取class控件
var quarter = this.options.form.getWidgetByName("quarter"); //获取class控件
var year = this.options.form.getWidgetByName("year"); //获取supplierID控件
var week = this.options.form.getWidgetByName("week"); //获取week
// var labl1 = this.options.form.getWidgetByName("lable1"); //获取supplierID控件
var date =new Date();
var y = date.getFullYear()  ; 
var m =date.getMonth()+1;
var d =date.getDate()-1;
if (m >= 1 && m <= 9) {
            m = "0" + m;
        }
if (d >= 1 && d <= 10) {
            d= "0" + d;
        }
var  smon = date.getFullYear() + m;
var  currentdate=y+m+d;
var form = this.options.form;//定义一个变量 form ，将获取到的当前表单对象赋值给form
var value = this.getValue(); // 获取控件值
if (value == "周") {
    //隐藏class控件
    form.visible(["week"]);
    form.invisible(["mon"]);
    form.invisible(["year"]);
    form.invisible(["quarter"]);
    // 重置控件
    year.reset();
    quarter.reset();
    mon.reset();
    week.options.allowBlank = false;    //周日期不为空
    quarter.options.allowBlank = true;	//季度选择为空
}else if (value == "月") {
//隐藏class控件
    form.visible(["mon"]);
    form.invisible(["week"]);
    form.invisible(["year"]);
    form.invisible(["quarter"]);
    year.reset();
    quarter.reset();
    week.reset();
    week.options.allowBlank = true;
    quarter.options.allowBlank = true;	
    this.options.form.getWidgetByName("mon").setValue(smon);
} else if (value == "季") {
    form.invisible(["mon"]);
    form.invisible(["week"]);
    form.visible(["quarter"]);
    form.invisible(["year"]);
    mon.reset();  //重置supplierID控件
    year.reset();
    week.reset();
    week.options.allowBlank = true;
    quarter.options.allowBlank = false;	
} else if (value == "年") {
    form.invisible(["mon"]);
    form.invisible(["week"]);
    form.invisible(["quarter"]);
    form.visible(["year"]);
    mon.reset();
    quarter.reset();
    week.reset();
    quarter.options.allowBlank = true;	
    week.options.allowBlank = true;
    this.options.form.getWidgetByName("year").setValue(y);
}