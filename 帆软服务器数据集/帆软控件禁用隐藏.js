//帆软控件禁用隐藏 js 
var classID = this.options.form.getWidgetByName("class"); //获取class控件
var dept = this.options.form.getWidgetByName("dept"); //获取supplierID控件
var form = this.options.form;//定义一个变量 form ，将获取到的当前表单对象赋值给form
var value = this.getValue(); // 获取控件值
if(value == "1"){
//隐藏class控件
form.invisible(["class"]);
classID.reset();  //重置supplierID控件
form.visible(["dept"]);
    class.options.allowBlank = true;
    dept.options.allowBlank=false;
}else if(value == "2"){
form.invisible(["dept"]);
dept.reset();  //重置supplierID控件
    form.visible(["class"]);
    dept.options.allowBlank = true;
    classID.options.allowBlank = false;

}