
// 参考： https://help.fanruan.com/finereport/doc-view-1197.html
var classID = this.options.form.getWidgetByName("class"); //获取class控件
var dept = this.options.form.getWidgetByName("dept"); //获取supplierID控件
var classIDvalue = this.options.form.getWidgetByName("class").getValue(); //获取class控件

var form = this.options.form;//定义一个变量 form ，将获取到的当前表单对象赋值给form
var value = this.getValue(); // 获取控件值
if(value == "1"){
//隐藏class控件
form.invisible(["class"]);
classID.reset();  //重置supplierID控件
form.visible(["dept"]);
    classID.options.allowBlank = true;
    dept.options.allowBlank=false;
    classID.setEnable= false;
}else if(value == "2"){
form.invisible(["dept"]);
dept.reset();  //重置supplierID控件
    form.visible(["class"]);
    dept.options.allowBlank = true;
    classID.options.allowBlank = false;


}


;


// 点击查询判断控件为空提示
var c=this.options.form.getWidgetByName("tree_c").getValue();
var d=this.options.form.getWidgetByName("tree").getValue();
var a=this.options.form.getWidgetByName("class").getValue();
var b=this.options.form.getWidgetByName("catgradio").getValue();
setTimeout(function(){
	if(c==""&&d==""){
	FR.Msg.toast("请选择发货DC或收货DC");
	return false;
	};

  if (b == "2") {
    if (a == "" || a == null) {
      FR.Msg.toast("请选择管理分类");
      return false;
    }
  }
},500);
