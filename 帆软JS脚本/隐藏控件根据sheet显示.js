//现金采买隐藏控件
//参数面板设置
setTimeout(function () {
	//以下修改文本控件、数字控件的属性        
	$(".fr-texteditor").css({
		"background": "rgba(0,0,0,0)",
		"color": "#c0c0c0"
	}); //设置背景和字体颜色
	$(".fr-texteditor").css("border", "none"); //去除边框
	//  $("fr-trigger-text fr-trigger-texteditor").css({ "padding": "1px 8px!important"} );//文本框间距      
	//以下修改下拉单选控件，下拉复选控件和日期控件的属性
	$(".fr-trigger-text.fr-border-box").css({
		"background": "rgba(0,0,0,0)"
	}); //设置控件本身背景
	$(".fr-trigger-text").find("input").css({
		"background": "#FFFFFF",
		"color": "#000000"
	}); //设置控件输入框背景和字体颜色
	$(".fr-trigger-text").css("border", "none"); //去除边框
	$(".fr-trigger-btn-up").css({
		"background": "rgba(35,46,64,.40)",
		"border": "none",
		"width": "16px",
		"height": "18px"
	}); //设置控件右侧点击按钮  

}, 100);
var form = this.options.form;
var quarter = form.getWidgetByName("quarter");
var year = form.getWidgetByName("years");
var labelquarter = form.getWidgetByName("labelquarter");
var labelyear = form.getWidgetByName("labelyear");
//quarter.setVisible(false);
//quarter.options.allowBlank=true;
//year.setVisible(false);
//labelyear.setVisible(false); 
//labelquarter.setVisible(false);
var a = _g().selectedIndex;
//alert(a);
if(a==0){
	quarter.setVisible(false);
	quarter.options.allowBlank=true;
	labelquarter.setVisible(false);
} ;



// 填报页面设置


$(".fr-sheetbutton-container").click(function() //切换sheet时

{    var a = contentPane.$contentPane.data('TabPane').tabBtns[contentPane.selectedIndex].options.name; //获取当前sheet的名字
    if(a == "无票现金采买可用额度录入") {
        contentPane.parameterEl.getWidgetByName("labelyear").setVisible(false);
        contentPane.parameterEl.getWidgetByName("years").setVisible(false);
        contentPane.parameterEl.getWidgetByName("labelquarter").setVisible(false);
        contentPane.parameterEl.getWidgetByName("quarter").setVisible(false);
    } else {
        contentPane.parameterEl.getWidgetByName("label0").setVisible(false);
        contentPane.parameterEl.getWidgetByName("mon").setVisible(false);

    }

});

setTimeout(function(){
    contentPane.parameterEl.getWidgetByName("labelquarter").setVisible(false);
    },5000);
    $(".fr-sheetbutton-container").click(function() //切换sheet时
    
    {    var a = contentPane.$contentPane.data('TabPane').tabBtns[contentPane.selectedIndex].options.name; //获取当前sheet的名字
        if(a == "无票现金采买可用额度录入") {
            contentPane.parameterEl.getWidgetByName("labelyear").setVisible(false);
            contentPane.parameterEl.getWidgetByName("years").setVisible(false);
            contentPane.parameterEl.getWidgetByName("labelquarter").setVisible(false);
        } else {
            contentPane.parameterEl.getWidgetByName("label0").setVisible(false);
            contentPane.parameterEl.getWidgetByName("mon").setVisible(false);
    
        }
    
    });