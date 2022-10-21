/*新背景色*/
var background_color = "rgb(255,0,0)";
/*用于存放原样式*/
var row_frozen_back_color = new Array();
/*用于存放原样式*/
var row_back_color = new Array();
/*用于调用原DOM*/
var $last_tr;
/*遍历计数器*/
var i = 0;
/*用于存放原样式*/
var col_back_color = new Array();
/*用于存放原样式*/
var last_col = "";
/*用于存放原样式*/
var current_col = "";
/*当鼠标经过时执行*/
$(".x-table td[id]").bind("mouseenter", function () {
    /*判断DOM是否可以获取到，防止空指针异常*/
    const $this = $(this);
    const $table = $this.closest('table')
    const $tr = $this.closest('tr')
    if ($table.find('>tr').index($tr) < 5) {
        return ;
    }
    if (typeof($last_tr) != "undefined") {
        if (typeof($(this).parent("tr").attr("id")) != "undefined") {
            /*判断DOM是否可以获取到，防止空指针异常*/
            if (typeof($("#content-container #frozen-west").attr("id")) != "undefined") {
                /*遍历id=content-container 下 id= $last_tr的元素*/
                $("#content-container #" + $last_tr.attr("id")).each(function () {
                    /*遍历当前元素下的td单元格*/
                    $(this).children("td").each(function () {
                        /*将单元格样式恢复原样*/
                        $(this).css("background-color", row_frozen_back_color[i][$(this).index()]);
                    });
                    /*遍历下一个元素*/
                    i = i + 1;
                });
                /*初始化遍历计数器*/
                i = 0;
            }
            /*if (typeof($("#content-container #frozen-west").attr("id")) != "undefined") 不成立那么执行*/
            else {
                /*遍历td元素*/
                $last_tr.children("td").each(function () {
                    /*将单元格恢复原来的样式*/
                    $(this).css("background-color", row_back_color[$(this).index()]);
                });
            }
            /*初始化存放原样式的数组*/
            row_frozen_back_color = [];
            /*初始化存放原样式的数组*/
            row_back_color = [];
        }
    }
    /*判断last_col 是否存在数据*/
    if (last_col != "") {
        /*过滤并遍历所有同列元素*/
        $("td[id^='" + last_col + "']").filter(function () {
            /*返回同列元素*/
            if ($(this).attr("id").split("-")[0].replace(/[^a-zA-Z]/g, "") == last_col) {
                /*返回同列元素*/
                return $(this);
            }
            /*遍历*/
        }).each(function () {
            /*将同列元素恢复原始样式*/
            $(this).css("background-color", col_back_color[$(this).parent("tr").attr("tridx")]);
        });
        /*初始化原样式数组*/
        col_back_color = [];
        /*初始化原样式数组*/
        last_col = "";
    }
    /*判断DOM对象是否存在，防止空指针异常*/
    if (typeof($(this).attr("id")) != "undefined") {
        /*将所有的列元素赋值于 current_col */
        current_col = $(this).attr("id").split("-")[0].replace(/[^a-zA-Z]/g, "");
        /*过滤属于同列的元素*/
        $("td[id^='" + current_col + "']").filter(function () {
            /*判断是否为同列的元素*/
            if ($(this).attr("id").split("-")[0].replace(/[^a-zA-Z]/g, "") == current_col) {
                /*返回同列的元素*/
                return $(this);
            }
            /*进行遍历*/
        }).each(function () {
            /*将同列元素的原始样式赋值到ol_back_color[$(this).parent("tr").attr("tridx")]*/
            col_back_color[$(this).parent("tr").attr("tridx")] = $(this).css("background-color");
        });
        /*判断DOM对象是否可以获取到，防止空指针操作*/
        if (typeof($("#content-container #frozen-west").attr("id")) != "undefined") {
            /*获取当前行元素*/
            $("#content-container #" + $(this).parent("tr").attr("id")).each(function () {
                /*初始化数组对象*/
                row_frozen_back_color[i] = new Array();
                /*遍历当前行下所有的td单元格*/
                $(this).children("td").each(function () {
                    /*将单元格背景样式赋予row_frozen_back_color[i][$(this).index()] */
                    row_frozen_back_color[i][$(this).index()] = $(this).css("background-color");
                    /*改变当前单元格的背景样式*/
                    $(this).css("background-color", background_color);
                });
                /*遍历下一个元素*/
                i = i + 1;
            });
            /*初始化遍历计数器*/
            i = 0;
        }
        /*if (typeof($("#content-container #frozen-west").attr("id")) != "undefined") { 不成立执行*/
        else {
            /*获取父元素下所有的行的单元格，并进行遍历*/
            $(this).parent("tr").children("td").each(function () {
                /*保存单元格样式到row_back_color[$(this).index()] */
                row_back_color[$(this).index()] = $(this).css("background-color");
                /*改变单元格的样式*/
                $(this).css("background-color", background_color);
            });
        }
        /*过滤出属于同一列单元格的元素对象*/
        $("td[id^='" + current_col + "']").filter(function () {
            /*是否属于同一列*/
            if ($(this).attr("id").split("-")[0].replace(/[^a-zA-Z]/g, "") == current_col) {
                /*返回同一列的DOM 对象*/
                return $(this);
            }
            /*对同一列的对象进行遍历*/
        }).each(function () {
            /*对同一列单元格对象的样式进行更改*/
            $(this).css("background-color", background_color);
        });
        /*初始化存储原始样式的变量*/
        current_col = "";
    }
});
/*鼠标离开后执行事件*/
$(".x-table td[id]").bind("mouseleave", function () {
    /*判断元素是否可以获取到，防止空指针异常*/
    if (typeof($(this).attr("id")) != "undefined") {
        /*将DOM对象保存到last_col，方便后续操作*/
        last_col = $(this).attr("id").split("-")[0].replace(/[^a-zA-Z]/g, "");
    }
    /*判断DOM对象是否可以获取到，防止空指针操作*/
    if (typeof($(this).parent("tr")) != "undefined") {
        /*将DOM 对象保存到last_tr 方便进行后续操作*/
        $last_tr = $(this).parent("tr");
    }
});