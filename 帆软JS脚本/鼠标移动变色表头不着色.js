//鼠标移动变色表头不着色
/*新的背景颜色*/
var background_color = "rgb(255,0,0)";
/*用于保存原始背景色*/
var frozen_back_color = new Array();
/*用于保存原始背景色*/
var back_color = new Array();
/*用于保存已发生改变的DOM对象，方便后续恢复样式操作*/
var $last_tr;
/*遍历计数器*/
var i = 0;
/* 首行除外，tr:gt(0)定位首行，索引从0开始，标题行为多行时注意此位置代码要做相应修改*/
$('.x-table tr:gt(1)') .bind("mouseenter", function () {
/*$(".x-table tr")*/
/*判断元素是否可以获取到，防止空指针操作*/
    if (typeof($last_tr) != "undefined") {
        /*判断元素是否可以获取到，防止空指针操作*/
        if (typeof($(this).attr("id")) != "undefined") {
            /*判断元素是否可以获取到，防止空指针操作*/
            if (typeof($("#content-container #frozen-west").attr("id")) != "undefined") {
                /*遍历所有的id值等于content-container下id=$last_tr的元素 ，即对已改变的元素进行操作*/
                $("#content-container #" + $last_tr.attr("id")).each(function () {
                    /*遍历所有的单元格元素怒*/
                    $(this).children("td").each(function () {
                        /*将单元格元素恢复原始色*/
                        $(this).css("background-color", frozen_back_color[$(this).index()]);
                    });
                    /*遍历下一个*/
                    i = i + 1;
                });
                /*初始化遍历计数器*/
                i = 0;
            }
            /*if (typeof($("#content-container #frozen-west").attr("id")) != "undefined") { 不成立*/
            else {
                /*获取所有的单元格元素*/
                $last_tr.children("td").each(function () {
                    /*恢复原来的样式*/
                    $(this).css("background-color", back_color[$(this).index()]);
                });
            }
            /*初始化存储原样式的数组*/
            frozen_back_color = [];
            /*初始化存储原样式的数组*/
            back_color = [];
        }
    }
    /*判断元素是否可以获取到，防止空指针操作*/
    if (typeof($(this).attr("id")) != "undefined") {
        /*判断元素是否可以获取到，防止空指针操作*/
        if (typeof($("#content-container #frozen-west").attr("id")) != "undefined") {
            /*获取content-container下所有的id=当前对象的元素*/
            $("#content-container #" + $(this).attr("id")).each(function () {
                /*初始化存放原始色的对象数组*/
                frozen_back_color = new Array();
                /*对所有单元格进行操作*/
                $(this).children("td").each(function () {
                    /*保存原始单元格背景色到frozen_back_color*/
                    frozen_back_color[$(this).index()] = $(this).css("background-color");
                    /*对当前单元格赋予新的颜色*/
                    $(this).css("background-color", background_color);
                });
                /*遍历下一个元素*/
                i = i + 1;
            });
            /*初始化遍历计数器*/
            i = 0;
        }
        /* if (typeof($("#content-container #frozen-west").attr("id")) != "undefined") { 不成立*/
        else {
            /*对当前节点下多有的td元素进行遍历操作*/
            $(this).children("td").each(function () {
                /*将当前对象的原始样式保存在 back_color数组中*/
                back_color[$(this).index()] = $(this).css("background-color");
                /*给当前元素赋予新的背景色*/
                $(this).css("background-color", background_color);
            });
        }
    }
});
/*首行除外的元素如果发生鼠标离开事件，tr:gt(0)定位首行，索引从0开始，标题多行时此位置代码要做相应修改*/
$('.x-table tr:gt(1)') .bind("mouseleave", function () {
    /*判断元素是否可以获取到，防止空指针异常*/
    if (typeof($(this).attr("id")) != "undefined") {
        /*将当前对象赋予$last_tr  方便后续操作*/
        $last_tr = $(this);
    }
});