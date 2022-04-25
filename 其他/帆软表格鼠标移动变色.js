var background_color = "#D3D3D3"; //新背景色
var frozen_back_color = new Array();
var back_color = new Array();
var $last_tr;
var i = 0;
// 首行除外
$(".x-table tr:gt(1)").bind("mouseenter", function () {
	//$(".x-table tr")
	if (typeof $last_tr != "undefined") {
		if (typeof $(this).attr("id") != "undefined") {
			if (
				typeof $("#content-container #frozen-west").attr("id") != "undefined"
			) {
				$("#content-container #" + $last_tr.attr("id")).each(function () {
					$(this)
						.children("td")
						.each(function () {
							$(this).css(
								"background-color",
								frozen_back_color[$(this).index()]
							);
						});
					i = i + 1;
				});
				i = 0;
			} else {
				$last_tr.children("td").each(function () {
					$(this).css("background-color", back_color[$(this).index()]);
				});
			}
			frozen_back_color = [];
			back_color = [];
		}
	}
	if (typeof $(this).attr("id") != "undefined") {
		if (typeof $("#content-container #frozen-west").attr("id") != "undefined") {
			$("#content-container #" + $(this).attr("id")).each(function () {
				frozen_back_color = new Array();
				$(this)
					.children("td")
					.each(function () {
						frozen_back_color[$(this).index()] = $(this).css(
							"background-color"
						);
						$(this).css("background-color", background_color);
					});
				i = i + 1;
			});
			i = 0;
		} else {
			$(this)
				.children("td")
				.each(function () {
					back_color[$(this).index()] = $(this).css("background-color");

					$(this).css("background-color", background_color);
				});
		}
	}
});
//首行除外
$(".x-table tr:gt(1)").bind("mouseleave", function () {
	if (typeof $(this).attr("id") != "undefined") {
		$last_tr = $(this);
	}
});
