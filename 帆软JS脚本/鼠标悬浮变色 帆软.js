// 鼠标悬浮变色 帆软
var $lasttd;
var lastcolor;
$('.x-table tr:gt(1) td').bind("mouseenter", function() {
	if ($lasttd) {
		$lasttd.parent().find("td").css('background', lastcolor);
	}
	lastcolor = $(this).css('background-color');
	if (!lastcolor)
		lastcolor = $(this).css('background');
	$(this).parent().find("td").css('background', '#e1fcfc');
	$lasttd = $(this);
});