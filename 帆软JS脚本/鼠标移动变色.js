// 鼠标移动变色20230529 - 优化版
// 功能：实现表格行鼠标悬停变色效果，支持冻结列表格
// 日期：2026-01-15

// 配置参数
var CONFIG = {
    HOVER_BACKGROUND: "#E0FFFF",  // 鼠标悬停时的背景颜色
    SUPPORT_FROZEN: true,         // 是否启用冻结列支持
    HEADER_ROWS: 3                // 忽略的行数（表头行数）
};

// 存储原始样式的对象
var styleCache = {
    normal: {},      // 普通表格行的原始样式
    frozen: {}       // 冻结列表格行的原始样式
};

// 当前悬停的行元素
var $hoveredRow = null;
// 遍历计数器（保持兼容性）
var i = 0;

/**
 * 初始化鼠标悬停效果
 */
function initHoverEffect() {
    try {
        var $tableRows = $('.x-table tr:gt(' + (CONFIG.HEADER_ROWS - 1) + ')');
        
        if ($tableRows.length === 0) {
            console.warn('未找到表格行元素');
            return;
        }

        // 鼠标进入事件
        $tableRows.on('mouseenter', handleMouseEnter);
        
        // 鼠标离开事件
        $tableRows.on('mouseleave', handleMouseLeave);

        console.log('鼠标悬停效果初始化完成');

    } catch (error) {
        console.error('初始化失败:', error);
    }
}

/**
 * 处理鼠标进入事件
 */
function handleMouseEnter() {
    var $currentRow = $(this);
    var rowId = $currentRow.attr('id');
    
    if (!rowId) {
        rowId = 'row_' + Date.now() + '_' + Math.random().toString(36).substr(2);
        $currentRow.attr('id', rowId);
    }

    $hoveredRow = $currentRow;
    applyHoverEffect($currentRow, true);
}

/**
 * 处理鼠标离开事件
 */
function handleMouseLeave() {
    var $currentRow = $(this);
    
    if ($hoveredRow) {
        applyHoverEffect($hoveredRow, false);
    }
    
    $hoveredRow = null;
}

/**
 * 应用悬停效果
 */
function applyHoverEffect($row, isHover) {
    var rowId = $row.attr('id');
    if (!rowId) return;

    try {
        if (CONFIG.SUPPORT_FROZEN && $('#content-container #frozen-west').length > 0) {
            handleFrozenRow($row, rowId, isHover);
        } else {
            handleNormalRow($row, rowId, isHover);
        }
    } catch (error) {
        console.error('应用效果失败:', error);
    }
}

/**
 * 处理冻结行
 */
function handleFrozenRow($row, rowId, isHover) {
    $("#content-container #" + rowId).each(function() {
        var $currentRow = $(this);
        $currentRow.children("td").each(function() {
            var $cell = $(this);
            var cellIndex = $cell.index();
            var cacheKey = rowId + '_' + cellIndex;
            
            if (isHover) {
                if (!styleCache.frozen[cacheKey]) {
                    styleCache.frozen[cacheKey] = $cell.css("background-color");
                }
                $cell.css("background-color", CONFIG.HOVER_BACKGROUND);
            } else {
                if (styleCache.frozen[cacheKey]) {
                    $cell.css("background-color", styleCache.frozen[cacheKey]);
                    delete styleCache.frozen[cacheKey];
                }
            }
        });
    });
}

/**
 * 处理普通行
 */
function handleNormalRow($row, rowId, isHover) {
    $row.children("td").each(function() {
        var $cell = $(this);
        var cellIndex = $cell.index();
        var cacheKey = rowId + '_' + cellIndex;
        
        if (isHover) {
            if (!styleCache.normal[cacheKey]) {
                styleCache.normal[cacheKey] = $cell.css("background-color");
            }
            $cell.css("background-color", CONFIG.HOVER_BACKGROUND);
        } else {
            if (styleCache.normal[cacheKey]) {
                $cell.css("background-color", styleCache.normal[cacheKey]);
                delete styleCache.normal[cacheKey];
            }
        }
    });
}

// 页面加载后初始化
$(document).ready(function() {
    setTimeout(initHoverEffect, 100);
});

/**
 * 清除所有悬停效果
 */
function clearAllHoverEffects() {
    // 恢复所有改变的单元格样式
    $.each(styleCache.normal, function(cacheKey, originalColor) {
        var parts = cacheKey.split('_');
        var rowId = parts[0] + '_' + parts[1];
        var cellIndex = parseInt(parts[2]);
        
        $('#' + rowId + ' td:eq(' + cellIndex + ')').css('background-color', originalColor);
    });
    
    $.each(styleCache.frozen, function(cacheKey, originalColor) {
        var parts = cacheKey.split('_');
        var rowId = parts[0] + '_' + parts[1];
        var cellIndex = parseInt(parts[2]);
        
        $("#content-container #" + rowId + " td:eq(" + cellIndex + ")").css('background-color', originalColor);
    });
    
    // 清空缓存
    styleCache = { normal: {}, frozen: {} };
    $hoveredRow = null;
}

// 提供全局函数供外部调用
window.TableHoverEffect = {
    init: initHoverEffect,
    clear: clearAllHoverEffects,
    setConfig: function(config) {
        $.extend(CONFIG, config);
    },
    getConfig: function() {
        return $.extend({}, CONFIG);
    }
};