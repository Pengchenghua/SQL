# 销售额与毛利率变动对利润贡献的分析公式

## 原始公式
```
(SUM_AGG(${上期销售额})*(${环比毛利率差值})+(SUM_AGG(${本期销售额})-SUM_AGG(${上期销售额}))*(${本期毛利率}-TOTAL(SUM_AGG(${本期毛利额}),0,"sum")/total(SUM_AGG(${本期销售额}),0,"sum")))/total(SUM_AGG(${上期销售额}),0,"sum")
```

## 改写后的清晰版本

### 定义变量
- `LY_Sales`: 上期销售额总和 (SUM_AGG(${上期销售额}))
- `CY_Sales`: 本期销售额总和 (SUM_AGG(${本期销售额}))
- `LY_Gross_Margin_Rate`: 上期毛利率
- `CY_Gross_Margin_Rate`: 本期毛利率
- `Gross_Margin_Diff`: 环比毛利率差值 (${环比毛利率差值})
- `Avg_Gross_Margin_Rate`: 平均毛利率 (TOTAL(SUM_AGG(${本期毛利额}),0,"sum")/total(SUM_AGG(${本期销售额}),0,"sum"))

### 改写后的公式
```
( LY_Sales * Gross_Margin_Diff + (CY_Sales - LY_Sales) * (CY_Gross_Margin_Rate - Avg_Gross_Margin_Rate) ) / LY_Sales
```

## 公式详解

### 第一部分：基期销售额 × 毛利率变化的影响
```
LY_Sales * Gross_Margin_Diff
```
- 作用：衡量如果所有商品都保持上期销售规模，仅因毛利率提升带来的利润增量

### 第二部分：销量增长 × 毛利率差异的影响
```
(CY_Sales - LY_Sales) * (CY_Gross_Margin_Rate - Avg_Gross_Margin_Rate)
```
- 作用：衡量新增销售额所带来的利润贡献，但要扣除“平均利润水平”的影响

### 第三部分：标准化结果
```
/ LY_Sales
```
- 作用：将结果标准化为每单位上期销售额所贡献的利润变化

## 伪代码形式
```
// 计算上期销售额总和
LY_Sales = SUM_AGG(上期销售额)

// 计算本期销售额总和
CY_Sales = SUM_AGG(本期销售额)

// 计算环比毛利率差值
Gross_Margin_Diff = 本期毛利率 - 上期毛利率

// 计算平均毛利率
Avg_Gross_Margin_Rate = 总毛利 / 总销售额

// 计算利润贡献
Profit_Contribution = (
    LY_Sales * Gross_Margin_Diff +
    (CY_Sales - LY_Sales) * (CY_Gross_Margin_Rate - Avg_Gross_Margin_Rate)
) / LY_Sales

return Profit_Contribution
```

## 应用场景
该公式主要用于：
1. 财务分析报告中的"利润驱动因素拆解"
2. 评估促销、调价或供应链优化的实际效果
3. 对比不同品类、区域或时间段的盈利能力变化
4. 分析销售额增长和毛利率提升对整体利润的不同贡献度
```
