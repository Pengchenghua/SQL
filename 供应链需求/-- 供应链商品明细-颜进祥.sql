-- 供应链商品明细-颜进祥
-- 年份	销售大区名称	销售省区名称	销售渠道名称	销售业务类型名称	管理大类编码	管理大类名称	管理中类编码	管理中类名称	管理小类编码	管理小类名称	品牌名称	商品编码	商品条码	商品名称	彩食鲜商品采购级别名称	销售数量	含税销售金额	含税销售成本	含税毛利额
select substr(sdt,1,4) as   `年份`,
    performance_region_name as  `销售大区名称`,
    performance_province_name `销售省区名称`,
    classify_large_code	as	`管理大类编号`
,	classify_large_name	as	`管理大类名称`
,	classify_middle_code	as	`管理中类编号`
,	classify_middle_name	as	`管理中类名称`
,	classify_small_code	as	`管理小类编号`
,	classify_small_name	as	`管理小类名称`,

    channel_name as `销售渠道名称`,
    business_type_name as `销售业务类型名称`,
    a.goods_code	as	`商品编码`,
    goods_bar_code as   `商品条码`,
	goods_name	as	    `商品名称`,
	b.brand_name as       `品牌名称`,
	csx_purchase_level_name  as `彩食鲜商品采购级别名称`,
	sum(sale_qty)	as 	`销售数量`
,   sum(sale_amt)	as 	`含税销售金额`
,   sum(sale_cost)	as 	`含税销售成本`
,   sum(profit)	as	    `含税定价毛利额`
from csx_dws.csx_dws_sale_detail_di a 
left join 
(select goods_code,brand_name,csx_purchase_level_name from    csx_dim.csx_dim_basic_goods where sdt='current') b on a.goods_code=b.goods_code
where (sdt>='20230101' and sdt<='20230831') 
-- or (sdt<='20230831' and sdt>='20230101')
group by  
    substr(sdt,1,4)  ,
    performance_region_name  ,
    performance_province_name ,
    classify_large_code		
,	classify_large_name		
,	classify_middle_code		
,	classify_middle_name		
,	classify_small_code		
,	classify_small_name	,	

    channel_name  ,
    business_type_name  ,
    a.goods_code		,
    goods_bar_code  ,
	goods_name		,
	b.brand_name  ,
	csx_purchase_level_name 