-- 安徽大客户销售
select  a.customer_no,customer_name,second_category,sum(mon_sale)/datediff(DATE_FORMAT(CURRENT_TIMESTAMP(),  '%Y%m%d'),DATE_FORMAT(CURRENT_TIMESTAMP(), '%Y%m01') ) as avg_sale,sum(mon_sale)mon_sale,sum(mon_profit)mon_profit,sum(goods_cn)goods_cn,sum(day_sale)day_sale,sum(day_profit)day_profit,sum(day_goods_cn)day_goods_cn
from 
(
select a.customer_no,b.customer_name,second_category,sum(sales_value)mon_sale,sum(profit)mon_profit,COUNT(DISTINCT goods_code)goods_cn,0 day_sale,0 day_profit,0 day_goods_cn from data_center_report.sale_b2b_item_anhui a 
join
customer_m b on a.customer_no=b.customer_no
and a.sdt>=DATE_FORMAT(CURRENT_TIMESTAMP(),'%Y%m01')and a.sdt<=DATE_FORMAT(date_sub(CURRENT_TIMESTAMP(), INTERVAL 1 DAY),'%Y%m%d')
group by a.customer_no,b.customer_name,second_category 
union all 
select a.customer_no,b.customer_name,second_category,0 mon_sale,0 mon_profit,0 goods_cn,sum(sales_value)day_sale,sum(profit)day_profit,COUNT(DISTINCT goods_code)day_goods_cn from data_center_report.sale_b2b_item_anhui a 
join
customer_m b on a.customer_no=b.customer_no
and  a.sdt=DATE_FORMAT(CURRENT_TIMESTAMP(),'%Y%m%d')
group by a.customer_no,b.customer_name,second_category
union all 

select customer_name ,sum(mon_sale)mon_sale,sum(mon_profit)mon_profit,sum(goods_cn)goods_cn,sum(day_sale)day_sale,sum(day_profit)day_profit,SUM(day_goods_cn)day_goods_cn
from (
select '总计' as  customer_name,sum(sales_value)mon_sale,sum(profit)mon_profit,COUNT(DISTINCT goods_code)goods_cn,
	0 day_sale,0 day_profit,0 day_goods_cn from data_center_report.sale_b2b_item_anhui a 
where  a.sdt>=DATE_FORMAT(CURRENT_TIMESTAMP(),'%Y%m01')and a.sdt<=DATE_FORMAT(date_sub(CURRENT_TIMESTAMP(), INTERVAL 1 DAY),'%Y%m%d')
union all 
select '总计' as  customer_name,0 mon_sale,0 mon_profit,0 goods_cn,sum(sales_value)day_sale,
	sum(profit)day_profit,COUNT(DISTINCT goods_code)day_goods_cn from data_center_report.sale_b2b_item_anhui a 
where   a.sdt=DATE_FORMAT(CURRENT_TIMESTAMP(),'%Y%m%d')
)a 
group by customer_name;


select * from b2b.sale