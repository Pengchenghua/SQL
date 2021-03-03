SELECT week_num,calweek,customer_no ,customer_name,goods_code,goods_name,division_code,division_name,first_category ,second_category ,sum(sales_qty )qty,sum(sales_value )sales_value,sum(profit )profit ,sum(sales_cost ) cost_price 
FROM csx_dw.dws_sale_r_d_customer_sale as a 
join 
(select calday,week_num,calweek from  csx_dw.dws_w_a_date_m) b on a.sdt=b.calday
WHERE sdt>='20200101' and customer_name like '%监狱%' and province_name like '%福建%'
group by 
customer_no ,customer_name,goods_code,goods_name,division_code,division_name,first_category ,week_num,calweek,second_category 
order by week_num,calweek asc 
;


SELECT week_num,calweek,goods_code,goods_name,division_code,division_name,first_category ,
second_category ,cost_price/qty as avg_cost,sales_value/qty as avg_price,qty,sales_value,profit ,cost_price,date_m ,
DENSE_RANK()over(PARTITION by division_code order by (sales_value) desc) as rank_num
from (
SELECT week_num,calweek,goods_code,c.goods_name,division_code,division_name,first_category ,
second_category,sum(sales_cost )/sum(sales_qty ) as avg_cost,sum(sales_value )/sum(sales_qty ) as avg_price,sum(sales_qty )qty,sum(sales_value )sales_value,sum(profit )profit ,sum(sales_cost ) cost_price,date_m 
FROM csx_dw.dws_sale_r_d_customer_sale as a 
join 
(
select calday,c.week_num,c.calweek,ROW_NUMBER()over(partition by c.week_num,c.calweek order by c.calweek) as row_s,date_m from  csx_dw.dws_w_a_date_m  c 
left join  
(select week_num,calweek,concat_ws('-',min(calday),max(calday)) as date_m from  csx_dw.dws_w_a_date_m c
where `year` ='2020'
 group by week_num,calweek) b on c.calweek=b.calweek
 where `year` ='2020' 
 ) b 
 on a.sdt=b.calday
 join 
 (select goods_id,goods_name from csx_dw.goods_m where sdt='20200602') c on a.goods_code=c.goods_id
WHERE sdt>='20200101' and customer_name like '%监狱%' and province_name like '%福建%'
and second_category='监狱'
group by 
customer_no ,customer_name,goods_code,c.goods_name,division_code,division_name,first_category ,week_num,calweek,second_category ,date_m
)a
order by week_num,calweek asc 
;