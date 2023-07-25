
select province_code ,
  		province_name ,
  		b.customer_no,
  		c.customer_name,
  		c.attribute_name,
       last_sale,
       last_profit,
       sale_num,
       b_sale,
       b_profit,
       first_order_date
    from 
(SELECT 
		province_code ,
  		province_name ,
       customer_no,
       last_sale,
       last_profit,
       sale_num
FROM
  (
  SELECT 	province_code ,
  		province_name ,
          customer_no,
          sum(sales_value)last_sale,
          sum(profit)last_profit,
          count(DISTINCT sdt ) as sale_num
   FROM csx_dw.dws_sale_r_d_detail
   WHERE sdt>='20201001' 
     and sdt<='20201231'
     AND channel_code IN ('1','7')
   GROUP BY province_code,
            province_name,
            customer_no 
    )a
WHERE last_profit < 0
  AND last_sale >0 
  )b
 left join 
(select a.customer_no ,sum(sales_value)b_sale,sum(profit) b_profit
from csx_dw.dws_sale_r_d_detail a
  where sdt>='20201001' and sdt<='20201231'
  group by 
  a.customer_no) a  on a.customer_no =b.customer_no
 join 
(select cm.customer_no ,customer_name,attribute_name from csx_dw.dws_crm_w_a_customer_20200924 cm where sdt='current') c on b.customer_no=c.customer_no
join 
(select customer_no ,i.first_order_date from csx_dw.dws_crm_r_a_customer_active_info i where sdt='20201231') d on b.customer_no=d.customer_no;
  
 
 
 
 with customer_sale as 
(
  select 
   -- substr(sdt, 1, 6) as month,
    customer_no,
    sum(profit) as profit_month,
    sum(sales_value) as sales_month
  from csx_dw.dws_sale_r_d_customer_sale 
  where sdt >= '20200701' and sdt < '20201001' 
	and channel  in ('1','7')
  group by  customer_no 
--  having sales_month > 0 and profit_month < 0
) 
select 
  count(DISTINCT  customer_no)
from customer_sale
where sales_month > 0 and profit_month < 0;

select
	sum(profit)/ sum(sales_value)
from
	csx_dw.dws_sale_r_d_customer_sale
where
	sdt >= '20201001'
	and sdt <= '20201123'
	and channel in ('1','7','9'	)
	and attribute_code !=5;
	

-- B端自营
 select
	sum(sales_value) sale,
	sum(profit),
	sum(profit)/sum(sales_value)
from
	csx_dw.dws_sale_r_d_customer_sale
where
	sdt >= '20201001'
	and sdt <= '20201130'
	and channel in ('1','7','9')
	and attribute_code != 5;
	
SELECT 
		province_code ,
  		province_name ,
       customer_no,
       last_sale,
       last_profit,
       sale_num
FROM
  (  SELECT 	province_code ,
  		province_name ,
          customer_no,
          sum(sales_value)last_sale,
          sum(profit)last_profit,
          count(DISTINCT sdt ) as sale_num
   FROM csx_dw.dws_sale_r_d_detail
   WHERE sdt>='20201001'
     AND sdt<='20201231'
     AND channel_code IN ('1','7')
   GROUP BY province_code,
            province_name,
            customer_no 
    )a
WHERE last_profit < 0
  AND last_sale >0 
  ;