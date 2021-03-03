

-- 负毛利客户追踪 2-2--822  
 select
    region_code,
    region_name,
    province_code,
    province_name,
    customer_no,
    customer_name,
    sales_value_04,
    profit_04,
    profit_rate_04,
    sale_sdt_04,
    sales_value_05,
    profit_05,
    profit_rate_05,
    sale_sdt_05,
    sales_value_06,
    profit_06,
    profit_rate_06,
    sale_sdt_06,
    sales_value_07,
    profit_07,
    profit_rate_07,
    sale_sdt_07,
    sales_value_08,
    profit_08,
    profit_rate_08,
    sale_sdt_08,
    sales_value_09,
    profit_09,
    profit_rate_09,
    sale_sdt_09,
    first_sale_day
from csx_tmp.temp_negative_profit_01
;


-- 负毛利额

select region_code,
          region_name,
          province_code,
          province_name ,
          a.customer_no,
          customer_name,
          first_category_name,
          second_category_name,
          third_category_name,
          first_sale_day,
          attribute_name,
          age_note,
        (sales_value_04) sales_value_04,
        (profit_04) profit_04,
        (profit_rate_04) profit_rate_04,
        (sale_num_04) sale_num_04,
        (sales_value_05) sales_value_05,
        (profit_05) profit_05,
        (profit_rate_05) profit_rate_05,
        (sale_num_05) sale_num_05,
        (sales_value_06) sales_value_06,
        (profit_06) profit_06,
        (profit_rate_06) profit_rate_06,
        (sale_num_06) sale_num_06,
        (sales_value_07) sales_value_07,
        (profit_07) profit_07,
        (profit_rate_07) profit_rate_07,
        (sale_num_07) sale_num_07,
        (sales_value_08) sales_value_08,
        (profit_08) profit_08,
        (profit_rate_08) profit_rate_08,
        (sale_num_08) sale_num_08,
        (sales_value_09) sales_value_09,
        (profit_09) profit_09,
        (profit_rate_09) profit_rate_09,
        (sale_num_09) sale_num_09
from 
(select   region_code,
          region_name,
          province_code,
          province_name ,
          customer_no,
          customer_name,
          first_sale_day,
          age_note,
          `attribute`,          
        sum(case when mon='202004' then  sales_value end ) sales_value_04,
        sum(case when mon='202004' then profit end ) profit_04,
        sum(case when mon='202004' then profit/sales_value end ) profit_rate_04,
        sum(case when mon='202004' then sale_num end ) sale_num_04,
        sum(case when mon='202005' then  sales_value end ) sales_value_05,
        sum(case when mon='202005' then profit end ) profit_05,
        sum(case when mon='202005' then profit/sales_value end ) profit_rate_05,
        sum(case when mon='202005' then sale_num end ) sale_num_05,
        sum(case when mon='202006' then  sales_value end ) sales_value_06,
        sum(case when mon='202006' then profit end ) profit_06,
        sum(case when mon='202006' then profit/sales_value end ) profit_rate_06,
        sum(case when mon='202006' then sale_num end ) sale_num_06,
        sum(case when mon='202007' then  sales_value end ) sales_value_07,
        sum(case when mon='202007' then profit end ) profit_07,
        sum(case when mon='202007' then profit/sales_value end ) profit_rate_07,
        sum(case when mon='202007' then sale_num end ) sale_num_07,
        sum(case when mon='202008' then  sales_value end ) sales_value_08,
        sum(case when mon='202008' then profit end ) profit_08,
        sum(case when mon='202008' then profit/sales_value end ) profit_rate_08,
        sum(case when mon='202008' then sale_num end ) sale_num_08,
        sum(case when mon='202009' then  sales_value end ) sales_value_09,
        sum(case when mon='202009' then profit end ) profit_09,
        sum(case when mon='202009' then profit/sales_value end ) profit_rate_09,
        sum(case when mon='202009' then sale_num end ) sale_num_09
from  csx_tmp.temp_negative_profit_01
where  1=1
group by region_code,
          region_name,
          province_code,
          province_name ,
          customer_no,
          customer_name,
          first_sale_day,
          `attribute`,
          age_note
) a 
left join 
(select customer_no,first_category_name,second_category_name,third_category_name,attribute_name from csx_dw.dws_crm_w_a_customer_20200924 where sdt='current') b on a.customer_no=b.customer_no
where 1=1
and province_code ='23'
and (round(profit_04,0)<=-100 or round(profit_05,0)<=-100 or profit_06<=-100 or profit_07<=-100 or profit_08<=-100 or profit_09<=-100) 
--and (round(sales_value_04,0)>0 or round(sales_value_05,0)>0 or sales_value_06>0 or sales_value_07>0 or sales_value_08>0 or sales_value_09>0) 
;
select * from csx_ods.source_wms_r_d_bills_config where sdt='20201024';
select * from csx_tmp.temp_negative_profit_01 where customer_no='108709';
SELECT * FROM csx_dw.wms_shipped_order where order_no='OM200929002340';
SELECT
	*
FROM
	csx_dw.dws_sale_r_d_sale_item_simple_20200921 a 
	where order_no ='OM20102200009571'
	and goods_code ='168'
	;

select province_code,
        0 sales_value,
        0 profit,
        sum(last_sale) last_sale,
        sum(last_profit) last_profit,
        0 sale_cust_num,
        count(DISTINCT a.customer_no) AS last_sale_cust_num
   from (

   SELECT substr(sdt,1,6) mon,
                    province_code,
                    a.customer_no,
                    sum(sales_value) last_sale,
                    round(sum(profit),2) last_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a 
    WHERE sdt>='20200404'
     AND sdt<='20200615'
    and channel in ('7','1','9')
   --  and a.return_flag !='X'
   and a.customer_no not in ('104444','102998')
   GROUP BY  province_code,
            customer_no,
            substr(sdt,1,6)
  )a 
    where  last_sale>0.00 and last_profit<-100.00
     group by province_code   
   ;
   -- and a.customer_no not in ('104444','102998')
   
 
select region_code,
          region_name,
          province_code,
          province_name ,
          customer_no,
          customer_name,
          first_sale_day,
          `attribute`,
          age_note,
          all_num,
        (sales_value_04) sales_value_04,
        (profit_04) profit_04,
        (profit_rate_04) profit_rate_04,
        (sale_num_04) sale_num_04,
        (sales_value_05) sales_value_05,
        (profit_05) profit_05,
        (profit_rate_05) profit_rate_05,
        (sale_num_05) sale_num_05,
        (sales_value_06) sales_value_06,
        (profit_06) profit_06,
        (profit_rate_06) profit_rate_06,
        (sale_num_06) sale_num_06,
        (sales_value_07) sales_value_07,
        (profit_07) profit_07,
        (profit_rate_07) profit_rate_07,
        (sale_num_07) sale_num_07,
        (sales_value_08) sales_value_08,
        (profit_08) profit_08,
        (profit_rate_08) profit_rate_08,
        (sale_num_08) sale_num_08,
        (sales_value_09) sales_value_09,
        (profit_09) profit_09,
        (profit_rate_09) profit_rate_09,
        (sale_num_09) sale_num_09,
        min_profit_rate,
         max_profit_rate
from 
(select   region_code,
          region_name,
          province_code,
          province_name ,
          customer_no,
          customer_name,
          first_sale_day,
          age_note,
          `attribute`,
          sum(sale_num) as all_num,
          sum(sales_value) as sales_value ,
          min(profit_rate) min_profit_rate,
          max(profit_rate) max_profit_rate,
        sum(case when mon='202004' then  sales_value end ) sales_value_04,
        sum(case when mon='202004' then profit end ) profit_04,
        sum(case when mon='202004' then profit/sales_value end ) profit_rate_04,
        sum(case when mon='202004' then sale_num end ) sale_num_04,
        sum(case when mon='202005' then  sales_value end ) sales_value_05,
        sum(case when mon='202005' then profit end ) profit_05,
        sum(case when mon='202005' then profit/sales_value end ) profit_rate_05,
        sum(case when mon='202005' then sale_num end ) sale_num_05,
        sum(case when mon='202006' then  sales_value end ) sales_value_06,
        sum(case when mon='202006' then profit end ) profit_06,
        sum(case when mon='202006' then profit/sales_value end ) profit_rate_06,
        sum(case when mon='202006' then sale_num end ) sale_num_06,
        sum(case when mon='202007' then  sales_value end ) sales_value_07,
        sum(case when mon='202007' then profit end ) profit_07,
        sum(case when mon='202007' then profit/sales_value end ) profit_rate_07,
        sum(case when mon='202007' then sale_num end ) sale_num_07,
        sum(case when mon='202008' then  sales_value end ) sales_value_08,
        sum(case when mon='202008' then profit end ) profit_08,
        sum(case when mon='202008' then profit/sales_value end ) profit_rate_08,
        sum(case when mon='202008' then sale_num end ) sale_num_08,
        sum(case when mon='202009' then  sales_value end ) sales_value_09,
        sum(case when mon='202009' then profit end ) profit_09,
        sum(case when mon='202009' then profit/sales_value end ) profit_rate_09,
        sum(case when mon='202009' then sale_num end ) sale_num_09
from  csx_tmp.temp_negative_profit_01
where  1=1
-- AND MON BETWEEN '202004' AND '202006'
--and sales_value >0
--and round(profit_rate*100,2)BETWEEN 0 and 5
--and province_code='32'
--and customer_no='104703'
group by region_code,
          region_name,
          province_code,
          province_name ,
          customer_no,
          customer_name,
          first_sale_day,
          `attribute`,
          age_note
) a where 1=1
and sales_value >0
and (round(profit_rate_04,2) BETWEEN 0.00 and 0.05 
     or round(profit_rate_05,2) BETWEEN 0.00 and 0.05 
     or round(profit_rate_06,2) BETWEEN 0.00 and 0.05 
     or round(profit_rate_07,2) BETWEEN 0.00 and 0.05 
     or round(profit_rate_08,2) BETWEEN 0.00 and 0.05 
     or round(profit_rate_09,2) BETWEEN 0.00 and 0.05 
     ) 

;        

select province_code ,
	province_name ,
	a.customer_no,
	customer_name,	
	first_category_name,
	second_category_name,
	third_category_name,
	attribute_name,
	sale_num,
	sales_value ,
	profit,
	profit/sales_value as profitrate,
	first_sale_day
from (
select province_code ,
	province_name ,
	a.customer_no,
	customer_name,
	first_category_name,
	second_category_name,
	third_category_name,
	attribute_name,
	count(distinct case when sales_value >0 then sdt end )sale_num,
	sum(sales_value)sales_value ,
	sum(profit)profit
from 
(select province_code ,
	province_name ,
	customer_no,
	sdt,
	sum(sales_value)sales_value ,
	sum(profit)profit
from csx_dw.dws_sale_r_d_customer_sale  
where sdt>='20200701' and sdt<'20201001'
	and channel in ('1','7','9')
group by province_code ,
	province_name ,
	customer_no,
	sdt
	)a 
join 
(select
	customer_no,
	customer_name ,
	first_category_name,
	second_category_name,
	third_category_name,
	attribute_name
from
	csx_dw.dws_crm_w_a_customer_20200924
where
	sdt = 'current') b on a.customer_no=b.customer_no
group by province_code ,
	province_name ,
	a.customer_no,
	customer_name,
	first_category_name,
	second_category_name,
	third_category_name,
	attribute_name
) a 
left join 
(select customer_no ,first_sale_day from csx_dw.ads_sale_w_d_ads_customer_sales_q where sdt='20201011') c on a.customer_no=c.customer_no
where profit <=-1000;