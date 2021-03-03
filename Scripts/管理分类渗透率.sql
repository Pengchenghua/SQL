--二级分类 冻品、蔬菜、米、面、粮油
with tmp_sale as 
(select
    case when a.channel in ('1','7','9') then '1'
        else a.channel
    end channel_code,
    case when a.channel in ('1','7','9') then '大客户'
        else a.channel_name
    end channel_name_01,
    a.customer_no,
    case when a.channel ='7' then 'BBC'
        when ( a.channel in ('1','9') and b.attribute_code=3) then '贸易客户'
        when ( a.channel in ('1','9') and b.attribute_code=5) then '合伙人客户'
        when ( a.channel in ('1','9') and order_kind='WELFARE') then '福利单'
    else '日配单'
    --    else  a.channel_name
    end attribute_name_01,
    case when channel='7' then '7'
            when ( a.channel in ('1','9') and b.attribute_code=3)  then '3'
            when ( a.channel in ('1','9') and b.attribute_code=5)  then '5'
            when ( a.channel in ('1','9') and order_kind='WELFARE') then '2'
            else '1'
    end attribute_code_01,
    division_code ,
    division_name,    
    classify_middle_code ,
    classify_middle_name ,
    sum(sales_value) sales_value,
    sum(profit) profit,
    count(distinct a.customer_no) as sale_cust_num
from
    csx_dw.dws_sale_r_d_customer_sale a
left join 
(select customer_no,attribute_code
		from csx_dw.dws_crm_w_a_customer_20200924
		where sdt='current' ) as b on a.customer_no =b.customer_no
left join 
(SELECT category_small_code,
       classify_middle_code,
       classify_middle_name
    FROM csx_dw.dws_basic_w_a_manage_classify_m
        WHERE sdt='current' 
        -- and classify_middle_code IN ('B0102','B0202','B0304','B0601','B0603')
        ) m on a.category_small_code=m.category_small_code
where
    sdt >=  '${l_sdate}'
    and sdt <=  '${edate}'
    and channel in ('1','7','9')
    and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)	
group by 
    case when a.channel in ('1','7','9') then '1'
        else a.channel
    end  ,
    case when a.channel in ('1','7','9') then '大客户'
        else a.channel_name
    end  ,
    case when a.channel ='7' then 'BBC'
        when ( a.channel in ('1','9') and b.attribute_code=3) then '贸易客户'
        when ( a.channel in ('1','9') and b.attribute_code=5) then '合伙人客户'
        when ( a.channel in ('1','9') and order_kind='WELFARE') then '福利单'
    else '日配单'
    --    else  a.channel_name
    end  ,
    case when channel='7' then '7'
            when ( a.channel in ('1','9') and b.attribute_code=3)  then '3'
            when ( a.channel in ('1','9') and b.attribute_code=5)  then '5'
            when ( a.channel in ('1','9') and order_kind='WELFARE') then '2'
            else '1'
    end  ,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name,
    a.customer_no
  )select classify_middle_code ,
    classify_middle_name,
    sales_value,
    profit,
    profit/    sales_value as profit_rate,
    sale_cust_num/all_sale_cust_num as stl,
    sale_cust_num,
    all_sale_cust_num
    from    
  (select classify_middle_code ,
    classify_middle_name,
    sum(sales_value)sales_value,
    sum(profit) profit,
    count(distinct case when sales_value>0 then  a.customer_no end ) as sale_cust_num
    from tmp_sale a 
     where a. attribute_name_01='日配单' 
    -- and classify_middle_code IN ('B0102','B0202','B0304','B0601','B0603') 
    group by classify_middle_code ,
    classify_middle_name
    )a
    left join 
   (select count(distinct case when sales_value>0 then customer_no end ) as all_sale_cust_num 
   from tmp_sale 
   where attribute_name_01='日配单' ) b on 1=1
  
;

INVALIDATE METADATA csx_tmp.sale_01;


select a.calquarter,
	classify_middle_code ,
    classify_middle_name,
    sales_value,
    profit,
    profit/    sales_value as profit_rate,
    sale_cust_num/all_sale_cust_num as stl,
    sale_cust_num,
    all_sale_cust_num
    from    
  (select 
  	calquarter,
  	classify_middle_code ,
    classify_middle_name,
    sum(sales_value)sales_value,
    sum(profit) profit,
    count(distinct case when sales_value>0 then  a.customer_no end ) as sale_cust_num
    from csx_tmp.sale_01 a 
    join
    (select calday,calquarter from csx_dw.dws_w_a_date_m d) d on a.sdt=d.calday
     where a. sale_group='日配单' 
      and channel in('1','7','9')
   	and sdt>='20200101'
   	and sdt<='20201226'
    -- and classify_middle_code IN ('B0102','B0202','B0304','B0601','B0603') 
    group by classify_middle_code ,
    classify_middle_name,
    calquarter
    )a
    left join 
   (select calquarter,count(distinct case when sales_value>0 then customer_no end ) as all_sale_cust_num 
   from csx_tmp.sale_01 a    
   join
    (select calday,calquarter from csx_dw.dws_w_a_date_m d) d on a.sdt=d.calday
   where sale_group='日配单'
   and channel in('1','7','9')
   and sdt>='20200101'
   and sdt<='20201226'
   group by calquarter ) b on 1=1 and a.calquarter=b.calquarter
  
;




select	classify_middle_code ,
    classify_middle_name,
    sales_value,
    profit,
    profit/    sales_value as profit_rate,
    sale_cust_num/all_sale_cust_num as stl,
    sale_cust_num,
    all_sale_cust_num
    from    
  (select 
  	classify_middle_code ,
    classify_middle_name,
    sum(sales_value)sales_value,
    sum(profit) profit,
    count(distinct case when sales_value>0 then  a.customer_no end ) as sale_cust_num
    from csx_tmp.sale_01 a 

     where a. sale_group='日配单' 
      and channel in('1','7','9')
   	and sdt>='20200101'
   	and sdt<='20201226'
    -- and classify_middle_code IN ('B0102','B0202','B0304','B0601','B0603') 
    group by classify_middle_code ,
    classify_middle_name
    )a
    left join 
   (select count(distinct case when sales_value>0 then customer_no end ) as all_sale_cust_num 
   from csx_tmp.sale_01 a    
  
   where sale_group='日配单'
   and channel in('1','7','9')
   and sdt>='20200101'
   and sdt<='20201226'
    ) b on 1=1 
  
;

with  tmp_sale as 
(select goods_code ,p.goods_name,
p.unit_name, 
sum(sales_cost) as sales_cost ,
sum(sales_value)sales_value ,
sum(profit)profit ,
sum(sales_qty)sales_qty 
from csx_tmp.sale_01 a  
join 
(select p.goods_id ,p.goods_name,p.unit_name from csx_dw.dws_basic_w_a_csx_product_m p where sdt='current') p on a.goods_code =p.goods_id 
  
   where channel in('1','7','9')
   and classify_middle_code ='B0602'
   and sale_group !='城市服务商'
   and sdt>='20200101'
   and sdt<='20201226'
 group by goods_code ,p.goods_name,p.unit_name
 )
 select goods_code ,
 goods_name,
 unit_name,
 sales_cost /sales_qty as  cost,
 sales_value /sales_qty as price,
 sales_cost,
sales_value ,
profit ,
profit/sales_value as profit_rate,
sales_qty ,
row_1 
from 
(
 select goods_code ,
 goods_name,
 unit_name,
 sales_cost ,
sales_value ,
profit ,
sales_qty ,
rank()over(order by sales_value desc) as row_1 
from tmp_sale
) a
where row_1<21;


