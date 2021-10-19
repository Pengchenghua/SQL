--B03肉禽水产销售&客户退货 20211019
select classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    goods_name,
    sales_value,
    sales_value/sum(sales_value)over(partition by classify_middle_code order by sales_value desc) aa,
    sales_qty,
    profit,
    return_amt,
    return_qty,
    sales_no,
    return_no
from (
select classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    goods_name,
    sum(sales_value)/10000 sales_value,
    sum(sales_qty) sales_qty,
    sum(profit)/10000 profit,
    sum(case when return_flag='X' then sales_value end )/10000 as return_amt,
    sum(case when return_flag='X' then sales_qty end ) as return_qty,
    count(distinct order_no ) as sales_no,
    count(distinct case when return_flag='X' then order_no end ) as return_no
from csx_dw.dws_sale_r_d_detail 
where sdt>='20210901'
    and sdt<='20210930'
    and business_type_code='1'
    and sales_type ='qyg'
    and classify_large_code ='B03'
group by classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    goods_name
   ) a 
order by classify_large_code,
    sales_value desc 
 ;
