--1、销售额、毛利  2019年各省区各渠道销售额，合伙人单列

-- 清洁用品A05、干性杂货A03

select a.province_code,
    province_name,
    case when (a.channel_sale like'大%' or a.channel_sale like'企业购%') then 
    if((a.smonth>substr(d.break_date,1,6) or d.break_date is null),a.channel_sale,'合伙人')
    else a.channel_sale end channel_name,
    a.smonth,
    a.customer_no ,
    shop_name ,
    company_code ,
    company_name ,
    sales_belong_flag,
     goods_code ,
     goods_name,
     department_code ,
     department_name ,
     category_large_code,
     category_large_name,
    sum(a.sales_value) as sales_value,
    sum(excluding_tax_sales) as excluding_tax_sales,
    sum(a.profit) as profit,
    sum(a.excluding_tax_profit) as excluding_tax_profit     
from
    (select province_code ,
      province_name ,
      customer_no,
      case when channel in ('5','6') and a.customer_no like 'S%' then '商超' 
        when channel in ('5','6') and a.customer_no not like 'S%'then '大' 
        else channel_name 
      end channel_sale,
      goods_code ,
      department_code ,
      department_name ,
      substr(sdt,1,6) as smonth,
      sum(sales_value) as sales_value,
      sum(excluding_tax_sales) as excluding_tax_sales,
      sum(sales_cost) as sales_cost,
      sum(excluding_tax_cost) as excluding_tax_cost,
      sum(profit) as profit,
      sum(excluding_tax_profit) as excluding_tax_profit      
      from csx_dw.dws_sale_r_d_customer_sale a
      where sdt >= '20200101' and sdt < '20200701'
      -- and department_code in ('A03','A05')
      and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046') or order_no is null) 
     -- AND customer_no like 'S%' 
      and channel in('2','5','6')
    group by province_code ,
      goods_code ,
      department_code ,
      department_name ,
      customer_no,
      case when channel in ('5','6') and a.customer_no like 'S%' then '商超' 
        when channel in ('5','6') and a.customer_no not like 'S%'then '大' 
        else channel_name 
      end,
      substr(sdt,1,6) ,
      province_name
    )a
left join
    (select
    cust_group,
    customer_no,
    if((cust_group = '合伙人'
    or break_date = ''),
    '202006',
    break_date)break_date
from
    csx_tmp.partner_info
where
    sdt = '20200710')d on d.customer_no=a.customer_no 
left join 
(select f.goods_id ,
    f.goods_name ,
    f.category_large_code,
    f.category_large_name 
from csx_dw.dws_basic_w_a_csx_product_m f 
where sdt='current'
)e on a.goods_code=e.goods_id
left join 
(select CONCAT( 'S',f.shop_id) shop_id,
    f.shop_name ,
    f.company_code ,
    f.company_name ,
    sales_belong_flag
from csx_dw.dws_basic_w_a_csx_shop_m f 
where sdt='current'
)p on a.customer_no =p.shop_id
group by a.province_code,
    province_name,
    a.customer_no,
     case when (a.channel_sale like'大%' or a.channel_sale like'企业购%') then 
    if((a.smonth>substr(d.break_date,1,6) or d.break_date is null),a.channel_sale,'合伙人')
    else a.channel_sale end ,
    a.smonth,
     goods_code ,
     e.goods_name,
     department_code ,
     department_name ,
     e.category_large_code,
     category_large_name,
    shop_name ,
    company_code ,
    company_name,
sales_belong_flag;
     
 select * from csx_dw.dws_wms_r_d_entry_order_all_detail where   sdt>='20200101' and sdt<='20200131' and receive_location_code ='W0H4'
 and goods_code ='63110';

 select receive_location_code,goods_code,
    department_id,
    department_name,
    sum(receive_qty)qty,
    sum(amount)amount,
    sum(amount/(1+tax_rate/100)) as no_tax_amount
 from csx_dw.wms_entry_order 
 where sdt>='20200101' and sdt<='20200630'
 and department_id in ('A03','A05')
 -- and (entry_type_code like 'T%' or business_type like '调拨%')
 group by 
    receive_location_code,goods_code,
    department_id,
    department_name
    ;

