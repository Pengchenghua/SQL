SELECT * FROM csx_dw.dws_crm_w_a_customer_m  where sdt='20200505' and is_parter ='是';
select a.customer_no,sale from 
(select * from csx_dw.csx_partner_list where sdt='202005' )a join 
(select customer_no,sum(sales_value)sale from csx_dw.dws_sale_r_d_customer_sale  where sdt>='20200401' and sdt<='20200430'
and channel in ('1','7')
group by customer_no) b
on a.customer_no=b.customer_no;


select a.customer_no,sale from 
(select * from csx_dw.csx_partner_list where sdt='202005' )a join 
(select customer_no,sum(sales_value)sale from csx_dw.dws_sale_r_d_customer_sale  where sdt>='20200401' and sdt<='20200430'
and channel in ('1','7')
group by customer_no) b
on a.customer_no=b.customer_no;


-- 省区top30
select
    province_code ,
    province_name,
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name ,
    goods_code ,
    goods_name ,
    sales_qty,
    sale,
    rank_num,
    profit 
    from (
select
    province_code ,
    province_name,
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name ,
    goods_code ,
    goods_name ,
    sum(sales_qty)sales_qty,
    sum(sales_value )sale,
    rank()over(partition by  province_code ,
    province_name,
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name 
    order by sum(sales_value )desc) as rank_num,
    sum(profit )profit 
from
    csx_dw.dws_sale_r_d_customer_sale a 
    left join 
    (select DISTINCT customer_no from csx_dw.csx_partner_list where sdt='202005' ) b on  
    a.customer_no =b.customer_no
where
    b.customer_no is null
    and sdt >= '20190101'
    and sdt <= '20190430'
    and a.channel in ('1','7')
    group by  province_code ,
    province_name,
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name ,
    goods_code ,
    goods_name ) a where rank_num<31 order by province_code ,
    province_name,
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name ,
    rank_num;
    


-- 全国top30
select
  
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name ,
    goods_code ,
    goods_name ,
    sales_qty,
    sale,
    rank_num,
    profit 
    from (
select
   
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name ,
    goods_code ,
    goods_name ,
    sum(sales_qty)sales_qty,
    sum(sales_value )sale,
    rank()over(partition by  
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name 
    order by sum(sales_value )desc) as rank_num,
    sum(profit )profit 
from
    csx_dw.dws_sale_r_d_customer_sale a 
    left join 
    (select DISTINCT customer_no from csx_dw.csx_partner_list where sdt='202005' ) b on  
    a.customer_no =b.customer_no
where
    b.customer_no is null
    and sdt >= '20200101'
    and sdt <= '20200430'
    and a.channel in ('1','7')
    group by  
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name ,
    goods_code ,
    goods_name ) a where rank_num<31 order by 
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name ,
    rank_num;


select
    a.customer_no,
    customer_name ,
    b.customer_no,
    sum(sales_value)
    from csx_dw.dws_sale_r_d_customer_sale a 
    left join (select DISTINCT customer_no from csx_dw.csx_partner_list where sdt='202005' ) b on a.customer_no=b.customer_no
where
    sdt >= '20190101'
    and sdt <= '20190430'
    and goods_code = '967510'
    and province_code = '1'
    and channel in ('1',
    '7')
    group by a.customer_no,
    customer_name ,
    b.customer_no;
    


-- 入库标识合伙人供应商
 select
    mon,
    source_type,
       case when source_type=1  then '1_采购导入'
        when source_type=2 then '2_直送客户'
        when source_type=3 then '3_键代发'
        when source_type=4 then '4_项目合伙人'
        when source_type=5 then '5_无单入库'
        when source_type=6 then '6_寄售调拨'
        when source_type=7 then '7_自营调拨'
        when source_type=8 then '8_云超采购'
        when source_type=9 then '9_工厂采购'
         end as source_name,
    dist_code,
    dist_name,
    receive_location_code ,
    receive_location_name,
    entry_type ,
    business_type ,
    a.supplier_code ,
    supplier_name ,
    if(b.supplier_code is null , '否', '是') as note,
    goods_code ,
    goods_name ,
    division_code ,
    division_name ,
    department_id ,
    department_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    category_small_code ,
    category_small_name ,
    qty,
    amt
from
    (select
    substr(sdt, 1, 6)mon,
    source_type,
    receive_location_code ,
    receive_location_name,
    entry_type ,
    business_type ,
    supplier_code ,
    supplier_name ,
    goods_code ,
    goods_name ,
    division_code ,
    division_name ,
    department_id ,
    department_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    category_small_code ,
    category_small_name ,
    sum(receive_qty)qty,
    sum(price*receive_qty) amt
from
    csx_dw.wms_entry_order a 
    left join 
(select received_order_code ,source_type from csx_ods.source_scm_r_d_scm_order_header where sdt>='20200101' ) b on a.order_code=b.received_order_code
where
    sdt >= '20200101'
    and (entry_type like '采购入库'
    or business_type like '%采购入库%')
group by
    receive_location_code ,
    receive_location_name,
    supplier_code ,
    supplier_name ,
    source_type,
    goods_code ,
    goods_name ,
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    category_small_code ,
    category_small_name ,
    entry_type ,
    business_type,
    department_id ,
    department_name ,
    substr(sdt, 1, 6))a
left join (
    select
        supplier_code1 as supplier_code
    from
        csx_dw.csx_partner_list
    where
        sdt = '202005'
union all
    select
        supplier_code2 as supplier_code
    from
        csx_dw.csx_partner_list
    where
        sdt = '202005'
union all
    select
        supplier_code3 as supplier_code
    from
        csx_dw.csx_partner_list
    where
        sdt = '202005')b on
    a.supplier_code = b.supplier_code
left join (
    select
        location_code, province_code , province_name , dist_code , dist_name
    from
        csx_dw.csx_shop
    where
        sdt = 'current')c on
    a.receive_location_code = c.location_code;
    


select channel_name ,
   province_code,
   province_name,
    division_code ,
    division_name ,
    department_code ,
    department_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    category_small_code ,
    category_small_name ,
    goods_code ,
    goods_name ,
    sum(sales_qty)sales_qty,
    sum(sales_value )sale,
    rank()over(partition by  
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name 
    order by sum(sales_value )desc) as rank_num,
    sum(profit )profit 
from
    csx_dw.dws_sale_r_d_customer_sale a 
    left join 
    (select DISTINCT customer_no from csx_dw.csx_partner_list where sdt='202005' ) b on  
    a.customer_no =b.customer_no
where
    b.customer_no is null
    and sdt >= '20200601'
    and sdt <= '20200602'
    and a.channel in ('1')
    and a.province_name like '%重庆%'
    group by  
    province_code,
    province_name,
    division_code ,
    division_name ,
     category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    category_small_code ,
    category_small_name ,
    goods_code ,
    goods_name ,
    department_code ,
    department_name ,channel_name;
    
select * from csx_dw.wms_entry_order where receive_location_code ='W0H2' and supplier_code ='20029532' and sdt>='20200201';
 where receive_location_code ='W0H2' and supplier_code ='20029532' ;
 
 
 select  *
    from   csx_dw.ads_sale_r_m_dept_sale_mon_report  where sdt='20200415';
    select channel_name,customer_no ,customer_name ,sum(sales_value ) from csx_dw.dws_sale_r_d_customer_sale 
    where sdt>='20200401' and sdt<='20200430' and province_code ='1' 
    --and channel in('1','7') 
    and dc_code='W0A3' GROUP  by channel_name,customer_no ,customer_name ;
    

select channel_name ,
   province_code,
   province_name,
   a.customer_no ,
   customer_name ,
   first_category ,
   second_category ,
    sum(sales_qty)sales_qty,
    sum(sales_value )sale,
    sum(profit )profit ,
    count(DISTINCT goods_code ) as sale_sku
from
    csx_dw.dws_sale_r_d_customer_sale a 
     join 
    (select DISTINCT customer_no from csx_dw.csx_partner_list where sdt='202005' ) b on  
    a.customer_no =b.customer_no
    and sdt >= '20200601'
    and sdt <= '20200602'
    group by  
    channel_name ,
   province_code,
   province_name,
   a.customer_no ,
   customer_name ,
   first_category ,
   second_category ;