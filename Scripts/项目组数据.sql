--销售数据 采购
select  SUBSTRING(sdt,1,6)as mon ,division_code ,division_name,department_code ,department_name ,category_large_code,category_large_name,
COUNT(DISTINCT goods_code )sale_sku,
sum(sales_value )sale,
sum(profit )profit
from csx_dw.dws_sale_r_d_customer_sale
where 
sdt>='20190101' and sdt<'20200608'
group by SUBSTRING(sdt,1,6) ,division_code ,division_name,department_code ,department_name ,category_large_code,category_large_name;

-- 销售数据 销售
select SUBSTRING(sdt,1,6) mon,province_code ,province_name,case when division_code in('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end as bd_id ,
sum(sales_value )/10000 sale ,sum(profit )/10000 profit 
from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200101' and sdt<'20200601' and channel ='1'
group by 
 SUBSTRING(sdt,1,6) ,province_code ,province_name,case when division_code in('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end 
;


-- 供应商入库
 select vendor_pur_lvl_name,
   dist_name,
 --  receive_location_code ,
   receive_location_name ,
     supplier_code ,
    supplier_name,
     sum(amount) amt,
    SUBSTRING(sdt, 1, 4) yy,
    SUBSTRING(sdt, 1, 6)mon,
   -- province_code,
  
   -- division_code ,
    division_name,
   -- department_id ,
    department_name,
   -- category_large_code ,
    category_large_name ,
   -- category_middle_code ,
    category_middle_name ,
   -- category_small_code ,
    category_small_name ,
    COUNT(DISTINCT goods_code )sku
   
from
    csx_dw.wms_entry_order a
join (
    select
        location_code, dist_code, dist_name
    from
        csx_dw.csx_shop
    where
        sdt = 'current')b on
    a.receive_location_code = b.location_code
    join 
    (select vendor_id,vendor_pur_lvl_name from dws_basic_w_a_csx_supplier_m where  sdt='20200608') c on a.supplier_code =c.vendor_id
where
    sdt >= '20190101'
    and sdt<'20200608'
    AND business_type in ('直送', '供应商配送', '采购入库(old)')
group by vendor_pur_lvl_name,
    SUBSTRING(sdt, 1, 4) ,
    SUBSTRING(sdt, 1, 6),
    receive_location_code,
    receive_location_name,
    dist_code, dist_name,
    supplier_code ,
    supplier_name,
    division_code ,
    division_name,
    department_id ,
    department_name,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    category_small_code ,
    category_small_name;


-- 商品销售

--类型    商品年份    商品月 1级品类    2级品类    3级品类    4级品类    5级品类    销售SKU   区域  销售量 销售额 销售额%    月末库存额   月末库存额%  库存周转天数  毛利  毛利率
--customer_type 01长期；02临时
 select
    channel ,
    `attribute` ,
   case when  customer_type='01' then '长期' when customer_type='02'then '临时' else customer_type end cust_type,
    channel_name,
    customer_name ,
    yyyy,
    a.mon ,
    division_name ,
    department_name,
    category_large_name,
    category_middle_name ,
    category_small_name ,
    sum(sale_sku) as sale_sku,
    province_name ,
    sum(qty) qty,
    sum(sale) sale,
    sum(profit) profit ,
    sum(profit)/ sum(sale) as profit_rate,
    sum(end_inv_amt)end_inv_amt,
    sum(inventory_amt)/ sum(cost) days_trun
from
    (select
    cm.channel , cm.`attribute` , customer_type, cm.customer_name,
    channel_name,
    a.customer_no ,
    substring(sdt, 1, 4) as yyyy,
    substring(sdt, 1, 6) as mon ,
    division_name ,
    department_name,
    category_large_name,
    category_middle_name ,
    category_small_name ,
    COUNT(DISTINCT goods_code )as sale_sku,
    province_name ,
    sum(sales_qty) qty,
    sum(sales_value) sale,
    sum(profit) profit ,
    dc_code
from
    csx_dw.dws_sale_r_d_customer_sale as a
 join (
select
    customer_no , cm.channel , `attribute` , customer_type, customer_name
from
    csx_dw.dws_crm_w_a_customer_m as cm
where
    sdt = '20200609')as cm on
a.customer_no = cm.customer_no
where
sdt >= '20190101'
and sdt<'20200608'
group by
channel_name,cm.channel , cm.`attribute` , customer_type, cm.customer_name,a.customer_no ,
substring(sdt, 1, 4) ,
substring(sdt, 1, 6) ,
division_name ,
department_name,
category_large_name,
category_middle_name ,
category_small_name ,
province_name ,
dc_code) a
left join (
    select
        substring(sdt, 1, 6)as mon, dc_code, sum(sales_cost) cost, sum(inventory_amt) inventory_amt , sum(case when sdt = regexp_replace(to_date(last_day(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'))), '-', '') then inventory_amt end) as end_inv_amt
    from
        csx_dw.dc_sale_inventory
    where
        sdt >= '20190101'
        and sdt<'20200608'
    group by
        substring(sdt, 1, 6), dc_code) b on
    a.dc_code = b.dc_code
    and a.mon = b.mon
group by
    channel_name,
    channel ,
    yyyy,
    mon ,
    division_name ,
   -- a.customer_no,
    department_name,
    category_large_name,
    category_middle_name ,
    category_small_name ,
    province_name,
    channel ,
    `attribute` ,
     case when  customer_type='01' then '长期' when customer_type='02'then '临时' else customer_type end,
    channel_name,
    customer_name
