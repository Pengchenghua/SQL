select
    sdt,
    location_code,
    product_code ,
    sum(cas_amt),
    sum(loss_amt) as loss_amt,
    sum(cas_amt-loss_amt) diff_amt
from
    (
    select
        regexp_replace(to_date(posting_time) ,'-','')as sdt,
        location_code,
        product_code ,
        sum(amt_no_tax)as cas_amt,
        0 loss_amt
    from
        csx_dw.dwd_cas_r_d_accounting_credential_item
    where
        sdt >= '20200701'
        and sdt <= '20200712'
        and move_type = '117A'
        and location_code = 'W0M6'
        and product_code in ('608', '456')
    group by
        location_code,  regexp_replace(to_date(posting_time) ,'-',''), product_code
union all
    select
        regexp_replace(to_date(posting_time) ,'-','')as sdt,
        location_code ,
        product_code,
        0 as cas_amt,
        sum(case when move_type = '117A' then amt_no_tax end)loss_amt
    from
        csx_dw.dwd_cas_r_d_accounting_stock_detail a
    where
        sdt >= '20200701'
        and sdt <= '20200712'
        and posting_time <'2020-07-13 00:00:00.0'
        and location_code = 'W0M6'
        and product_code in ('608', '456')
    group by
        location_code,  regexp_replace(to_date(posting_time) ,'-','') , product_code) a
group by
    location_code,
    sdt,
    product_code ;
 
refresh  csx_dw.dwd_cas_r_d_accounting_stock_detail ;
refresh  csx_dw.dwd_cas_r_d_accounting_credential_item;
select sdt,location_code,move_type ,product_code,sum( amt_no_tax  )loss_amt from  csx_dw.dwd_cas_r_d_accounting_stock_detail a 
    where
        sdt >= '20200701'
        and sdt <= '20200712'
        and posting_time <'2020-07-13 00:00:00.0' and location_code ='W0M6' and product_code in ('608','456')
        and move_type ='117A'
        group by location_code,sdt,move_type ,product_code;
 
    select * from csx_dw.dws_sale_r_d_customer_sale  where goods_code ='270380' and sdt>='20200701' and sdt<='20200712' and channel_name ='大';
 
    select
       move_name ,move_type ,sum(amt_no_tax ),direction
    from
        csx_dw.dwd_cas_r_d_accounting_credential_item
    where
        sdt >= '20200701'
        and sdt <= '20200712'
    group by move_name ,move_type,direction;
  select * from  csx_ods.source_bbc_w_a_wshop_goods_factory where sdt ='20200711';
  select * from csx_dw.dws_basic_w_a_csx_product_info where product_code ='C894681';
  select * from csx_data_center_table_manage cdctm WHERE hive_table_name=hive_table_name ;
  
select distinct 
    au.sys_province_id as id,
    au.sys_province_name as name
from
    csx_b2b_data_center.da_auth da
left join csx_b2b_data_center.da_sale_province au on
    da.da_permission_id = au.sys_province_id
where
    da.is_able = 1
    and da.da_type_id = -2
    and da.is_deleted = 0
    and au.is_able = 1
    and au.is_deleted = 0
    and au.sys_province_id not in ('-100', '16', '999')
    order by sys_province_id;
    
select * from  csx_dw.wms_shipped_order where send_sdt='20200713' and source_system ='BBC' and order_no ='0300687175';
select * from csx_dw.dws_wms_r_d_accounting_stock_m where sdt='20200713' and dc_code ='W0B6' and reservoir_area_code ='TH01' AND qty!=0;

select DISTINCT dist_code ,dist_name,zone_id from csx_dw.csx_shop where sdt='current' and zone_name like '华西%';

select a.sdt,a.zone_id,a.zone_name,a.dist_code,dist_name,plan_sale ,coalesce(sales_value,0)sales_value,
 coalesce(profit,0)profit
from 
(select sdt,a.dist_code,
        dist_name,
        a.zone_id,
        zone_name ,plan_sale from  csx_tmp.temp_plan_sale_huaxi a  
join 
(select distinct
        dist_code,
        dist_name,
        zone_id,
        zone_name
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and table_type = 1
        and zone_id = '3') b on a.dist_code =b.dist_code) a

left join
(
select
    sdt,
    province_code ,
    province_name,
    sum(sales_value) sales_value ,
    sum(profit) profit
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >= '20200701'
    and order_kind != 'WELFARE'
    and attribute_code = 1
group by
    sdt,
    province_code ,
    province_name) as b on a.sdt=b.sdt and a.dist_code=b.province_code
 ;
 
 
select from_unixtime(unix_timestamp(a.sdt,'yyyyMMdd'),'yyyy-MM-dd') as sdt,
 a.zone_id,
 a.zone_name,
 a.dist_code,
 dist_name,
 plan_sale ,
 coalesce(sales_value/10000,0) as sales_value,
 coalesce(profit/10000,0)as profit,
 coalesce(profit/sales_value,0) as profit_rate,
 coalesce(round(sales_value/10000/plan_sale,4),0) as sale_fill_rate
from 
(select sdt,
       a.dist_code,
        dist_name,
        a.zone_id,
        zone_name ,plan_sale from  csx_tmp.temp_plan_sale_huaxi a  
join 
(select distinct
        dist_code,
        dist_name,
        zone_id,
        zone_name
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and table_type = 1
        and zone_id = '3') b on a.dist_code =b.dist_code) a

left join
(
select
    sdt,
    province_code ,
    province_name,
    sum(sales_value) sales_value ,
    sum(profit) profit
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >= '20200701'
    and order_kind != 'WELFARE'
    and attribute_code = 1
group by
    sdt,
    province_code ,
    province_name) as b on a.sdt=b.sdt and a.dist_code=b.province_code
    order by a.sdt desc
 ;
 select * from csx_dw.csx_shop where sdt='current' ;
 ;
 select
    supplier_code ,supplier_name,vat_regist_num,prefecture_city_name ,sum(receive_amt )receive_amt ,sum(receive_qty )receive_qty 
from
    csx_dw.ads_supply_order_flow a  
 join 
 (select location_code,shop_name,dist_code,dist_name,prefecture_city_name,purpose 
    from csx_dw.csx_shop 
    where sdt='current' 
        and dist_code in ('2','10')
        and prefecture_city_name !='南京市'
 -- and location_code IN('W0A5','W0N1')
 -- and location_uses_code ='01'
 )b on a.location_code =b.location_code
LEFT join 
 (select vendor_id,vat_regist_num from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') c on a.supplier_code =c.vendor_id
where
    sdt >= '20200501'
    and sdt <= '20200714'
    and receive_close_date >='20200601' and receive_close_date <='20200714'
    and source_type = 1
  --  and a.location_code = 'W0A3'
    and receive_type_code = 'P01'
    and receive_business_type_code in('01','03');

select
       location_code ,sdt,
       sum(case when (move_type ='117A' and category_code in ('10','11')) then amt_no_tax*(1+tax_rate*100) 
                when (move_type ='117B' and category_code in ('10','11')) then amt_no_tax*(1+tax_rate*100)*-1 end ) as fresh_loss_amt, -- 生鲜报损金额
       sum(case when (move_type ='117A' and category_code in ('12','13','14')) then amt_no_tax*(1+tax_rate*100)  
                when (move_type ='117B' and category_code in ('12','13','14')) then amt_no_tax*(1+tax_rate*100)*-1 end ) as foods_loss_amt, -- 食百报损金额
       sum(case when move_type ='117A'then amt_no_tax*(1+tax_rate*100)  when  move_type ='117B' then amt_no_tax*(1+tax_rate*100)*-1 end ) as loss_amt, -- 生鲜报损金额       
       sum(case when move_type in('111A') and category_code in ('10','11')  then amt_no_tax*(1+tax_rate*100)  end ) as fresh_inventory_profit, -- 生鲜盘盈（生鲜取未过帐）
       sum(case when move_type in('111A') and category_code in ('12','13','14')  then amt_no_tax*(1+tax_rate*100)  end ) as foods_inventory_profit, -- 盘盈金额(食百取未过帐)
       sum(case when move_type in('110A') and category_code in ('10','11')  then amt_no_tax*(1+tax_rate*100)  end ) as fresh_inventory_loss, -- 盘亏金额(生鲜取未过帐)
       sum(case when move_type in('110A') and category_code in ('12','13','14') then amt_no_tax*(1+tax_rate*100)  end ) as foods_inventory_loss -- 食百盘亏金额(生鲜取未过帐、食百取未过帐)
from
        csx_dw.dwd_cas_r_d_accounting_credential_item
    where
         sdt='20200715' 
         and location_code='W0H2'
  --  and sdt<='20200723'
    group by location_code,sdt;

select *
from
        csx_dw.dwd_cas_r_d_accounting_credential_item
    where
         sdt='20200715' 
         and location_code='W0H2'
         and move_type in('110A','111A');

     select *
from
        csx_dw.dwd_cas_r_d_accounting_credential_item
    where
         sdt='20200715' 
         and to_date(posting_time )='2020-07-15'
         and location_code='W0H2'
         and move_type in('110A')
     and reservoir_area_code in ('PD01','PD02')
 --and product_code ='914656'
 ;

select sum(sales_value) from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200601' and sdt<='20200630'
and channel ='1';
select * from csx_ods.source_wms_r_d_bills_config where sdt='20200715' ;
 
 select
    receive_business_type ,supplier_code ,supplier_name,shipped_location_code ,
    shipped_location_name ,
    sum(receive_amt )receive_amt ,
    sum(receive_qty )receive_qty 
from
    csx_dw.ads_supply_order_flow a 
where
    sdt >= '20200501'
    and sdt <= '20200714'
    and receive_close_date >='20200601' and receive_close_date <='20200714'
   -- and source_type = 1
    and a.location_code = 'W0N1'
  --  and receive_type_code = 'P01'
  --  and receive_business_type_code ='01'
group by supplier_code ,supplier_name,receive_business_type,shipped_location_code ,shipped_location_name
;

select
   supplier_name,location_code ,category_code ,category_name ,goods_code ,goods_name ,sum(receive_amt )receive_amt 
from
    csx_dw.ads_supply_order_flow a 
where
    sdt >= '20200501'
    and sdt <= '20200714'
    and receive_close_date >='20200601' and receive_close_date <='20200714'
    and source_type = 1
  --  and a.location_code = 'W0N1'
    and supplier_name like '昆山汇和轩食品有限公司%'
   group by  supplier_name,
        location_code ,
        category_code ,
        category_name ,
        goods_code ,
        goods_name
 --   and receive_business_type ='申偿入库'
  --  and receive_type_code = 'P01'
  --  and receive_business_type_code ='01'
;

refresh csx_dw.ads_wms_r_d_pd_detail_days
;
select * from csx_dw.ads_wms_r_d_bs_detail_days 
where sdt='20200715' ;
select * from csx_dw.ads_wms_r_d_pd_detail_days 
where sdt='20200715' ;

select *
 from csx_ods.source_wms_w_d_wms_product_stock_log  
 where sdt='19990101' 
    and regexp_replace(to_date(update_time ),'-','')>= '20200701'
    and regexp_replace(to_date(update_time ),'-','')<='20200716'
    and task_type ='04'
    ;
   -- and warehouse_code='W0B6'
group by 
    warehouse_code ,
    create_by ,
    sdt
;

 select 
     *
 from 
  csx_dw.dws_sale_r_d_customer_sale
 where 
  sdt>='20200630'
        and 
        customer_no='110015'
        ;

select coalesce* from csx_tmp.ads_bbc_r_d_document_stat;
select * from csx_dw.wms_shipped_order where sdt ='20200716' and shipped_location_code ='W0B6';
select * 
  from
        csx_dw.provinces_attribute_sale 
    where
        sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') ;
        
   select * from  
        csx_dw.provinces_kanban 
     where
        sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') ; 
 
    select
        *
        from
        csx_dw.customer_sales
        where sdt='20200722';
        
    select region_code ,province,province_code from csx_ods.source_crm_w_a_sys_province where province='安徽省';
    
select location_code,concat(location_code,'_',shop_name) as full_name,location_type,location_type_code from csx_dw.csx_shop where sdt='current' and table_type=1 
-- and dist_code ='${layer1}'
-- and location_type in('仓库')
order by location_code;

select
    location_code,
    concat(location_code, '_', shop_name) as full_name
from
    csx_dw.csx_shop
where
    sdt = 'current'
    and table_type = 1
    and dist_code = '"+$layer1+"'
    and location_type_code in('"+$dctype+"');


-- 类型销售占比 
 select
    case
        when attribute_name = '' then '其他'
        else attribute_name
    end as attribute,
    sale / 10000 as sale ,
    profit / 10000 as profit,
    profit_rate,
    cust_num,
    ring_sale / 10000 as ring_sale,
    ring_profit / 10000 as ring_profit ,
    ring_profit_rate,
    ring_cust_num ,
    diff_cust_num,
    mom_sale_ratio,
    sale_ratio
from
    csx_dw.provinces_attribute_sale
where
    sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 1)), '-', '')
    and province_code = '1'
    and channel_name = '大'
order by
    attribute desc;
        
    select attribute_name,sum(sales_value ) from csx_dw.customer_sales where sdt>='20200701' and province_code ='1'
    group by attribute_name;

select type,division_code ,division_name , avg(sku ) avg_sku,sum(sale)sale from (
select case when channel in ('1','7') then '大' else channel_name end type,sdt,division_code ,division_name , count(distinct goods_code ) as sku,sum(sales_value )sale
from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200601' and sdt<'20200724'
group by division_code ,division_name ,sdt,case when channel in ('1','7') then '大' else channel_name end
)a group by type,division_code ,division_name 

;

select
    concat("['",dist_code,"','",location_code,"']")
from
    csx_dw.csx_shop
where
    sdt = 'current'
    and table_type = 1
    
order by
    location_code;

select concat("['",dist_code,"','",location_code,"']" ) from csx_dw.csx_shop where sdt='current'    ;

select * from csx_dw.provinces_kanban_cust where sdt='20200723';

SELECT zone_id,
       zone_name,
       province_code,
       province_name,
       attribute_name,
       sale,
       ring_sale,
       sale_ratio,
       profit,
       profit_rate,
       cust_num,
       diff_cust_num
FROM csx_dw.provinces_attribute_sale a
JOIN
  (SELECT dist_code,
       dist_name,
       zone_id,
       zone_name
    FROM csx_dw.csx_shop
    WHERE sdt='current'
      AND table_type=1
      and zone_id='3'
    GROUP BY dist_code,
         dist_name,
         zone_id,
         zone_name) b ON a.province_code=b.dist_code
WHERE sdt='20200723'
  AND channel_name ='大' ;
  
  select * from csx_dw.csx_shop where sdt='current';
  
    select
        province_code,
        sales_name,
        customer_no,
        customer_name,
        note,
        cust_num,
        sale,
        profit,
        prorate,
        desc_rank,
        ratio,
         regexp_replace(sign_date,'-','')sign_date 
    from
        csx_dw.provinces_kanban_cust_lose a  
    where
        profit>0 
        and  province_code in ('32','26','23')
        and type='up' 
        and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') 
   and desc_rank<11;
   
select receive_location_code,sdt,
       sum(case when business_type_code ='R21' then amount end )as return_city_amt ,
       sum(case when business_type_code ='R22' then amount end )as return_express_amt , 
       sum(case when business_type_code ='R20' then amount end )as return_pick_amt ,
       sum(case when business_type_code ='R73' then amount end )as return_wholesale_amt ,
       sum(case when business_type_code ='71' then amount end )as return_noorder_amt
from csx_dw.wms_entry_order 
where business_type_code in ('R21','R22','71','R20','R73') 
    and   sdt>='20200701'
    and sdt<='20200722'
     and receive_location_code ='W0K8'
group by receive_location_code,sdt
;
select * from csx_dw.wms_entry_order 
where  business_type_code in ('R21','R22','71','R20','R73') 
    and   sdt>='20200701'
    and sdt<='20200722'
     and receive_location_code ='W0K8';
 
 select sales_region,a.province_code ,a.province_name ,sales_name ,customer_name ,
 sale,
 profit ,
 profit_rate,
 sale_sdt,
 sign_date 
 from (
 select sales_region,a.province_code ,a.province_name ,sales_name ,customer_no ,customer_name ,sum(sales_value )/10000 sale,sum(profit )/10000 profit ,
 sum(profit)/sum(sales_value ) as profit_rate,
 count(distinct sdt) as sale_sdt,
 sign_date 
 from csx_dw.dws_sale_r_d_customer_sale a
 join 
 (select * from 
csx_ods.source_crm_w_a_sys_province
) b on a.province_code =b.province_code
 where sdt>='20200701' and sdt<='20200723' 
 and channel in ('1','7')
 group by sign_date,
 sales_region,a.province_code ,a.province_name ,sales_name ,customer_no ,customer_name
 ) a where sales_region ='HuaXi'
 order by sale desc
 limit 10
 ;
 
 SELECT purchase_org_code,
          purchase_org_name,
          region_code,
          region_name
   FROM csx_ods.source_basic_w_a_base_purchase_org_info
   WHERE sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','');
   
select location_code,province_code,province_name, region_code ,case when region_name='' then '' else concat(region_name,'大区') end region_name from 
csx_dw.dim_area
where area_rank=13;

select * from 
csx_ods.source_crm_w_a_sys_province
;
select 
 
select shipped_location_code,send_sdt as sdt,
   -- count(distinct case when business_type_code !='73' then  order_no end )order_num,
    -- count(distinct case when business_type_code !='73' then goods_code end)as shipped_sku,
    sum(case when business_type_code !='73' then order_shipped_qty end)as shipped_qty ,
    sum(case when (business_type_code !='73' and plan_qty != order_shipped_qty) then (plan_qty-coalesce(order_shipped_qty,0)) end ) as diff_shipped_qty,  --  缺货数据
   -- count(distinct  case when (plan_qty != order_shipped_qty and business_type_code !='73') then goods_code  end )as stock_out_sku, -- 缺货SKU
    sum(case when (business_type_code !='73'and plan_qty != order_shipped_qty) then (coalesce(plan_qty*price,0) -coalesce(amount,0) )end ) as diff_shipped_amt, --缺货金额
    sum(case when business_type_code ='22' then amount end ) as bbc_express_amt, -- 快递配出库额
    sum(case when business_type_code ='21' then amount end ) as bbc_city_amt, -- 同城配
    sum(case when business_type_code ='20' then amount end ) as bbc_pick_amt, -- 自提
    sum(case when business_type_code ='73' then amount end ) as bbc_wholesale_amt -- 一件代发
from csx_dw.wms_shipped_order
where   send_sdt>='20200701'
    and send_sdt<='20200722'
-- and source_system ='BBC'
and shipped_location_code ='W0K8'
group by shipped_location_code,send_sdt 
;



SELECT a.sales_region,
     --  province_name,
       a.channel,
       a.department_code,
       a.department_name,
       qty,
       sale,
       ring_sale,
       coalesce(sale/ring_sale,0) ring_sale_rate,
       profit,
       profit_rate,
       avg_sale,
       sale_sku,
       sale_cust,
       all_sale_cust,
       sale_cust/all_sale_cust AS cust_penetration,
       row_number()over(partition BY a.sales_region  ORDER BY sale DESC) AS row_no
       
FROM
  (SELECT sales_region,
          channel,
          department_code,
          department_name,
          sum(sales_qty)qty,
          sum(sales_value)sale,
          sum(profit)profit,
          coalesce(sum(profit)/sum(sales_value),0) AS profit_rate,
          coalesce(sum(sales_value)/datediff(from_unixtime(unix_timestamp('20200723','yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp('20200701','yyyyMMdd'),'yyyy-MM-dd')),0) AS avg_sale,
          count(DISTINCT goods_code) sale_sku,
        0 sale_cust
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
 (select * from 
csx_ods.source_crm_w_a_sys_province
) b on a.province_code =b.province_code 
   WHERE sdt>= '20200701'
     AND sdt<='20200723'
   --  AND province_code='1'
     AND channel IN ('1','7')
   GROUP BY sales_region,
            department_code,
            department_name,
            channel
    ) a 
    left join 
   ( SELECT sales_region,
        --  province_name,
          channel,
          department_code,
          department_name,
           sum(sales_value) as ring_sale
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
 (select * from 
csx_ods.source_crm_w_a_sys_province
) b on a.province_code =b.province_code
   WHERE sdt>= '20200601'
     AND sdt<='20200623'
   --  AND province_code='1'
     AND channel IN ('1',
                     '7')
   GROUP BY sales_region,
            department_code,
            department_name,
            channel
    )b on a.sales_region=b.sales_region and a.department_code=b.department_code and a.channel=b.channel
LEFT JOIN
  (SELECT sales_region,
          count(DISTINCT customer_no) all_sale_cust
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
 (select * from 
csx_ods.source_crm_w_a_sys_province
) b on a.province_code =b.province_code
   WHERE sdt>='20200701'
     AND sdt<='20200723'
   --  AND province_code='1'
     AND channel IN ('1','7')
   GROUP BY sales_region) c ON a.sales_region=c.sales_region;

   
   SELECT 
       province_code ,
       province_name ,
       attribute,
       attribute_code,
       sum(sales_value )sale,
       sum(profit )profit,
       count(distinct a.customer_no )as sale_cust
   FROM csx_dw.customer_sales a 
   join 
   (select
    customer_no ,
    attribute ,
    attribute_code
    from
    csx_dw.dws_crm_w_a_customer_m_v1
    where
    sdt = 'current') as b on a.customer_no =b.customer_no
   where sdt>='20200701' and sdt<='20200724' and a.channel in('1','7')
   group by attribute,
       attribute_code,
       province_code,
     province_name
 union all 
   SELECT 
       province_code ,
       province_name ,
       attribute,
       attribute_code,
       sum(sales_value )sale,
       sum(profit )profit,
       count(distinct a.customer_no )as sale_cust
   FROM csx_dw.customer_sales a 
   join 
   (select
    customer_no ,
    attribute ,
    attribute_code
    from
    csx_dw.dws_crm_w_a_customer_m_v1
    where
    sdt = 'current') as b on a.customer_no =b.customer_no
   where sdt>='20200701' and sdt<='20200724' and a.channel in('1','7')
   group by attribute,
       attribute_code,
       province_code,
     province_name;
   
 
-- 属性销售  
select  
       zone_id,zone_name ,
       province_code ,
       province_name ,
       attribute,
       attribute_code,
       sum(days_sale/10000 )as days_sale,
       sum(days_profit/10000) as days_profit,
       sum(days_profit)/sum(days_sale) as days_profit_rate,
       sum(sale/10000 )sale,
        sum(ring_sale/10000 ) as ring_sale,
        sum(sale/10000 )/sum(ring_sale/10000 )-1 as ring_sale_ratio,
       sum(profit/10000)profit,
       sum(profit)/sum(sale) as profit_rate,
       sum(sale_cust )as sale_cust,
       sum(sale_cust-ring_sale_cust) as diff_sale_cust,
       sum(ring_profit/10000) as ring_profit,
       sum(ring_sale_cust) as ring_sale_cust
from (
   SELECT 
       province_code ,
       province_name ,
       attribute,
       attribute_code,
       sum(case when sdt='${edate}' then sales_value end )as days_sale,
       sum(case when sdt='${edate}'then profit end) as days_profit,
       sum(sales_value )sale,
       sum(profit )profit,
       count(distinct a.customer_no )as sale_cust,
       0 as ring_sale,
       0 as ring_profit,
       0 as ring_sale_cust
   FROM csx_dw.customer_sales a 
   join 
   (select
    customer_no ,
    attribute ,
    attribute_code
    from
    csx_dw.dws_crm_w_a_customer_m_v1
    where
    sdt = 'current') as b on a.customer_no =b.customer_no
   where sdt>='${sdate}' and sdt<='${edate}' and a.channel in('1','7')
   group by attribute,
       attribute_code,
       province_code,
     province_name
 union all 
   SELECT 
       province_code ,
       province_name ,
       attribute,
       attribute_code,
       0 as days_sale,
       0 as days_profit,
       0 as sale,
       0 as profit,
       0 as sale_cust,
       sum(sales_value)as ring_sale,
       sum(profit)as ring_profit,
       count(distinct a.customer_no)as ring_sale_cust       
   FROM csx_dw.customer_sales a 
   join 
   (select
    customer_no ,
    attribute ,
    attribute_code
    from
    csx_dw.dws_crm_w_a_customer_m_v1
    where
    sdt = 'current') as b on a.customer_no =b.customer_no
   where sdt>='${l_sdate}' and sdt<='${l_edate}' and a.channel in('1','7')
   group by attribute,
       attribute_code,
       province_code,
     province_name
) a 
join 
(select distinct   dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) b on a.province_code=b.dist_code 
group by zone_id,zone_name ,
        province_code ,
       province_name ,
       attribute,
       attribute_code
;
   
-- 部类课组销售
select
    province_code ,
    province_name ,
    division_code ,
    division_name,
    department_code ,
    department_name,
    sum(days_sale)as days_sale,
    sum(days_profit) as days_profit,
    sum(days_profit)/ sum(days_sale) as days_profit_rate,
    sum(sale) sale,
    sum(sale-ring_months_sale)/sum(ring_months_sale) as ring_sales_ratio,
    sum(profit) profit,
    sum(profit)/sum(sale)as profit_rate,
    sum(sale_sku)as sale_sku,
    sum(sale_cust)as sale_cust,
    sum(sale_cust)/sum(all_sale_cust) as penetration_rate,  -- 渗透率
    sum(ring_months_sale) as ring_months_sale,
    sum(all_sale_cust) as all_sale_cust
from
(
select
    province_code ,
    province_name ,
    division_code ,
    division_name,
    department_code ,
    department_name,
    sum(case when sdt = '20200724' then sales_value end )as days_sale,
    sum(case when sdt = '20200724' then profit end) as days_profit,
    sum(sales_value) sale,
    sum(profit) profit,
    count(distinct a.customer_no )as sale_cust,
    count(distinct goods_code )as sale_sku,
    0 as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale
where
    sdt >= '20200701'
    and sdt <= '20200724'
    and  channel in ('1','7')
group by 
province_code ,
    province_name ,
    division_code ,
    division_name
union all 
select
    province_code ,
    province_name ,
    division_code ,
    division_name,
    department_code ,
    department_name,
    0 as days_sale,
    0 as days_profit,
    0 sale,
    0 profit,
    0 sale_cust,
    0 sale_sku,
    sum(sales_value)as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale
where
    sdt >= '20200601'
    and sdt <= '20200624'
    and  channel in ('1','7')
group by 
    province_code ,
    province_name ,
    division_code ,
    division_name
) a 
left join 
(
select
    province_code ,
    province_name ,
    count(distinct a.customer_no )as all_sale_cust
from
    csx_dw.dws_sale_r_d_customer_sale
where
    sdt >= '20200701'
    and sdt <= '20200724'
    and  channel in ('1','7')
group by 
    province_code ,
    province_name 
   ) b on a.province_code=b.province_code 
 group by province_code ,
    province_name ,
    division_code ,
    division_name,
    department_code ,
    department_name;

refresh csx_dw.supply_turnover_province;
select * from csx_dw.supply_turnover_province where sdt='20200724';


-- 负毛利
select
zone_id,zone_name,
    province_code ,
    province_name ,
    division_code ,division_name ,
    count(goods_code )as sale_sku,
    sum(sale)sale,
    sum(profit)profit,
    sum(profit) /sum(sale) as profit_rate
 from (
select
zone_id,zone_name,
    province_code ,
    province_name ,
    goods_code ,
    goods_name,
    division_code ,division_name ,
    avg(cost_price )avg_cost,
    avg(sales_price )avg_sale,
    sum(sales_qty )qty,
    sum(sales_value) sale,
    sum(profit) profit
from
    csx_dw.dws_sale_r_d_customer_sale a 
 join 
 (select DISTINCT dist_code ,zone_id,zone_name from csx_dw.csx_shop where sdt='current')      b on a.province_code =b.dist_code
where
    sdt >= '20200727'
    and sdt <= '20200727'
    and channel in ('1','7')
group by 
province_code ,
    province_name ,
    goods_code ,
    goods_name,
    division_code ,division_name ,zone_id,zone_name
   ) a where zone_id ='3'and profit<0
   group by zone_id,zone_name,
    province_code ,
    province_name ,
    division_code ,division_name;
   
  select * from csx_dw.dws_sale_r_d_customer_sale  where sdt >= '20200701'
    and sdt <= '20200724'
    and channel in ('1','7')
    and province_code ='32'
    and goods_code ='846681'
    ;
    
select dc_type,
zone_id,zone_name,
    prov_code ,
    prov_name,
    bd_id ,
    bd_name ,
    final_amt ,
    final_qty,
    days_turnover,
    goods_sku ,
    sale_sku,
    pin_rate,
  negative_inventory/goods_sku  negative_rate,
  highet_sku /goods_sku  as highet_sku_rate,
  no_sale_sku /goods_sku as no_sale_sku_rate, 
    negative_amt,
    negative_inventory,
    highet_amt,
    highet_sku,
    no_sale_amt ,
    no_sale_sku 
from
    csx_dw.supply_turnover_province a 
join 
(select distinct province_code ,zone_id,zone_name from csx_dw.csx_shop where sdt='current' and zone_id='3') b on a.prov_code =b.province_code
where
    sdt = '20200726'
    and (dept_id ='00' or bd_id='00')
    and dc_type=    '仓库';

-- 商超销售查询
select
    province_code ,
    province_name,
    sales_belong_flag,
    sum(days_sale/10000 )as days_sale,
    sum(days_profit/10000) as days_profit,
    sum(days_profit)/sum(days_sale ) as days_profit_rate,
    sum(sale/10000) sale,
    sum(ring_sale/10000)  as ring_sale,
   (sum(sale)-sum(ring_sale))/sum(ring_sale) as ring_sale_ratio,
    sum(profit/10000 )profit ,
    sum(profit )/sum(sale )as profit_rate,
    
    sum(ring_profit/10000)  as ring_profit
from
(
select
    province_code ,
    province_name,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
    end sales_belong_flag,
    sum(case when sdt='${edate}' then sales_value end )as days_sale,
    sum(case when sdt='${edate}' then profit end )as days_profit,
    sum(sales_value) sale,
    sum(profit )profit ,
    0 as ring_sale,
    0 as ring_profit
from
    csx_dw.dws_sale_r_d_customer_sale as a
left join (
    select
        concat('S', shop_id)shop_id, sales_belong_flag
    from
        csx_dw.dws_basic_w_a_csx_shop_m a
    where
        sdt = 'current') b on
    a.customer_no = shop_id
where
    sdt >= '${sdate}'
    and sdt <= '${edate}'
    and channel = '2'
    and province_code in ('32','23','24')
  group by 
   province_code ,
    province_name,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
    end  
union all 
select 
 province_code ,
    province_name,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
    end  sales_belong_flag,
    0 as days_sale,
    0 as days_profit,
    0 as sale,
    0 as profit ,
    sum(sales_value) ring_sale,
    sum(profit ) ring_profit 
from
    csx_dw.dws_sale_r_d_customer_sale as a
left join (
    select
        concat('S', shop_id)shop_id, sales_belong_flag
    from
        csx_dw.dws_basic_w_a_csx_shop_m a
    where
        sdt = 'current') b on
    a.customer_no = shop_id
where
    sdt >= '${l_sdate}'
    and sdt <= '${l_edate}'
    and channel = '2'
    and province_code in ('32','23','24')
  group by 
   province_code ,
    province_name,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
    end 
) a 
group by 
    province_code ,
    province_name,
    sales_belong_flag;


select * from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current' and customer_name like '%红旗%';


-- 负毛利统计 

select
zone_id,zone_name,
    province_code ,
    province_name  ,
    count(goods_code )as sale_sku,
    sum(sale/10000)sale,
    sum(profit/10000 )profit,
    sum(profit) /sum(sale) as profit_rate
 from (
select
zone_id,zone_name,
    province_code ,
    province_name ,
    goods_code ,
    goods_name,
    division_code ,division_name ,
    avg(cost_price )avg_cost,
    avg(sales_price )avg_sale,
    sum(sales_qty )qty,
    sum(sales_value) sale,
    sum(profit) profit
from
    csx_dw.dws_sale_r_d_customer_sale a 
 join 
 (select DISTINCT dist_code ,zone_id,zone_name from csx_dw.csx_shop where sdt='current')      b on a.province_code =b.dist_code
where
    sdt >= '20200727'
    and sdt <= '20200727'
    and channel in ('1','7')
group by 
province_code ,
    province_name ,
    goods_code ,
    goods_name,
    division_code ,division_name ,zone_id,zone_name
   ) a where zone_id ='3'and profit<0
   group by zone_id,zone_name,
    province_code ,
    province_name ;


     
select
   zone_id,zone_name , province_code,province_name  ,
    count(distinct a.customer_no )as all_sale_cust
from
    csx_dw.dws_sale_r_d_customer_sale a
 left join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
where
 sdt >=  '20200701'
    and sdt <=  '20200726'
    and  channel in ('1','7')
group by 
   zone_id,zone_name ,province_code,province_name  
   ;
   select * from csx_ods.source_crm_w_a_sys_province ;
   select distinct dist_code ,dist_name ,zone_id ,zone_name from csx_dw.csx_shop where sdt='current';
 select location_code,province_code,province_name, region_code ,case when region_name='' then '' else concat(region_name,'大区') end region_name from 
csx_dw.dim_area;

select *  from csx_dw.dim_area where sdt='current';