-- select * from csx_dw.dws_wms_r_d_entry_batch where origin_order_code='POW053210202000985';

-- 1.获取工厂商品销售明细
drop table if exists csx_tmp.temp_fac_sale ;
create temporary table if not exists  csx_tmp.temp_fac_sale as 
select split(id,'&')[0] as id ,
    business_type_code,
    business_type_name,
    city_group_code,
    city_group_name,
    goods_code,
    sum(sales_qty) as sales_qty,
    sum(sales_cost)as sales_cost,
    sum(profit) as profit,
    sum(excluding_tax_sales) as no_tax_sales,
    sum(excluding_tax_cost) as no_tax_cost,
    sum(excluding_tax_profit) as no_tax_profit
from csx_dw.dws_sale_r_d_detail
where sdt='20210520' 
    and is_factory_goods='1' 
    -- and dc_code='W0A8' 
    and classify_middle_code in ('B0304','B0305')
group by split(id,'&')[0] ,
    goods_code,
    business_type_code,
    business_type_name,
    city_group_code,
    city_group_name
    ;


-- 2.根据销售凭证号查找工单单号 WO210521000796  source_order_no来源单号(工单号)
-- ZH wms_batch_no WMS原料单号
drop table if exists csx_tmp.temp_fac_sale_01;
create temporary table if not exists csx_tmp.temp_fac_sale_01 as 
select business_type_code,
    business_type_name,
    city_group_code,
    city_group_name,
    a.credential_no,
    wms_batch_no
from csx_dw.dws_wms_r_d_batch_detail a 
join 
csx_tmp.temp_fac_sale b on a.credential_no=b.id 
 where a.move_type in ('107A','108B')
group by 
    a.credential_no,
    wms_batch_no,
     business_type_code,
    business_type_name,
    city_group_code,
    city_group_name
; 

-- select distinct business_type_name from  csx_tmp.temp_fac_sale_01;
-- create temporary table if not exists csx_tmp.temp_fac_sale_01 as 

-- 3.计算原料领用金额
select b.business_type_code,
    business_type_name,
    city_group_code,
    city_group_name,
    link_wms_batch_no,
    goods_code,
    sum(amt_no_tax) as no_tax_amt,
    sum(amt) as amt
from csx_dw.dws_wms_r_d_batch_detail a 
join 
csx_tmp.temp_fac_sale_01 b on a.link_wms_batch_no=b.wms_batch_no
where a.move_type in ('119A','119B')
and in_or_out ='1'
group by b.business_type_code,
    business_type_name,
    city_group_code,
    city_group_name,
    link_wms_batch_no,
    goods_code

;



-- 中台报价 
select  
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
    a.goods_code
    from 
    (
select  
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.goods_code,
    sum(a.excluding_tax_sales) as excluding_tax_sales,
    sum((a.purchase_price/(1+a.tax_rate))*a.sales_qty) as purchase_price_amt,
    sum(warehouse_rate*a.excluding_tax_sales) as warehouse_fee_amt,
    sum(delivery_rate*a.excluding_tax_sales) as deliver_fee_amt,
    sum(credit_rate*a.excluding_tax_sales) as credit_fee_amt,
    sum(run_rate*a.excluding_tax_sales) as run_fee_amt,
    sum(joint_venture_rate*a.excluding_tax_sales) as joint_venture_fee_amt
from csx_dw.dws_sale_r_d_detail a  
left join
(select warehouse_code,
    dimension_value_code as goods_code,
    warehouse_rate,         --仓储率
    delivery_rate,          --配送率
    credit_rate,            --信控率       
    run_rate,               --运营率
    joint_venture_rate,     --联营率
    price_begin_time,
    price_end_time
from  csx_ods.source_price_r_d_effective_middle_office_prices
where dimension_type=0
and sdt='20210523'
)b on  a.dc_code=b.warehouse_code and a.goods_code=b.goods_code
where a.purchase_price_flag=1 
and a.sdt>='20210501'
and a.dc_code='W0A2'
and a.sales_time>=b.price_begin_time 
and a.sales_time<=b.price_end_time 


--and a.classify_middle_code in ('B0304','B0305')
group by 
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.goods_code
)a
;
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name
;

show create table csx_dw.dws_price_r_d_goods_prices_m;