-- 商品销售追溯采购入库
-- 大区处理 增加低毛利DC 标识,关联供应链仓信息
drop table csx_analyse_tmp.csx_analyse_tmp_jd_dc_new ;
create  TABLE csx_analyse_tmp.csx_analyse_tmp_jd_dc_new as 
select case when performance_region_code!='10' then '大区'else '平台' end dept_name,
    purchase_org,
    purchase_org_name,
    belong_region_code  region_code,
    belong_region_name  region_name,
    shop_code ,
    shop_name ,
    company_code ,
    company_name ,
    purpose,
    purpose_name,
    basic_performance_city_code as performance_city_code,
    basic_performance_city_name as performance_city_name,
    basic_performance_province_code as performance_province_code,
    basic_performance_province_name as performance_province_name,
    case when c.dc_code is not null then '1' else '0' end as is_purchase_dc ,
    enable_date,
    shop_low_profit_flag
from csx_dim.csx_dim_shop a 
 left join 
 (select belong_region_code,
        belong_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name
  from csx_dim.csx_dim_basic_performance_attribution) b on a.basic_performance_city_code= b.performance_city_code
 left join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date 
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current') c on a.shop_code=c.dc_code
 where sdt='current'    
    ;

CREATE table csx_analyse_tmp.csx_analyse_tmp_jd_order as 
SELECT a.credential_no,
       a.location_code as receive_dc_code,
       b.central_pursh_class_tag,
       a.source_order_no,
       a.batch_no as purchase_batch_no,
       a.wms_batch_no,
       a.wms_order_no,
       a.supplier_code,
       c.supplier_name,
       c.joint_purchase,
       b.settle_dc_code,
       a.goods_code,
       a.qty as pur_qty,
       a.amt as pur_amt,
       a.price as pur_price,
       b.business_type,                              -- 基地标识
       case when d.is_purchase_dc=1 
            and enable_date<=a.sdt
            and c.joint_purchase=1 
            and central_pursh_class_tag=1 
        then 1 else 0 end as central_purchase_tag   -- 集采标识 
FROM csx_dws.csx_dws_cas_credential_detail_di a 
LEFT JOIN 
(select order_code,
        goods_code,
        business_type,
        source_type,
        super_class,
        settle_location_code as settle_dc_code,
        case when c.classify_small_code is not null 
                and start_date<=sdt and end_date>=sdt 
        then 1 else 0 end  as central_pursh_class_tag
 from
    csx_dws.csx_dws_scm_order_detail_di a
     left join 
    (select short_name,
        classify_small_code,
        start_date,
        end_date
     from csx_ods.csx_ods_data_analysis_prd_source_scm_w_a_group_purchase_classily_df
    ) c on a.classify_small_code=c.classify_small_code
    where sdt>='20200101'
        and sdt<='20221009'
        and source_type not in ('4','15','18') -- 剔除 4项目合伙人、15联营直送、18城市服务商
        and super_class in (1,2)    -- 1供应商订单、2供应商退货单
    group by 
        order_code,
        goods_code,
        business_type,
        settle_location_code,
        case when c.classify_small_code is not null 
                and start_date<=sdt and end_date>=sdt 
            then 1 else 0 end ,
            source_type,
        super_class
) b on a.source_order_no=b.order_code and a.goods_code=b.goods_code
left join 
(select supplier_code,
        supplier_name,
        joint_purchase
 from csx_dim.csx_dim_basic_supplier
    where sdt='current') c on  a.supplier_code=c.supplier_code
left join csx_analyse_tmp.csx_analyse_tmp_jd_dc_new d on a.location_code=d.shop_code
WHERE a.sdt>='20210101'
    and a.sdt<='20221008'
    and move_type_code='101A'
    and source_type not in ('4','15','18') -- 剔除 4项目合伙人、15联营直送、18城市服务商
    and super_class in (1,2)    -- 1供应商订单、2供应商退货单
;



-- 销售指定供应链仓
drop table   csx_analyse_tmp.csx_analyse_tmp_jd_sale_detal ;
create table csx_analyse_tmp.csx_analyse_tmp_jd_sale_detal as 
    select 
      sdt,
	  split(id, '&')[0] as credential_no,
	  order_code ,
      region_code,
      region_name,
      b.performance_province_code province_code,
      b.performance_province_name province_name,
	  b.performance_city_code city_group_code,
	  b.performance_city_name city_group_name,
	  business_type_name,
	  inventory_dc_code as dc_code, 
      customer_code,
      customer_name,
      goods_code,
      goods_name,
      sale_qty,
      sale_amt,
      sale_amt_no_tax ,
      sale_cost_no_tax , 
      profit_no_tax ,
      sale_cost,
      profit,
      cost_price,
      sale_price,
      division_code,
      classify_large_code,
      classify_middle_code,
      a.classify_small_code,
      case when c.classify_small_code is not null and start_date<=sdt and end_date>=sdt then 1 else 0 end  as central_pursh_class_tag
    from  csx_dws.csx_dws_sale_detail_di a
    left join  csx_analyse_tmp.csx_analyse_tmp_jd_dc_new b on a.inventory_dc_code=b.shop_code
    left join 
    (select short_name,
        classify_small_code,
        start_date,
        end_date
     from csx_ods.csx_ods_data_analysis_prd_source_scm_w_a_group_purchase_classily_df
    ) c on a.classify_small_code=c.classify_small_code
    where sdt>='20220901'
        and sdt<'20221001'
        and is_purchase_dc=1
        and channel_code not in ('2','4', '6','5')
 	    and business_type_code ='1'   -- 日配业务 
        and b.shop_low_profit_flag =0
        and refund_order_flag =0
	    and order_channel_code <>'4'  -- 不含返利
	    and division_code in('11','10','12','13')
;



-- 查找销售批次号
drop table csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale;
create table csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale as 
select sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	city_group_code,
	city_group_name,
    dc_code,
    a.credential_no,
    order_code,
    central_pursh_class_tag,    -- 集采品类标识
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    classify_middle_code,
    a.classify_small_code,
    sale_qty,
    sale_amt,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    sale_cost,
    profit,
    cost_price,
    sale_price,
    b.batch_no,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    batch_price
FROM csx_analyse_tmp.csx_analyse_tmp_jd_sale_detal a
left join
(SELECT credential_no,
       batch_no,
       goods_code,
       qty  batch_qty,
       amt  batch_amt,
       amt_no_tax batch_amt_no_tax,
       price batch_price
FROM csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='20210101'
  AND in_out_type='SALE_OUT') b on a.credential_no=b.credential_no and a.goods_code=b.goods_code;
  
 
 -- 根据批次号查找采购入库凭证 
 drop table  csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_01;
 create table  csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_01 as 
 select sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	city_group_code,
	city_group_name,
    dc_code,           -- 销售出库DC
    receive_dc_code,   -- 入库DC
    a.credential_no,
    order_code,
    central_pursh_class_tag,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.profit,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    batch_price,
    purchase_crdential_no,
    a.batch_no,
    b.qty as pur_qty,
    b.amt as pur_amt,
    b.amt_no_tax as pur_amt_no_tax,
    b.price as pur_price
from  csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale a 
 left join
(SELECT credential_no as purchase_crdential_no,
       batch_no as purchase_batch_no,
       dc_code as receive_dc_code,
       goods_code,
       qty,
       amt,
       amt_no_tax,
       price
FROM  csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='20210101'
  AND in_out_type='PURCHASE_IN') b on a.batch_no=b.purchase_batch_no and a.goods_code=b.goods_code
  where purchase_crdential_no is not null 
  ;
  
-- select * from csx_tmp.temp_batch_sale_01 where sales_qty>0;
 
  -- 根据成品批次号查找领料凭证号 
drop table   csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_02;
create table csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_02 as 
select sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	city_group_code,
	city_group_name,
    dc_code,
    a.credential_no,
    order_code,
    central_pursh_class_tag,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.profit,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    batch_price,
    a.batch_no,
    transfer_crdential_no,
    transfer_qty,
    transfer_amt,
    transfer_price,
    transfer_amt_no_tax 
from csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale a 
 left join
(SELECT credential_no as transfer_crdential_no,
       batch_no as transfer_batch_no,
       goods_code,
       qty as   transfer_qty,
       amt as   transfer_amt,
       amt_no_tax as transfer_amt_no_tax,
       price as transfer_price
FROM  csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='20220101'
    and sdt<='20221008'
  AND in_out_type='FINISHED'
  and in_or_out=0
  ) b on a.batch_no=b.transfer_batch_no and a.goods_code=b.goods_code
where  b.transfer_crdential_no is not null 
;


-- select * from csx_tmp.temp_batch_sale_02 where sales_qty>0 and transfer_batch_no is not null;
 -- select distinct province_name from csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_03

-- 根据领料凭证号查找原料批次号
drop table    csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_03;
create table  csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_03 as 
select  a.transfer_crdential_no,
    a.goods_code,
    transfer_qty,
    transfer_amt,
    transfer_price,
    meta_batch_no,
    product_code,
    meta_qty,
    meta_amt,
    meta_amt_no_tax,
    meta_amt/sum(meta_amt)over(partition by transfer_crdential_no ) as ratio
from
    (select transfer_crdential_no,
        goods_code,
        transfer_qty,
        transfer_amt,
        transfer_price
    from csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_02
    group by transfer_crdential_no,
        goods_code,
        transfer_qty,
        transfer_amt,
        transfer_price
    ) a 
 left join
(SELECT credential_no as meta_crdential_no,
       batch_no as meta_batch_no,
       goods_code product_code,
       sum(qty) as meta_qty,
       sum(amt) as meta_amt,
       sum(amt_no_tax) meta_amt_no_tax
FROM csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='20220101'
  AND in_out_type='FINISHED'
  and in_or_out=1
  group by credential_no ,
       batch_no ,
       goods_code
  ) b on a.transfer_crdential_no=b.meta_crdential_no 

;


-- 判断是否基地与集采

drop table csx_analyse_tmp.csx_analyse_tmp_jd_purchase_jd ;
CREATE table csx_analyse_tmp.csx_analyse_tmp_jd_purchase_jd as
select  batch_no,
        business_type,
        central_purchase_tag,
        source_order_no
from 
(select distinct batch_no
from 
(select distinct meta_batch_no batch_no  from  csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_03 a 
union all 
select distinct batch_no  from csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_01
where purchase_crdential_no is not null 
) a
)a
left join 
(SELECT  
       purchase_batch_no,
       a.business_type,
       central_purchase_tag,
       source_order_no
FROM  csx_analyse_tmp.csx_analyse_tmp_jd_order a
    group by  purchase_batch_no,
       a.business_type,
       source_order_no,
       central_purchase_tag
) b on a.batch_no = b.purchase_batch_no
where (business_type=1 or central_purchase_tag=1)

;




-- 计算占比，根据销售凭证号计算占比
drop table csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_04 ;
create table csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_04 as 
select sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	city_group_code,
	city_group_name,
    dc_code,
    a.credential_no,
    order_code,
    central_pursh_class_tag,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.profit,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    batch_price,
    a.batch_no,
    a.transfer_crdential_no,
    a.transfer_qty,
    a.transfer_amt,
    a.transfer_price,
    a.transfer_amt_no_tax , 
    meta_batch_no,
    product_code,
    meta_qty,
    meta_amt,
    meta_amt/sum(meta_amt)over(partition by a.credential_no,a.goods_code ) as ratio
from csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_02 a 
left join 
(select a.transfer_crdential_no,
    goods_code,
    transfer_qty,
    transfer_amt,
    transfer_price,
    meta_batch_no,
    product_code,
    meta_qty,
    meta_amt,
    ratio
from csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_03 a 
-- join
--  csx_analyse_tmp.csx_analyse_tmp_jd_purchase_jd b on a.meta_batch_no=b.batch_no
  ) b on b.transfer_crdential_no =a.transfer_crdential_no and a.goods_code=b.goods_code
 ;


-- 工厂商品
drop table csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product;
create table csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product as 
select 
    sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	city_group_code,
	city_group_name,
    dc_code,
    a.credential_no,
    order_code,
    central_pursh_class_tag,
    a.goods_code,
    a.goods_name,
    division_code,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.profit,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    batch_price,
    a.batch_no,
    a.transfer_crdential_no,
    a.transfer_qty,
    a.transfer_amt,
    a.transfer_price,
    a.transfer_amt_no_tax ,  
    meta_batch_no,
    source_order_no,
    receive_dc_code,
    product_code,
    product_name, 
    product_tax_rate,                           -- 原料商品税率
    b.classify_large_code  as product_classify_large_code   ,
    b.classify_large_name  as product_classify_large_name   ,
    b.classify_middle_code as product_classify_middle_code  ,
    b.classify_middle_name as product_classify_middle_name  ,
    b.classify_small_code  as product_classify_small_code   ,
    b.classify_small_name  as product_classify_small_name   ,   
    meta_qty,
    meta_amt,
    meta_amt_no_tax,
    meta_amt/sum(meta_amt)over(partition by a.credential_no,a.goods_code ) as use_ratio,  -- 原料使用占比
    product_ratio,
    order_qty,
    order_amt,
    business_type,
    central_purchase_tag,
    supplier_code,
    supplier_name
from csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_02 a 
left join 
(SELECT a.transfer_crdential_no,
       a.goods_code,
       transfer_qty,
       transfer_amt,
       transfer_price,
       meta_batch_no,
       source_order_no,
       receive_dc_code,
       product_code,
       goods_name as product_name,
       product_tax_rate,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       meta_qty,
       meta_amt,
       meta_amt_no_tax,
       order_qty,
       order_amt,
       ratio as product_ratio,
       business_type,
       central_purchase_tag,
       supplier_code,
        supplier_name
FROM csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_03 a
left JOIN
  (SELECT  meta_batch_no batch_no,
           business_type,
           central_purchase_tag,
           source_order_no,
           order_qty,
           order_amt,
           receive_dc_code,
           supplier_code,
            supplier_name
   FROM csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_04 a
   LEFT JOIN
     (SELECT  purchase_batch_no,
            a.business_type,
            central_purchase_tag,
            source_order_no,
            receive_dc_code,
            supplier_code,
            supplier_name,
            sum(a.pur_qty) order_qty,
            sum(a.pur_amt) order_amt
      FROM csx_analyse_tmp.csx_analyse_tmp_jd_order a
      group by purchase_batch_no,
                a.business_type,
                source_order_no,
                receive_dc_code,
                supplier_code,
                supplier_name,
                central_purchase_tag
    ) b ON a.meta_batch_no = b.purchase_batch_no
    group by meta_batch_no  ,
             business_type,
             central_purchase_tag,
             source_order_no,
             order_qty,
             order_amt,
             supplier_code,
            supplier_name,
            receive_dc_code
) b ON a.meta_batch_no=b.batch_no
JOIN
  (SELECT goods_code,
          goods_name,
          tax_rate/100 product_tax_rate,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          classify_small_code,
          classify_small_name
   FROM csx_dim.csx_dim_basic_goods
   WHERE sdt='current') g ON a.product_code=g.goods_code
WHERE b.batch_no IS NOT NULL
) b on b.transfer_crdential_no =a.transfer_crdential_no and a.goods_code=b.goods_code
where   b.transfer_crdential_no is not null

;

-- 工厂端 基地成品销售
drop table  csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product_01;
create table csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product_01 as 
select 
    sdt as sale_sdt,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
	a.city_group_code,
	a.city_group_name,
    dc_code,
    d.shop_name as  dc_name,
    a.credential_no,
    order_code,
    a.goods_code,
    a.goods_name,
    tax_rate,                       -- 商品税率
    division_code,
    division_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    a.profit,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,   
    a.batch_no, 
    batch_price,
    batch_qty   ,
    batch_amt   ,
    batch_amt_no_tax,
    (a.sale_price* batch_qty ) as batch_sale_amt,
    (a.sale_price/(1+tax_rate)*batch_qty ) as batch_sale_amt_no_tax,  
    (a.sale_price* batch_qty )- batch_amt as batch_profit,
    (a.sale_price/(1+tax_rate)*batch_qty )-batch_amt_no_tax as batch_profit_no_tax,
    (a.sale_price* batch_qty - batch_amt)/(a.sale_price* batch_qty ) as batch_profit_rate,
    (a.sale_price/(1+tax_rate)*batch_qty -batch_amt_no_tax)/(a.sale_price/(1+tax_rate)*batch_qty ) as batch_profit_rate_no_tax,
    a.transfer_crdential_no,    -- 成品凭证单号
    a.transfer_price,
    a.transfer_qty,
    a.transfer_amt,
    a.transfer_amt_no_tax ,  
    meta_batch_no,              -- 原料批次成本单号           
    product_code,               -- 原料商品编码
    product_name,               -- 原料商品名称
    product_tax_rate,           -- 原料商品税率
    product_classify_large_code	    ,
    product_classify_large_name	    ,
    product_classify_middle_code    ,
    product_classify_middle_name    ,
    product_classify_small_code	    ,
    product_classify_small_name	    ,
    meta_qty,                   -- 原料消耗数量
    meta_amt,                   -- 原料消耗金额
    meta_amt_no_tax,            -- 原料消耗金额(未税)
    use_ratio,                  -- 原料使用占比
    product_ratio,              -- 原料工单占比
    source_order_no, 
    receive_dc_code,            -- 入库DC
    f.shop_name as receive_dc_name,
    order_qty,
    order_amt,
    supplier_code,
    supplier_name,
    (a.sale_price* batch_qty ) * a.product_ratio as product_sale_amt,
    (a.sale_price/(1+tax_rate) * batch_qty ) * a.product_ratio as product_sale_amt_no_tax,
    batch_amt * product_ratio as product_cost_amt,
    a.batch_amt_no_tax * product_ratio as product_cost_amt_no_tax,
    (sale_price*batch_qty*product_ratio-a.batch_amt * a.product_ratio) product_profit,
    (a.sale_price/(1+tax_rate)*batch_qty*product_ratio-a.batch_amt_no_tax * a.product_ratio) product_profit_no_tax,
    (sale_price*batch_qty*product_ratio-a.batch_amt * a.product_ratio)/((a.sale_price* batch_qty ) * a.product_ratio) as product_profit_rate,
    (a.sale_price/(1+tax_rate)*batch_qty*product_ratio-a.batch_amt_no_tax * a.product_ratio)/(a.sale_price/(1+tax_rate) * batch_qty) product_no_tax_profit_rate,
    '2' as purchase_order_type,         -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
    '2' as goods_shipped_type,           -- 商品出库类型1 A进A出 2工厂加工 3其他
    current_timestamp as update_time,
    sdt
from csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product a
left join 
(select distinct credential_no
 from csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product where business_type=1) b on a.credential_no=b.credential_no
join
(select goods_code,
    tax_rate/100 tax_rate,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    division_name
 from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code
join 
 csx_analyse_tmp.csx_analyse_tmp_jd_dc_new d on a.dc_code=d.shop_code
join csx_analyse_tmp.csx_analyse_tmp_jd_dc_new f on a.receive_dc_code=f.shop_code
where business_type=1
and classify_large_code='B02';

 
 -- 采购端入库销售
 drop table csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product_02;
 create table csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product_02 as 
select 
    sdt as sale_sdt,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
	a.city_group_code,
	a.city_group_name,
    dc_code,
    d.shop_name as dc_name,
    a.credential_no,
    order_code,
    a.goods_code,
    a.goods_name,
    tax_rate,
    division_code,
    c.division_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    a.profit,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.batch_no,
    batch_price,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    (a.sale_price* batch_qty ) as batch_sale_amt,
    (a.sale_price/(1+tax_rate)*batch_qty ) as batch_sale_amt_no_tax,  
    (a.sale_price* batch_qty )- batch_amt as batch_profit,
    (a.sale_price/(1+tax_rate)*batch_qty )-batch_amt_no_tax as batch_profit_no_tax,
    (a.sale_price* batch_qty - batch_amt)/(a.sale_price* batch_qty ) as batch_profit_rate,
    (a.sale_price/(1+tax_rate)*batch_qty -batch_amt_no_tax)/(a.sale_price/(1+tax_rate)*batch_qty ) as batch_profit_rate_no_tax,
    '' as transfer_crdential_no,    -- 成品凭证单号
    0 as transfer_price,
    0 as transfer_qty,
    0 as transfer_amt,
    0 as transfer_amt_no_tax ,  
    '' as meta_batch_no,              -- 原料批次成本单号           
    '' as product_code,               -- 原料商品编码
    '' as product_name,               -- 原料商品名称
    0 as product_tax_rate,
    '' as product_classify_large_code	    ,
    '' as product_classify_large_name	    ,
    '' as product_classify_middle_code    ,
    '' as product_classify_middle_name    ,
    '' as product_classify_small_code	    ,
    '' as product_classify_small_name	    ,
     0 as meta_qty,                   -- 原料消耗数量
     0 as meta_amt,                   -- 原料消耗金额
     0 as meta_amt_no_tax,            -- 原料消耗金额(未税)
     0 as use_ratio,                  -- 原料使用占比
     0 as produt_ratio,               -- 原料工单占比
    source_order_no,
    receive_dc_code, -- 入库DC
    f.shop_name receive_dc_name,
    order_qty,
    order_amt,
    supplier_code,
    supplier_name,
    a.sale_price*batch_qty as product_sale_amt,
   ( a.sale_price/(1+tax_rate))*a.batch_qty as product_sale_amt_no_tax,
   a.batch_amt as product_cost_amt,
   a.batch_amt_no_tax as product_cost_amt_no_tax,
   a.sale_price*batch_qty-batch_amt as product_profit,
   ( a.sale_price/(1+tax_rate))*a.batch_qty-batch_amt_no_tax as product_profit_no_tax,
   (a.sale_price*batch_qty-batch_amt)/(a.sale_price*batch_qty) as product_profit_rate,
   ( (a.sale_price/(1+tax_rate))*a.batch_qty-batch_amt_no_tax)/( a.sale_price/(1+tax_rate)*a.batch_qty) as product_no_tax_profit_rate,
    '2' as purchase_order_type,         -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
    '1' as goods_shipped_type,           -- 商品出库类型1 A进A出 2工厂加工 3其他
    current_timestamp() update_time,
    sdt
from csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_01 a
left join 
(SELECT  purchase_batch_no,
         a.business_type,
         source_order_no,
         goods_code,
         supplier_code,
         supplier_name,
         sum(a.pur_qty) order_qty,
         sum(a.pur_amt) order_amt
      FROM  csx_analyse_tmp.csx_analyse_tmp_jd_order a
      where business_type=1
      group by purchase_batch_no,
                a.business_type,
                source_order_no,
                supplier_code,
                supplier_name,
                a.goods_code
    ) b on a.batch_no=b.purchase_batch_no and b.goods_code=a.goods_code
join
(select goods_code,tax_rate/100 tax_rate,
        classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    division_name
 from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code
join 
 csx_analyse_tmp.csx_analyse_tmp_jd_dc_new d on a.dc_code=d.shop_code
join csx_analyse_tmp.csx_analyse_tmp_jd_dc_new f on a.receive_dc_code=f.shop_code
where business_type=1
and a.classify_large_code='B02';
 

-- 集采工厂入库商品
drop table  csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product_03;
create table csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product_03 as 
select 
    sdt as sale_sdt,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
	a.city_group_code,
	a.city_group_name,
    dc_code,
    d.shop_name as  dc_name,
    a.credential_no,
    order_code,
    a.goods_code,
    a.goods_name,
    tax_rate,                       -- 商品税率
    division_code,
    division_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    a.profit,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,   
    a.batch_no, 
    batch_price,
    batch_qty   ,
    batch_amt   ,
    batch_amt_no_tax,
    (a.sale_price* batch_qty ) as batch_sale_amt,
    (a.sale_price/(1+tax_rate)*batch_qty ) as batch_sale_amt_no_tax,  
    (a.sale_price* batch_qty )- batch_amt as batch_profit,
    (a.sale_price/(1+tax_rate)*batch_qty )-batch_amt_no_tax as batch_profit_no_tax,
    (a.sale_price* batch_qty - batch_amt)/(a.sale_price* batch_qty ) as batch_profit_rate,
    (a.sale_price/(1+tax_rate)*batch_qty -batch_amt_no_tax)/(a.sale_price/(1+tax_rate)*batch_qty ) as batch_profit_rate_no_tax,
    a.transfer_crdential_no,    -- 成品凭证单号
    a.transfer_price,
    a.transfer_qty,
    a.transfer_amt,
    a.transfer_amt_no_tax ,  
    meta_batch_no,              -- 原料批次成本单号           
    product_code,               -- 原料商品编码
    product_name,               -- 原料商品名称
    product_tax_rate,           -- 原料商品税率
    product_classify_large_code	    ,
    product_classify_large_name	    ,
    product_classify_middle_code    ,
    product_classify_middle_name    ,
    product_classify_small_code	    ,
    product_classify_small_name	    ,
    meta_qty,                   -- 原料消耗数量
    meta_amt,                   -- 原料消耗金额
    meta_amt_no_tax,            -- 原料消耗金额(未税)
    use_ratio,                  -- 原料使用占比
    product_ratio,              -- 原料工单占比
    source_order_no, 
    receive_dc_code,            -- 入库DC
    f.shop_name as receive_dc_name,
    order_qty,
    order_amt,
    supplier_code,
    supplier_name,
    (a.sale_price* batch_qty ) * a.product_ratio as product_sale_amt,
    (a.sale_price/(1+tax_rate) * batch_qty ) * a.product_ratio as product_sale_amt_no_tax,
    batch_amt * product_ratio as product_cost_amt,
    a.batch_amt_no_tax * product_ratio as product_cost_amt_no_tax,
    (sale_price*batch_qty*product_ratio-a.batch_amt * a.product_ratio) product_profit,
    (a.sale_price/(1+tax_rate)*batch_qty*product_ratio-a.batch_amt_no_tax * a.product_ratio) product_profit_no_tax,
    (sale_price*batch_qty*product_ratio-a.batch_amt * a.product_ratio)/((a.sale_price* batch_qty ) * a.product_ratio) as product_profit_rate,
    (a.sale_price/(1+tax_rate)*batch_qty*product_ratio-a.batch_amt_no_tax * a.product_ratio)/(a.sale_price/(1+tax_rate) * batch_qty) product_no_tax_profit_rate,
    '1' as purchase_order_type,         -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
    '2' as goods_shipped_type,           -- 商品出库类型1 A进A出 2工厂加工 3其他
    current_timestamp as update_time,
    sdt
from csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product a
left join 
(select distinct credential_no
 from csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product where business_type=1) b on a.credential_no=b.credential_no
join
(select goods_code,
    tax_rate/100 tax_rate,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    division_name
 from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code
join 
 csx_analyse_tmp.csx_analyse_tmp_jd_dc_new d on a.dc_code=d.shop_code
join csx_analyse_tmp.csx_analyse_tmp_jd_dc_new f on a.receive_dc_code=f.shop_code
where central_purchase_tag =1
and   classify_large_code !='B02';


-- 采购集采销售
drop table csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product_04;
 create table csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product_04 as 
select 
    sdt as sale_sdt,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
	a.city_group_code,
	a.city_group_name,
    dc_code,
    d.shop_name as dc_name,
    a.credential_no,
    order_code,
    a.goods_code,
    a.goods_name,
    tax_rate,
    division_code,
    c.division_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    a.profit,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.batch_no,
    batch_price,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    (a.sale_price* batch_qty ) as batch_sale_amt,
    (a.sale_price/(1+tax_rate)*batch_qty ) as batch_sale_amt_no_tax,  
    (a.sale_price* batch_qty )- batch_amt as batch_profit,
    (a.sale_price/(1+tax_rate)*batch_qty )-batch_amt_no_tax as batch_profit_no_tax,
    (a.sale_price* batch_qty - batch_amt)/(a.sale_price* batch_qty ) as batch_profit_rate,
    (a.sale_price/(1+tax_rate)*batch_qty -batch_amt_no_tax)/(a.sale_price/(1+tax_rate)*batch_qty ) as batch_profit_rate_no_tax,
    '' as transfer_crdential_no,    -- 成品凭证单号
    0 as transfer_price,
    0 as transfer_qty,
    0 as transfer_amt,
    0 as transfer_amt_no_tax ,  
    '' as meta_batch_no,              -- 原料批次成本单号           
    '' as product_code,               -- 原料商品编码
    '' as product_name,               -- 原料商品名称
    0 as product_tax_rate,
    '' as product_classify_large_code	    ,
    '' as product_classify_large_name	    ,
    '' as product_classify_middle_code    ,
    '' as product_classify_middle_name    ,
    '' as product_classify_small_code	    ,
    '' as product_classify_small_name	    ,
     0 as meta_qty,                   -- 原料消耗数量
     0 as meta_amt,                   -- 原料消耗金额
     0 as meta_amt_no_tax,            -- 原料消耗金额(未税)
     0 as use_ratio,                  -- 原料使用占比
     0 as produt_ratio,               -- 原料工单占比
    source_order_no,
    receive_dc_code, -- 入库DC
    f.shop_name receive_dc_name,
    order_qty,
    order_amt,
    supplier_code,
    supplier_name,
    a.sale_price*batch_qty as product_sale_amt,
   ( a.sale_price/(1+tax_rate))*a.batch_qty as product_sale_amt_no_tax,
   a.batch_amt as product_cost_amt,
   a.batch_amt_no_tax as product_cost_amt_no_tax,
   a.sale_price*batch_qty-batch_amt as product_profit,
   ( a.sale_price/(1+tax_rate))*a.batch_qty-batch_amt_no_tax as product_profit_no_tax,
   (a.sale_price*batch_qty-batch_amt)/(a.sale_price*batch_qty) as product_profit_rate,
   ( (a.sale_price/(1+tax_rate))*a.batch_qty-batch_amt_no_tax)/( a.sale_price/(1+tax_rate)*a.batch_qty) as product_no_tax_profit_rate,
    '1' as purchase_order_type,         -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
    '1' as goods_shipped_type,           -- 商品出库类型1 A进A出 2工厂加工 3其他
    current_timestamp() update_time,
    sdt
from csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_01 a
left join 
(SELECT  purchase_batch_no,
         a.business_type,
         source_order_no,
         goods_code,
         supplier_code,
         supplier_name,
         central_purchase_tag,
         sum(a.pur_qty) order_qty,
         sum(a.pur_amt) order_amt
      FROM  csx_analyse_tmp.csx_analyse_tmp_jd_order a
      where business_type=1
      group by purchase_batch_no,
                a.business_type,
                source_order_no,
                supplier_code,
                supplier_name,
                a.goods_code,
                central_purchase_tag
    ) b on a.batch_no=b.purchase_batch_no and b.goods_code=a.goods_code
join
(select goods_code,tax_rate/100 tax_rate,
        classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    division_name
 from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code
join 
 csx_analyse_tmp.csx_analyse_tmp_jd_dc_new d on a.dc_code=d.shop_code
join csx_analyse_tmp.csx_analyse_tmp_jd_dc_new f on a.receive_dc_code=f.shop_code
where central_purchase_tag=1
and a.classify_large_code !='B02';



insert overwrite table csx_analyse.csx_analyse_fr_fina_goods_sale_trace_po_di partition(sdt)
select * from csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product_01
union all 
select * from csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product_02
union all 
select * from csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product_03
union all 
select * from csx_analyse_tmp.csx_analyse_tmp_jd_puracse_product_04
;


 create table csx_analyse.csx_analyse_fr_fina_goods_sale_trace_po_di
(
sale_sdt string comment '销售日期',
region_code	string	comment '大区编码',
region_name	string	comment '大区名称',
province_code	string	comment '省区编码',
province_name	string	comment '省区名称',
city_group_code	string	comment '城市编码',
city_group_name	string	comment '城市名称',
dc_code	string	comment '销售出库DC编码',
dc_name	string	comment '销售出库DC名称',
credential_no	string	comment '销售凭证号',
order_code	string	comment '销售出库单号',
goods_code	string	comment '商品编码',
goods_name	string	comment '商品名称',
tax_rate    decimal(20,6) 	comment '商品税率',
division_code	string	comment '部类编码',
division_name string	comment '部类名称',
classify_large_code	string	comment '管理大类编码',
classify_large_name	string	comment '管理大类名称',
classify_middle_code	string	comment '管理中类编码',
classify_middle_name	string	comment '管理中类名称',
classify_small_code	string	comment '管理小类编码',
classify_small_name	string	comment '管理小类名称',
sale_price	decimal(20,6)	comment '销售单价'  ,
sale_cost	decimal(20,6)	comment '销售成本' ,
sale_amt	decimal(20,6)	comment '销售金额'  ,
sale_qty	decimal(20,6)	comment '销售数量',
profit	decimal(20,6)	    	comment '销售毛利额',
sale_amt_no_tax	decimal(20,6)	comment '未税销售额',
sale_cost_no_tax	decimal(20,6)	comment '未税销售成本',
profit_no_tax	decimal(20,6)	 comment '未税毛利额',
batch_no	string	    	comment '销售成本批次单号',
batch_price	decimal(20,6)	comment '销售批次单价',
batch_qty	decimal(20,6)	comment '销售批次数量',
batch_cost	decimal(20,6)	comment '销售批次成本',
batch_cost_no_tax	decimal(20,6)	comment '销售批次未税成本',
batch_sale_amt  decimal(20,6)	comment '批次销售额',
batch_sale_amt_no_tax	decimal(20,6)	comment '批次未税销售额',
batch_profit  decimal(20,6)	comment '批次毛利额',
batch_profit_no_tax	decimal(20,6)	comment '批次未税毛利额',
batch_profit_rate  decimal(20,6)	comment '批次毛利率',
batch_profit_rate_no_tax	decimal(20,6)	comment '批次未税毛利率',
transfer_crdential_no	string	comment '成品凭证单号',
transfer_price	decimal(20,6)	comment '成品单价',
transfer_qty	decimal(20,6)	comment '成品数量',
transfer_amt	decimal(20,6)	comment '成品成本金额',
transfer_amt_no_tax	decimal(20,6)	comment '成品未税成本金额',
meta_batch_no	string	comment '原料领用成本批次单号',
product_code	string	comment '原料商品编码',
product_name	string	comment '原料商品名称',
product_tax_rate decimal(20,6) comment '原料商品税率',
product_classify_large_code	    string	comment '原料商品管理大类编码',
product_classify_large_name	    string	comment '原料商品管理大类名称',
product_classify_middle_code	string	comment '原料商品管理中类编码',
product_classify_middle_name	string	comment '原料商品管理中类名称',
product_classify_small_code	    string	comment '原料商品管理小类编码',
product_classify_small_name	    string	comment '原料商品管理小类名称',
meta_qty	decimal(20,6)	comment '原料消耗数量',
meta_amt	decimal(20,6)	comment '原料消耗金额',
meta_amt_no_tax	decimal(20,6)	comment '原料消耗未税金额',
use_ratio	decimal(20,6)	    comment '原料消耗占比根据工单、成品使用占比',
product_ratio	decimal(20,6)	comment '原料占比根据工单凭证占比',
purchase_order_no	string	    	comment '采购订单单号',
receive_dc_code	string	comment '入库DC编码',
receive_dc_name string	comment '入库DC名称',
order_qty	decimal(20,6)	comment '入库数量',
order_amt	decimal(20,6)	comment '入库金额',
supplier_code string comment '供应商编码',
supplier_name string comment '供应商名称',
product_sale_amt	decimal(20,6) 	comment '原料销售额根据占比计算product_ratio',	
product_sale_amt_no_tax	decimal(20,6)	comment '原料未税销售额根据占比计算product_ratio',
product_cost_amt	decimal(20,6)	comment '原料销售成本根据占比计算product_ratio',
product_cost_amt_no_tax	decimal(20,6)	comment '原料未税销售成本根据占比计算product_ratio',
product_profit	decimal(20,6)	comment '原料毛利额根据占比计算product_ratio',
product_profit_no_tax	decimal(20,6)	comment '原料未税毛利额根据占比计算product_ratio',
product_profit_rate	decimal(20,6)	comment '原料毛利率根据占比计算product_ratio',
product_no_tax_profit_rate	decimal(20,6)	comment '原料未税毛利率根据占比计算product_ratio',
purchase_order_type 	string	comment '采购订单类型1 集采采购 2 基地采购 3 其他采购',
goods_shipped_type	string	comment '商品出库类型1 A进A出 2工厂加工 3其他',
update_time timestamp comment '更新日期'
)
comment '基地、集采商品销售入库分析'
partitioned by (sdt string comment '销售日期分区')

;


create table csx_data_market.report_csx_analyse_fr_fina_goods_sale_trace_po_di
(
id BIGINT NOT NULL auto_increment,
sale_sdt varchar(64) comment '销售日期',
region_code	varchar(64)	comment '大区编码',
region_name	varchar(64)	comment '大区名称',
province_code	varchar(64)	comment '省区编码',
province_name	varchar(64)	comment '省区名称',
city_group_code	varchar(64)	comment '城市编码',
city_group_name	varchar(64)	comment '城市名称',
dc_code	varchar(64)	comment '销售出库DC编码',
dc_name	varchar(64)	comment '销售出库DC名称',
credential_no	varchar(64)	comment '销售凭证号',
order_code	varchar(64)	comment '销售出库单号',
goods_code	varchar(64)	comment '商品编码',
goods_name	varchar(64)	comment '商品名称',
tax_rate    decimal(20,6) 	comment '商品税率',
division_code	varchar(64)	comment '部类编码',
division_name varchar(64)	comment '部类名称',
classify_large_code	varchar(64)	comment '管理大类编码',
classify_large_name	varchar(64)	comment '管理大类名称',
classify_middle_code	varchar(64)	comment '管理中类编码',
classify_middle_name	varchar(64)	comment '管理中类名称',
classify_small_code	varchar(64)	comment '管理小类编码',
classify_small_name	varchar(64)	comment '管理小类名称',
sale_price	decimal(20,6)	comment '销售单价'  ,
sale_cost	decimal(20,6)	comment '销售成本' ,
sale_amt	decimal(20,6)	comment '销售金额'  ,
sale_qty	decimal(20,6)	comment '销售数量',
profit	decimal(20,6)	    	comment '销售毛利额',
sale_amt_no_tax	decimal(20,6)	comment '未税销售额',
sale_cost_no_tax	decimal(20,6)	comment '未税销售成本',
profit_no_tax	decimal(20,6)	 comment '未税毛利额',
batch_no	varchar(64)	    	comment '销售成本批次单号',
batch_price	decimal(20,6)	comment '销售批次单价',
batch_qty	decimal(20,6)	comment '销售批次数量',
batch_cost	decimal(20,6)	comment '销售批次成本',
batch_cost_no_tax	decimal(20,6)	comment '销售批次未税成本',
batch_sale_amt  decimal(20,6)	comment '批次销售额',
batch_sale_amt_no_tax	decimal(20,6)	comment '批次未税销售额',
batch_profit  decimal(20,6)	comment '批次毛利额',
batch_profit_no_tax	decimal(20,6)	comment '批次未税毛利额',
batch_profit_rate  decimal(20,6)	comment '批次毛利率',
batch_profit_rate_no_tax	decimal(20,6)	comment '批次未税毛利率',
transfer_crdential_no	varchar(64)	comment '成品凭证单号',
transfer_price	decimal(20,6)	comment '成品单价',
transfer_qty	decimal(20,6)	comment '成品数量',
transfer_amt	decimal(20,6)	comment '成品成本金额',
transfer_amt_no_tax	decimal(20,6)	comment '成品未税成本金额',
meta_batch_no	varchar(64)	comment '原料领用成本批次单号',
product_code	varchar(64)	comment '原料商品编码',
product_name	varchar(64)	comment '原料商品名称',
product_tax_rate decimal(20,6) comment '原料商品税率',
product_classify_large_code	    varchar(64)	comment '原料商品管理大类编码',
product_classify_large_name	    varchar(64)	comment '原料商品管理大类名称',
product_classify_middle_code	varchar(64)	comment '原料商品管理中类编码',
product_classify_middle_name	varchar(64)	comment '原料商品管理中类名称',
product_classify_small_code	    varchar(64)	comment '原料商品管理小类编码',
product_classify_small_name	    varchar(64)	comment '原料商品管理小类名称',
meta_qty	decimal(20,6)	comment '原料消耗数量',
meta_amt	decimal(20,6)	comment '原料消耗金额',
meta_amt_no_tax	decimal(20,6)	comment '原料消耗未税金额',
use_ratio	decimal(20,6)	    comment '原料消耗占比根据工单、成品使用占比',
product_ratio	decimal(20,6)	comment '原料占比根据工单凭证占比',
purchase_order_no	varchar(64)	    	comment '采购订单单号',
receive_dc_code	varchar(64)	comment '入库DC编码',
receive_dc_name varchar(64)	comment '入库DC名称',
order_qty	decimal(20,6)	comment '入库数量',
order_amt	decimal(20,6)	comment '入库金额',
supplier_code varchar(64) comment '供应商编码',
supplier_name varchar(64) comment '供应商名称',
product_sale_amt	decimal(20,6) 	comment '原料销售额根据占比计算product_ratio',	
product_sale_amt_no_tax	decimal(20,6)	comment '原料未税销售额根据占比计算product_ratio',
product_cost_amt	decimal(20,6)	comment '原料销售成本根据占比计算product_ratio',
product_cost_amt_no_tax	decimal(20,6)	comment '原料未税销售成本根据占比计算product_ratio',
product_profit	decimal(20,6)	comment '原料毛利额根据占比计算product_ratio',
product_profit_no_tax	decimal(20,6)	comment '原料未税毛利额根据占比计算product_ratio',
product_profit_rate	decimal(20,6)	comment '原料毛利率根据占比计算product_ratio',
product_no_tax_profit_rate	decimal(20,6)	comment '原料未税毛利率根据占比计算product_ratio',
purchase_order_type 	varchar(64)	comment '采购订单类型1 集采采购 2 基地采购 3 其他采购',
goods_shipped_type	varchar(64)	comment '商品出库类型1 A进A出 2工厂加工 3其他',
update_time timestamp comment '更新日期',
primary key(id),
key index_dt(sale_sdt,province_code,city_group_code,dc_code,order_code,credential_no)using btree
)ENGINE=InnoDB CHARSET=utf8mb4 
comment='基地、集采商品销售入库分析'
