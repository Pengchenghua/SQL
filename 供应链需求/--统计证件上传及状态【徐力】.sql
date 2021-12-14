--统计证件上传及状态
-- 供应链指定仓商品入库&销售【徐力】
set edate='2021-12-13';
set edt =regexp_replace(${hiveconf:edate},'-','');
set sdt=regexp_replace(date_sub(${hiveconf:edate},90),'-','');

drop table  csx_tmp.temp_goods ;
create temporary table csx_tmp.temp_goods as 
select shop_code,
    product_code,
    goods_name,
    des_specific_product_status,
    product_status_name,
    regionalized_trade_names,
    unit_name,
    b.brand_name,
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    b.category_small_code,
    category_small_name,
    filing_valid
from csx_dw.dws_basic_w_a_csx_product_info a 
left join
(select goods_id,
    goods_name,
    unit_name,
    brand_name,
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    g.category_small_code,
    category_small_name,
    filing_valid
from csx_dw.dws_basic_w_a_csx_product_m  g 
 left join 
    (SELECT leve_code AS category_small_code,
               cer_status,
               filing_valid,
               order_valid,
               receiving_valid,
               send_out_valid,
               cert_type
        FROM csx_ods.source_fqs_w_a_product_certificate 
        where sdt=${hiveconf:edt}
            and filing_valid in (1,2)
    ) c on g.category_small_code=c.category_small_code
where sdt='current') b on a.product_code=b.goods_id
where sdt='current'
and shop_code in 
('W0A2','W0A3','W0F4','W0K1','W0A8','W0J2','W0L3','W0AU','W0G9','W0AJ','W0AL','W0AH',
    'WB11','W0K6','WA96','W0G6','W0F7','W0BK','W0Q2','W0Q9','W0BH','W0BS','W0BR',
    'W0R9','W0A5','W0P8','W0AS','W0N1','W0A6','W0N0','W0W7','W0X2','W0Z9','W0A7')
    
    ;
    

-- 查询入库
-- ，近3月入库金额，近6月入库金额，近一年入库金额，近3月销售金额，近6月销售金额，近一年入库金额，

drop table  csx_tmp.temp_goods_01 ;
create temporary table csx_tmp.temp_goods_01 as 
select receive_location_code,
    goods_code,
    sum( receive_qty   ) mon3_qty,
    sum( amount ) mon3_amt
    -- sum(case when sdt between '20210601' and '20211130' then  receive_qty end ) mon6_qty,
    -- sum(case when sdt between '20210601' and '20211130' then  amount end ) mon6_amt,
    -- sum(receive_qty) year_qty,
    -- sum(amount) year_amt
from csx_dw.dws_wms_r_d_entry_detail
where sdt>=${hiveconf:sdt}
    and sdt<=${hiveconf:edt}
    and order_type_code like 'P%'
    and business_type='01'
    and receive_location_code in ('W0A2','W0A3','W0F4','W0K1','W0A8','W0J2','W0L3','W0AU','W0G9','W0AJ','W0AL','W0AH',
    'WB11','W0K6','WA96','W0G6','W0F7','W0BK','W0Q2','W0Q9','W0BH','W0BS','W0BR',
    'W0R9','W0A5','W0P8','W0AS','W0N1','W0A6','W0N0','W0W7','W0X2','W0Z9','W0A7')
    and receive_status='2'
group by receive_location_code,
    goods_code;
    
 -- 近3月销售金额，近6月销售金额，近一年入库金额，

drop table  csx_tmp.temp_goods_02 ;
create temporary table csx_tmp.temp_goods_02 as 
select dc_code,
    goods_code,
    sum( sales_qty   ) mon3_qty,
    sum( sales_value ) mon3_amt
    -- sum(case when sdt between '20210601' and '20211130' then  sales_qty end ) mon6_qty,
    -- sum(case when sdt between '20210601' and '20211130' then  sales_value end ) mon6_amt,
    -- sum(sales_qty) year_qty,
    -- sum(sales_value) year_amt
from csx_dw.dws_sale_r_d_detail
where sdt>=${hiveconf:sdt}
   and sdt<=${hiveconf:edt}
    and dc_code in ('W0A2','W0A3','W0F4','W0K1','W0A8','W0J2','W0L3','W0AU','W0G9','W0AJ','W0AL','W0AH',
    'WB11','W0K6','WA96','W0G6','W0F7','W0BK','W0Q2','W0Q9','W0BH','W0BS','W0BR',
    'W0R9','W0A5','W0P8','W0AS','W0N1','W0A6','W0N0','W0W7','W0X2','W0Z9','W0A7')
group by dc_code,
    goods_code;


    

drop table csx_tmp.temp_goods_03

;
create temporary table csx_tmp.temp_goods_03 as 
select 
    province_code,
    province_name,
    city_code,
    city_name,
    a.shop_code,
    shop_name,
    product_code,
    goods_name,
    des_specific_product_status,
    product_status_name,
    regionalized_trade_names,
    unit_name,
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    filing_valid,
    b.mon3_qty,
    b.mon3_amt,
    c.mon3_qty sales_mon3_qty,
    c.mon3_amt sales_mon3_amt
from  csx_tmp.temp_goods a 
left join 
 csx_tmp.temp_goods_01 b on a.shop_code=b.receive_location_code and a.product_code=b.goods_code
 left join csx_tmp.temp_goods_02 c on a.shop_code=c.dc_code and a.product_code=c.goods_code
 left join 
 (select 
    sales_province_code,
    sales_province_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' 
                WHEN province_name LIKE '%上海%'   then town_code
    else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then ''
            WHEN province_name LIKE '%上海%'   then town_name
    else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '900002' 
        when shop_id in ('WB09') then '900003'
        WHEN province_name LIKE '%江苏%' and city_name='南京市' then '320100'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '320500' 
 
    else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '大宗一' 
        when shop_id in ('WB09') then '平台酒水'
       WHEN province_name LIKE '%江苏%' and city_name='南京市' then '南京市'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '昆山市'

    else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 ) j on a.shop_code=j.shop_id
    ;
 
 -- 证件数据  2 待审核  3 已驳回 4 正常 5 过期 6已超时
 drop table  csx_tmp.temp_goods_04 ;
 create temporary table csx_tmp.temp_goods_04 as 
    select a.product_code,
           a.certificate_status,
           b.loction_code
    from
      (SELECT a.id,
              a.product_code,
              a.supplier_code,
              a.certificate_status,
              small_classify_code category_small_code,
              create_time ,
              update_time 
        FROM csx_dw.dwd_fqs_w_d_supplier_product_certificate a
        WHERE sdt>='20200101'
            and a.certificate_status!='1')a 
      left join 
      (SELECT loction_code,
               certificate_id,
               product_code,
               supplier_code
        FROM csx_ods.source_fqs_w_a_supplier_product_loction
        WHERE sdt>='20200101') b on a.id=b.certificate_id and a.product_code=b.product_code and a.supplier_code=b.supplier_code 
      where b.loction_code in  ('W0A2','W0A3','W0F4','W0K1','W0A8','W0J2','W0L3','W0AU','W0G9','W0AJ','W0AL','W0AH',
            'WB11','W0K6','WA96','W0G6','W0F7','W0BK','W0Q2','W0Q9','W0BH','W0BS','W0BR',
            'W0R9','W0A5','W0P8','W0AS','W0N1','W0A6','W0N0','W0W7','W0X2','W0Z9','W0A7')
    group by  a.product_code,
           a.certificate_status,
           b.loction_code
    ; 
 
-- select count(*) from  csx_tmp.temp_goods_04;
 
 drop table  csx_tmp.temp_goods_05;
 create temporary table csx_tmp.temp_goods_05 as 
 select a.*,b.certificate_status from  csx_tmp.temp_goods_03 a 
 left join 
  csx_tmp.temp_goods_04  b on a.shop_code=b.loction_code and a.product_code =b.product_code
  where (a.mon3_qty>0 or a.sales_mon3_qty>0) and a.des_specific_product_status ='0' ;
  
 select province_code,
        province_name,
        CONCAT_WS(',', COLLECT_LIST(distinct shop_code)) AS shop_list,
        count(distinct product_code) all_sku,
        count(distinct case when certificate_status=2 then product_code end ) as check_pending,
        count(distinct case when certificate_status=3 then product_code end) as rejected,
        count(distinct case when certificate_status=4 then product_code end) as normal ,
        count(distinct case when certificate_status=5 then product_code end) as overdue,
        count(distinct case when certificate_status=6 then product_code end) as timeout
 from csx_tmp.temp_goods_05 
 where filing_valid in ('1','2')
 group by  province_code,
        province_name ;
        
        select count(distinct product_code) from csx_tmp.temp_goods_03 where mon3_qty>0 and filing_valid in ('1','2') 
        and shop_code='W0A3' 
        AND des_specific_product_status=0;
        
        select * from csx_tmp.temp_goods_04 where loction_code='W0A3' ;
        
        
        select * from  csx_tmp.temp_goods_03;