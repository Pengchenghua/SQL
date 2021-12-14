-- 证件查询省区
set shop= ('W0A2','W0A3','W0F4','W0K1','W0A8','W0J2','W0L3','W0AU','W0G9','W0AJ','W0AL','W0AH',
            'WB11','W0K6','WA96','W0G6','W0F7','W0BK','W0Q2','W0Q9','W0BH','W0BS','W0BR',
            'W0R9','W0A5','W0P8','W0AS','W0N1','W0A6','W0N0','W0W7','W0X2','W0Z9','W0A7');

 -- 证件数据  2 待审核  3 已驳回 4 正常 5 过期 6已超时
 drop table  csx_tmp.temp_goods_04 ;
 create temporary table csx_tmp.temp_goods_04 as 
    select  province_code,
            province_name,
            city_code,
            city_name,
            shop_name,
            a.product_code,
            a.certificate_status,
            b.loction_code,
            filing_valid
    from
      
      (SELECT loction_code,
               certificate_id,
               product_code,
               supplier_code,
               province_code,
               province_name,
               city_code,
               city_name,
               shop_name
        FROM csx_ods.source_fqs_w_a_supplier_product_loction g
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
    and  table_type=1 ) j on g.loction_code=j.shop_id
        WHERE sdt>='20200101') b 
    left join 
    (SELECT a.id,
              a.product_code,
              a.supplier_code,
              a.certificate_status,
              small_classify_code category_small_code,
              create_time ,
              update_time 
        FROM csx_dw.dwd_fqs_w_d_supplier_product_certificate a
        WHERE sdt>='20200101'
            and a.certificate_status!='1')a on a.id=b.certificate_id and a.product_code=b.product_code and a.supplier_code=b.supplier_code 
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
    ) c on a.category_small_code=c.category_small_code
      where b.loction_code in  ${hiveconf:shop}
    group by  province_code,
            province_name,
            city_code,
            city_name,
            shop_name,
            a.product_code,
            a.certificate_status,
            b.loction_code,
            filing_valid
    ; 
    
-- 插入表
 select province_code,
        province_name,
        CONCAT_WS(',', COLLECT_LIST(distinct loction_code)) AS shop_list,
        count(distinct product_code) all_sku,
        count(distinct case when certificate_status=2 then product_code end ) as check_pending,
        count(distinct case when certificate_status=3 then product_code end) as rejected,
        count(distinct case when certificate_status=4 then product_code end) as normal ,
        count(distinct case when certificate_status=5 then product_code end) as overdue,
        count(distinct case when certificate_status=6 then product_code end) as timeout
 from csx_tmp.temp_goods_04 
 where 1=1
   -- and filing_valid='2'
 group by  province_code,
        province_name ;


    csx_tmp_ads_fqs_r_d_province_certificate_statistics_fr
            
CREATE table csx_tmp.ads_fqs_r_d_province_certificate_statistics_fr
(province_code string COMMENT '省区编码',
    province_name STRING COMMENT '省区编码',
    shop_list STRING comment 'DC列表',
    all_sku BIGINT COMMENT '所有SKU',
    check_pending_sku BIGINT COMMENT '待审核SKU',
    rejected_sku BIGINT COMMENT'已驳回SKU',
    normal_sku BIGINT COMMENT'正常SKU',
    overdue_sku BIGINT COMMENT'过期SKU',
    timeout_sku BIGINT COMMENT'已超时SKU',
    update_time TIMESTAMP COMMENT '更新日期'
    
)comment '省区统计商品证件'
PARTITIONED BY(sdt STRING COMMENT '日期分区，按照查询插入日期')
STORED AS PARQUET;