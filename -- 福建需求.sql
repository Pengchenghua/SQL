-- 福建需求 
-- 时间段：2020年，2021年，分二段取 字段：年份，DC，管理品类一二三级，供应商编码，供应商名称，净入库额 ;

-- 福建需求 
-- 时间段：2020年，2021年，分二段取 字段：年份，DC，管理品类一二三级，供应商编码，供应商名称，净入库额 ;


SELECT substr(sdt,1,4) year,
    dc_code,
       dc_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_code,
       goods_name,
       supplier_code,
       supplier_name,
       sum(entry_amt)entry_amt,
       sum(entry_qty)entry_qty,
       sum(return_amt)return_amt,
       sum(return_qty)return_qty
FROM csx_tmp.ads_wms_r_d_supplier_in_out_report_fr
where sdt>='20200101'
and sdt<'20220101'
    and province_name like '福建省'
 GROUP BY substr(sdt,1,4) ,
    dc_code,
       dc_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_code,
       goods_name,
       supplier_code,
       supplier_name