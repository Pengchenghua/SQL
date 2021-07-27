select distinct send_location_code,send_location_name,supplier_code,supplier_name from csx_dw.dws_wms_r_d_entry_detail where business_type ='ZC01' AND SYS='old';



select * from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current';

show create table csx_dw.dws_basic_w_a_csx_supplier_m ;

-- 剔除合伙人仓、寄售小店仓
DROP TABLE csx_tmp.supplier_entry_amt;
CREATE table csx_tmp.supplier_entry_amt as 
SELECT CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END mon,
       supplier_code,
       supplier_name,
       a.goods_code,
       a.business_type,
       a.business_type_name,
       sum(receive_qty*price) AS amt
FROM csx_dw.dws_wms_r_d_entry_detail a
WHERE (business_type in ('ZN01','ZN02','ZC01')
       OR order_type_code LIKE 'P%')
  AND (sdt>='20200101'
       OR sdt='19990101')
  AND receive_status IN (1,2)
  AND purpose IN ('01','02','03','08','07')
GROUP BY CASE
             WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
             ELSE substr(sdt,1,6)
         END,
         supplier_code,
         supplier_name,
         a.goods_code,
         business_type,
         a.business_type_name
;


-- 1. 供应商集中度
-- TOP10/TOP30 供应商入库额含永辉供应商
select mon,supplier_code,supplier_name,amt,sku,aa from (
select mon,supplier_code,supplier_name,amt,sku,row_number()over (partition by mon order by amt desc) as aa  from 
(select mon,supplier_code,supplier_name,sum(amt) amt,count(distinct goods_code) as sku from csx_tmp.supplier_entry_amt  
group by  mon,supplier_code,supplier_name
) a 
) a where aa <11 or aa<31
 
 ;

-- 采购总额（含永辉供应商）
select mon, sum(amt) amt,count(distinct goods_code) as sku from csx_tmp.supplier_entry_amt 
group by mon

;
 
 
-- 1.1 供应商集中度
-- TOP10/TOP30 供应商入库额不含永辉供应商
select mon,supplier_code,supplier_name,amt,sku,aa from (
select mon,supplier_code,supplier_name,amt,sku,row_number()over (partition by mon order by amt desc) as aa  from 
(select mon,supplier_code,supplier_name,sum(amt) amt,count(distinct goods_code) as sku 
    from csx_tmp.supplier_entry_amt  
    where business_type not in ('ZC01','02')
group by  mon,supplier_code,supplier_name
) a 
) a where aa <11 or aa<31
 
 ;
 

 -- 不含永辉入库数据
--   采购总额（不含永辉供应商）
select mon, sum(amt) amt,count(distinct goods_code) as sku from csx_tmp.supplier_entry_amt 
where business_type not in ('ZC01','02')
group by mon

;

--- 2. 采购金额按供应商画像分类
-- 采购自永辉

 select mon,supplier_code,supplier_name,sum(amt) amt,count(distinct goods_code) as sku 
    from csx_tmp.supplier_entry_amt  
    where business_type  in ('ZC01','02')
group by  mon,supplier_code,supplier_name

 ;
 
 -- 产地直采
 select mon,supplier_code,supplier_name,sum(amt) amt,count(distinct goods_code) as sku 
    from csx_tmp.supplier_entry_amt  
    where business_type  in ('ZC01','02')
group by  mon,supplier_code,supplier_name

 ;



select distinct purpose,purpose_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose in ('01','02','03','08','07');