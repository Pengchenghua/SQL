select distinct send_location_code,send_location_name,supplier_code,
supplier_name from csx_dw.dws_wms_r_d_entry_detail where business_type ='ZC01' AND SYS='old';



select * from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current';

show create table csx_dw.dws_basic_w_a_csx_supplier_m ;

-- 剔除合伙人仓、寄售小店仓、彩食鲜小店
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

--剔除以下供应商
 supplier_code not in
('20015439','20019761','20021783','20024437','20026794','75000002',
'75000016',
'75000022',
'75000031',
'75000047',
'75000052',
'75000079',
'75000082',
'75000086',
'75000087',
'75000089',
'75000097',
'75000104',
'75000105',
'75000124',
'75000143',
'75000157',
'75000174',
'75000182',
'75000192',
'75000199',
'75000203',
'75000207',
'75000217',
'75000223',
'75000226',
'75000247',
'75000251',
'G2115',
'G2116',
'G2126',
'G2127',
'G3506')


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

--财务供应商入库行业、供应商类别
select mon,a.supplier_code,a.supplier_name,industry_name,dic_value, sum(amt) amt,count(distinct goods_code) as sku from csx_tmp.supplier_entry_amt  a 
left join 
(select vendor_id,supplier_type,dic_value,industry_name from csx_dw.dws_basic_w_a_csx_supplier_m a 
    join 
    (select memo,dic_value,dic_key from csx_ods.source_basic_w_a_md_dic where sdt='20210727' and memo like '供应商类%') b on a.supplier_type=b.dic_key
    where sdt='current') b on a.supplier_code=b.vendor_id
where 1=1
group by  mon,a.supplier_code,a.supplier_name,dic_value,industry_name
;

select mon,a.supplier_code,a.supplier_name,industry_name,dic_value,self_produce_self_sale, sum(amt) amt,count(distinct goods_code) as sku from csx_tmp.supplier_entry_amt  a 
left join 
(select vendor_id,supplier_type,dic_value,industry_name,self_produce_self_sale from csx_dw.dws_basic_w_a_csx_supplier_m a 
    join 
    (select memo,dic_value,dic_key from csx_ods.source_basic_w_a_md_dic where sdt='20210727' and memo like '供应商类%') b on a.supplier_type=b.dic_key
    left join 
    (select bloc_code,self_produce_self_sale from csx_ods.source_master_w_a_md_supplier_base_info where sdt = '20210728') c on a.vendor_id=c.bloc_code
    where sdt='current') b on a.supplier_code=b.vendor_id
where 1=1
group by  mon,a.supplier_code,a.supplier_name,dic_value,industry_name,self_produce_self_sale
;


select distinct purpose,purpose_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose in ('01','02','03','08','07');



-- 永辉供应商复用

 DROP TABLE csx_tmp.supplier_entry_amt_00;
CREATE table csx_tmp.supplier_entry_amt_00 as 
SELECT CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END mon,
       company_code,
       supplier_code,
       supplier_name,
       a.goods_code,
       a.business_type,
       a.business_type_name,
       sum(receive_qty*price) AS amt
FROM csx_dw.dws_wms_r_d_entry_detail a
JOIN
(select shop_id,company_code from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current')b on a.receive_location_code=b.shop_id
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
         a.business_type_name,
         company_code
;

select  mon,
       a.company_code,
       supplier_code,
       supplier_name,
       sum(amt)/10000 amt 
from csx_tmp.supplier_entry_amt_00 a 
join 
(select supplier_num,company_code from csx_tmp.ads_fr_r_m_supplier_reuse group by supplier_num,company_code) b on a.company_code=b.company_code and a.supplier_name=b.supplier_num
group by  mon,
       a.company_code,
       supplier_code,
       supplier_name
       ;
       
       
       
select  mon,       
       sum(amt)/10000 amt 
from csx_tmp.supplier_entry_amt_00 a 
join 
(select supplier_num,company_code from csx_tmp.ads_fr_r_m_supplier_reuse where yh_reuse_tag='是' group by supplier_num,company_code) b on a.company_code=b.company_code and a.supplier_name=b.supplier_num
group by  mon 
       ;
       



-- 自由品牌销售
select substr(sdt,1,6),goods_code,goods_name,
sum(excluding_tax_profit)/sum(excluding_tax_sales) as profit_rate ,
sum(excluding_tax_sales)/10000 sales
from csx_dw.dws_sale_r_d_detail 
where goods_code in ('1168187','1216354','1168186','1186702','1168188','762651','1216633','1216634','1194209','1194210','1201070','1201071','1201072','1201073','1201074')
and business_type_code !='4'
group by  substr(sdt,1,6),goods_code,goods_name
;




--部类入库
SELECT CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END mon,
		case when division_code  in ('10','11') then '11' else '12' end div_id,
       sum(receive_qty*price)/10000 AS amt
FROM csx_dw.dws_wms_r_d_entry_detail a
join
(select received_order_code from csx_dw.dwd_scm_r_d_order_header where local_purchase_flag =1 group by received_order_code) b on a.order_code =b.received_order_code
WHERE (business_type in ('ZN01','ZN02' )
       OR (order_type_code LIKE 'P%' and business_type !='02'))
  AND ( (sdt>='20200101' and sdt<'20210701')
       OR sdt='19990101')
  AND receive_status IN (1,2)
  AND purpose IN ('01','02','03','08','07','06')
  and supplier_code not in
('20015439','20019761','20021783','20024437','20026794','75000002',
'75000016',
'75000022',
'75000031',
'75000047',
'75000052',
'75000079',
'75000082',
'75000086',
'75000087',
'75000089',
'75000097',
'75000104',
'75000105',
'75000124',
'75000143',
'75000157',
'75000174',
'75000182',
'75000192',
'75000199',
'75000203',
'75000207',
'75000217',
'75000223',
'75000226',
'75000247',
'75000251',
'G2115',
'G2116',
'G2126',
'G2127',
'G3506')
GROUP BY CASE
             WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
             ELSE substr(sdt,1,6)
         END,
         case when division_code  in ('10','11') then '11' else '12' end 
;
