--2116公司入库、供应商排名
-- 剔除合伙人仓、寄售小店仓、彩食鲜小店
DROP TABLE csx_tmp.supplier_entry_amt_01;
CREATE table csx_tmp.supplier_entry_amt_01 as 
SELECT CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END mon,
       purchase_org,
       company_code,
       receive_location_code,
       supplier_code,
       supplier_name,
       a.goods_code,
       a.business_type,
       a.business_type_name,
       sum(receive_qty*price) AS amt
FROM csx_dw.dws_wms_r_d_entry_detail a
left join 
(select shop_id,company_code, purchase_org  from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and company_code='2116') b on a.receive_location_code=b.shop_id
WHERE (business_type in ('ZN01','ZN02')
       OR order_type_code LIKE 'P%')
  AND (sdt>='20200101'
       OR sdt='19990101')
  AND receive_status IN (1,2)
  AND purpose IN ('01','02','03','08','07')
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
         supplier_code,
         supplier_name,
         a.goods_code,
         business_type,
         a.business_type_name,
         receive_location_code,
         company_code,
         purchase_org
;

select yy,company_code,
    a.purchase_org,
    a.supplier_code,
    supplier_name,
    pay_condition,dic_value,
    amt,
    dense_rank()over(partition by yy, company_code order by amt desc)
from (
select substr(mon,1,4) as yy,company_code,purchase_org,supplier_code ,
    supplier_name,
    sum(amt) amt
from csx_tmp.supplier_entry_amt_01 
where company_code ='2116'
group by substr(mon,1,4),
company_code,
supplier_code,
supplier_name,
purchase_org
) a
left join
(select a.supplier_code,a.purchase_org,a.pay_condition,dic_value from csx_ods.source_basic_w_a_md_purchasing_info a
 LEFT JOIN 
 (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt='20211019' and dic_type='ACCOUNTCYCLE')b on a.pay_condition=b.dic_key
 where sdt='20211130') b on a.supplier_code=b.supplier_code and a.purchase_org=b.purchase_org;