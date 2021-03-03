
select x.* ,length(dist) from 
(select case when b.channel is null then '其他' else b.channel end sflag,
hkont,a.account_name,
comp_code,comp_name ,b.sales_province dist,b.sales_city, replace(kunnr,'0000','')   as  kunnr,
b.customer_name name,b.first_category,b.second_category,b.third_category,
b.work_no,b.sales_name,b.first_supervisor_name,b.credit_limit,b.temp_credit_limit,
 zterm,diff,ac_all,
 case when ac_all<0 then ac_all else ac_wdq end ac_wdq,
 case when ac_all<0 then 0 else ac_15d end ac_15d,
 case when ac_all<0 then 0 else ac_30d end ac_30d,
 case when ac_all<0 then 0 else ac_60d end ac_60d,
 case when ac_all<0 then 0 else ac_90d end ac_90d,
 case when ac_all<0 then 0 else ac_120d end ac_120d,
 case when ac_all<0 then 0 else ac_180d end ac_180d,
 case when ac_all<0 then 0 else ac_365d end ac_365d,
 case when ac_all<0 then 0 else ac_2y end ac_2y,
 case when ac_all<0 then 0 else ac_3y end ac_3y,
 case when ac_all<0 then 0 else ac_over3y end ac_over3y 
from 
(select * from data_center_report.account_age_dtl_fct_new a where sdt='${SDATE}' and a.ac_all<>0 and kunnr<>'0000910001'
and hkont not in ('1398030000','1398040000','1399020000','2202010000'))a 
 left join data_center_report.customer_m  b 
 on lpad(a.kunnr,10,'0')=lpad(b.customer_no,10,'0')
 union all 
 select a.sflag,
hkont,a.account_name,comp_code,comp_name,
case when substr(comp_name,1,2)in('上海','北京','重庆') then concat(substr(comp_name,1,2),'市')
    when substr(comp_name,1,2)='永辉' then substr(comp_name,1,2)
    else concat(substr(comp_name,1,2),'省') end dist,
substr(comp_name,1,2) sales_city,replace(kunnr,'0000','')as kunnr,name,
'个人及其他'first_category,'个人及其他'second_category,'个人及其他'third_category,
''work_no,''sales_name,''first_supervisor_name,''credit_limit,''temp_credit_limit,
 zterm,diff,ac_all,
 case when ac_all<0 then ac_all else ac_wdq end ac_wdq,
 case when ac_all<0 then 0 else ac_15d end ac_15d,
 case when ac_all<0 then 0 else ac_30d end ac_30d,
 case when ac_all<0 then 0 else ac_60d end ac_60d,
 case when ac_all<0 then 0 else ac_90d end ac_90d,
 case when ac_all<0 then 0 else ac_120d end ac_120d,
 case when ac_all<0 then 0 else ac_180d end ac_180d,
 case when ac_all<0 then 0 else ac_365d end ac_365d,
 case when ac_all<0 then 0 else ac_2y end ac_2y,
 case when ac_all<0 then 0 else ac_3y end ac_3y,
 case when ac_all<0 then 0 else ac_over3y end ac_over3y 
 from data_center_report.account_age_dtl_fct_new a where sdt='${SDATE}' and a.ac_all<>0 and kunnr='0000910001')x
 where 1=1 and (case when dist in ('平台-生鲜采购','平台-食百采购') then '平台-供应链' else dist end ) in ('北京市')
order by sflag,comp_code,dist,kunnr;

select distinct from data_center_report.account_age_dtl_fct_new a where sdt='20200601' and a.ac_all<>0 and kunnr<>'0000910001'
and hkont not in ('1398030000','1398040000','1399020000','2202010000');



 select case when a.sflag is null then '其他' else a.sflag end sflag,
hkont,a.account_name,
comp_code,comp_name,kunnr,b.vendor_name,
 zterm,diff,ac_all,
 case when ac_all>0 then ac_all else ac_wdq end ac_wdq,
 case when ac_all>0 then 0 else ac_15d end ac_15d,
 case when ac_all>0 then 0 else ac_30d end ac_30d,
 case when ac_all>0 then 0 else ac_60d end ac_60d,
 case when ac_all>0 then 0 else ac_90d end ac_90d,
 case when ac_all>0 then 0 else ac_120d end ac_120d,
 case when ac_all>0 then 0 else ac_180d end ac_180d,
 case when ac_all>0 then 0 else ac_365d end ac_365d,
 case when ac_all<0 then 0 else ac_2y end ac_2y,
 case when ac_all<0 then 0 else ac_3y end ac_3y,
 case when ac_all<0 then 0 else ac_over3y end ac_over3y 
from 
(select * from data_center_report.account_age_dtl_fct_new a where sdt='${SDATE}' and a.ac_all<>0
and hkont in ('1398030000','1398040000','1399020000','2202010000'))a 
 left join (select distinct vendor_id,vendor_name from data_center_report.dim_vendor a where edate='9999-12-31') b 
 on lpad(a.kunnr,10,'0')=lpad(b.vendor_id,10,'0')
order by sflag,comp_code,prctr,kunnr;


select distinct vendor_id,vendor_name from data_center_report.dim_vendor a ;

desc customer_m;

 select case when a.sflag is null then '其他' else a.sflag end sflag,
hkont,a.account_name,
comp_code,comp_name, replace(kunnr ,'00','') as kunnr,b.vendor_name,
 zterm,diff,ac_all,
 case when ac_all>0 then ac_all else ac_wdq end ac_wdq,
 case when ac_all>0 then 0 else ac_15d end ac_15d,
 case when ac_all>0 then 0 else ac_30d end ac_30d,
 case when ac_all>0 then 0 else ac_60d end ac_60d,
 case when ac_all>0 then 0 else ac_90d end ac_90d,
 case when ac_all>0 then 0 else ac_120d end ac_120d,
 case when ac_all>0 then 0 else ac_180d end ac_180d,
 case when ac_all>0 then 0 else ac_365d end ac_365d,
 case when ac_all<0 then 0 else ac_2y end ac_2y,
 case when ac_all<0 then 0 else ac_3y end ac_3y,
 case when ac_all<0 then 0 else ac_over3y end ac_over3y 
from (select sflag       ,
                    hkont       ,
                    account_name,
                    comp_code   ,
                    comp_name   ,
                    prctr       ,
                    shop_name   ,
                   kunnr      ,
                    NAME        ,
                    zterm       ,
                    diff        ,
                    ac_all      ,
                    ac_wdq      ,
                    ac_15d      ,
                    ac_30d      ,
                    ac_60d      ,
                    ac_90d      ,
                    ac_120d     ,
                    ac_180d     ,
                    ac_365d     ,
                    ac_2y       ,
                    ac_3y       ,
                    ac_over3y   ,
                    sdt from data_center_report.account_age_dtl_fct_new a where sdt='${SDATE}' and a.ac_all<>0
and hkont in ('1398030000','1398040000','1399020000','2202010000'))a 
 left join (select distinct vendor_id,vendor_name from data_center_report.dim_vendor a where 1=1) b 
 on lpad(a.kunnr,10,'0')=lpad(b.vendor_id,10,'0')
order by sflag,comp_code,prctr,kunnr;

select  vendor_pur_lvl ,vendor_pur_lvl_name from data_center_report.dim_vendor a where 1=1 group by vendor_pur_lvl ,vendor_pur_lvl_name ;

select comp_code ,comp_name  from data_center_report.account_age_dtl_fct_new a where sdt='20200622' group by comp_code ,comp_name ;
SELECT
    hkont ,
    account_name
FROM
    data_center_report.account_age_dtl_fct_new a
WHERE
    sdt = '20200622'
    AND hkont IN ('1398030000', '1398040000', '1399020000', '2202010000')
GROUP BY
    hkont ,
    account_name ;
    

select
    *
from
    csx_b2b_accounting.accounting_stock_detail
where
    location_code = 'W0A3'
    and shipper_code = 'YHCSX'
    and product_code = '926019'
    and reservoir_area_code = 'TH01';
    
select
    *
from
    csx_b2b_accounting.accounting_stock_detail_view
where
    location_code = 'W0A3'
    and shipper_code = 'YHCSX'
    and product_code = '926019'
    and reservoir_area_code = 'TH01';
    
select
    *
from
    csx_b2b_accounting.accounting_stock
where
    location_code = 'W0A3'
    and shipper_code = 'YHCSX'
    and product_code = '926019'
    and reservoir_area_code = 'TH01';
 select * from csx_basic_data.md_company_code mcc ;
 SELECT * FROM data_center_report.customer_m where customer_no='102787'; in ('100270','101024','101806','102540','102548','102582','102607','102679','102710','102750','102765','102787','102861','103052','103065','103339','103776','103852','103866','103877','103882','103892','103894','103895',
'103896','103935','103972','104041','104146','104152','104779','104981','105357','106191','106195','106244','910001','910001');
 