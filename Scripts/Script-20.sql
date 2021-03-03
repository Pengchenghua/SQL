select x.* from 
(select case when b.channel is null then '其他' else b.channel end sflag,
hkont,a.account_name,
comp_code,comp_name,b.sales_province dist,b.sales_city,kunnr,
b.customer_name name,b.first_category,b.second_category,b.third_category,
b.work_no,b.sales_name,b.credit_limit,b.temp_credit_limit,
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
hkont,a.account_name,comp_code,comp_name,substr(comp_name,1,2)dist,substr(comp_name,1,2) sales_city,kunnr,name,
'个人及其他'first_category,'个人及其他'second_category,'个人及其他'third_category,
''work_no,''sales_name,''credit_limit,''temp_credit_limit,
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
order by sflag,comp_code,dist,kunnr;



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
(select a.sflag,hkont,account_name,comp_code,comp_name,kunnr,zterm,diff,ac_all,ac_wdq,ac_15d,ac_30d,ac_60d,ac_90d,ac_120d,ac_180d,ac_365d,ac_2y,ac_3y,ac_over3y,prctr
	from data_center_report.account_age_dtl_fct_new a where sdt='${SDATE}' and a.ac_all<>0
and hkont in ('1398030000','1398040000','1399020000','2202010000'))a 
 left join (select distinct vendor_id,vendor_name from data_center_report.dim_vendor a where edate='9999-12-31' ) b 
 on lpad(a.kunnr,10,'0')=lpad(b.vendor_id,10,'0')
order by sflag,comp_code,prctr,kunnr;

select * from data_center_report.account_age_dtl_fct_new a where sdt='${SDATE}' and a.ac_all<>0
and hkont in ('1398030000','1398040000','1399020000','2202010000');


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
(select a.sflag,hkont,account_name,comp_code,comp_name,kunnr,zterm,diff,ac_all,ac_wdq,ac_15d,ac_30d,ac_60d,ac_90d,ac_120d,ac_180d,ac_365d,ac_2y,ac_3y,ac_over3y,prctr
	from data_center_report.account_age_dtl_fct_new a where sdt='${SDATE}' and a.ac_all<>0
and hkont in ('1398030000','1398040000','1399020000','2202010000'))a 
 left join (select distinct vendor_id,vendor_name from data_center_report.dim_vendor a where edate='9999-12-31' ) b 
 on lpad(a.kunnr,10,'0')=lpad(b.vendor_id,10,'0')
order by sflag,comp_code,prctr,kunnr;

