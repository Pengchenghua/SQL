--20190820业务提出：不要利润中心的字段，增加信控固定额度，临时额度，销售员三个字段
set i_sdate = '${START_DATE}';
set i_date=date_add(${hiveconf:i_sdate},1);


drop table b2b.csx_hepecc_bsid;
CREATE table b2b.csx_hepecc_bsid
as
select a.hkont,a.bukrs comp_code,case when length(a.kunnr)<3 then a.lifnr else a.kunnr end kunnr,a.budat,
'A'prctr,''shop_name,
a.dmbtr,c.zterm,c.diff,
concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)) sdate,
case when c.zterm like 'Y%' then 
date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),COALESCE(c.diff,0))
else date_add(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)),COALESCE(c.diff,0)) end edate
from (select * from ods_ecc.ecc_ytbcustomer  
where sdt=regexp_replace(${hiveconf:i_date},'-','') and  budat<regexp_replace(${hiveconf:i_date},'-','') and mandt='800'
and (substr(hkont,1,3)<>'139' or (substr(hkont,1,3)='139' and budat>='20190201'))) a 
left join 
(select customer_no,customer_name, payment_terms zterm,cast(payment_days as int) diff from csx_dw.customer_m 
 where sdt=regexp_replace(date_sub(current_date,1),'-','') and customer_no<>'')c 
on lpad(a.kunnr,10,'0')=lpad(c.customer_no,10,'0');
 
 


drop table b2b_tmp.temp_account_out;
CREATE temporary table b2b_tmp.temp_account_out
as
select a.*, 
row_number() OVER(PARTITION BY hkont,comp_code,kunnr,prctr ORDER BY budat asc)rno,
sum(amount)over(PARTITION BY hkont,comp_code,kunnr,prctr order by budat asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ) sum_sq,
sum(amount)over(PARTITION BY hkont,comp_code,kunnr,prctr order by budat asc)sum_bq
from (select comp_code,kunnr,hkont,cast(budat as int)budat,prctr,shop_name,sdate,edate,zterm,diff,sum(dmbtr)amount
 from b2b.csx_hepecc_bsid a where dmbtr>=0 
 group by comp_code,kunnr,hkont,budat,prctr,shop_name,sdate,edate,zterm,diff)a;
 
 drop table b2b_tmp.temp_account_in;
 CREATE temporary table b2b_tmp.temp_account_in 
 as
 select hkont,comp_code,kunnr,prctr,sum(dmbtr)amount
 from b2b.csx_hepecc_bsid a where dmbtr<0 
 group by hkont,comp_code,kunnr,prctr;

 
 
 --已收账款不足应收账款 
 drop table b2b_tmp.temp_account_left;
 CREATE temporary table b2b_tmp.temp_account_left
 as
 select a.comp_code,a.prctr,a.shop_name,a.kunnr,a.hkont,a.budat,a.sdate,a.edate,zterm,diff,
 case when coalesce(a.sum_sq,0)+b.amount<0 then a.sum_bq+b.amount else a.amount end amount
 ,a.rno,a.sum_bq+b.amount amount_left
 from b2b_tmp.temp_account_out a 
 join b2b_tmp.temp_account_in b on (a.hkont=b.hkont and a.comp_code=b.comp_code and a.kunnr=b.kunnr and a.prctr=b.prctr)
 where a.sum_bq+b.amount>=0
 --已收账款超过应收账款
  union all 
  select a.comp_code,a.prctr,a.shop_name,a.kunnr,a.hkont,a.budat,a.sdate,a.edate,zterm,diff,
  a.sum_bq+b.amount amount
 ,a.rno,a.sum_bq+b.amount amount_left
 from b2b_tmp.temp_account_out a 
 join (select hkont,comp_code,kunnr,prctr,max(rno)rno_max from b2b_tmp.temp_account_out group by hkont,comp_code,kunnr,prctr)c 
 on (a.hkont=c.hkont and a.comp_code=c.comp_code and a.kunnr=c.kunnr and a.rno=c.rno_max and a.prctr=c.prctr)
 join b2b_tmp.temp_account_in b on (a.hkont=b.hkont and a.comp_code=b.comp_code and a.kunnr=b.kunnr and a.prctr=b.prctr)
where a.sum_bq+b.amount<0 
--只有应收没有收款
 union all 
 select a.comp_code,a.prctr,a.shop_name,a.kunnr,a.hkont,a.budat,a.sdate,a.edate,zterm,diff,
 a.amount
 ,a.rno,a.sum_bq amount_left
 from b2b_tmp.temp_account_out a 
 left join b2b_tmp.temp_account_in b on (a.hkont=b.hkont and a.comp_code=b.comp_code and a.kunnr=b.kunnr and a.prctr=b.prctr)
 where b.amount is null
union all 
--只有预付没有收款
select 
a.comp_code,a.prctr,a.shop_name,a.kunnr,a.hkont,a.budat,a.sdate,a.edate,zterm,diff,
  a.amount amount
 ,null rno,a.amount amount_left
from 
(select comp_code,kunnr,hkont,cast(budat as int)budat,prctr,shop_name,sdate,edate,zterm,diff,sum(dmbtr)amount
 from b2b.csx_hepecc_bsid a where  dmbtr<0
 group by comp_code,kunnr,hkont,budat,prctr,shop_name,sdate,edate,zterm,diff)a 
left join (select hkont,comp_code,kunnr,prctr,sum(amount)amount from b2b_tmp.temp_account_out group by hkont,comp_code,kunnr,prctr)c 
on (a.hkont=c.hkont and a.comp_code=c.comp_code and a.kunnr=c.kunnr and a.prctr=c.prctr)
where c.amount is null;



set hive.exec.parallel=true;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.account_age_dtl_fct partition (sdt) 
 select c.sflag,a.hkont,d.account_name,a.comp_code,b.comp_name,a.prctr,a.shop_name,a.kunnr,c.cust_name name,
 zterm,case when zterm like 'Y%' then concat('月结',diff) else concat('票到',diff) end diff,
 sum(case when datediff(${hiveconf:i_sdate}, a.sdate) >=0 then amount else 0 end) ac_all,
 sum(case when a.edate>${hiveconf:i_sdate} then amount else 0 end) ac_wdq,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 0 and 15 then amount else 0 end) ac_15d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 16 and 30 then amount else 0 end) ac_30d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 31 and 60 then amount else 0 end) ac_60d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 61 and 90 then amount else 0 end) ac_90d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 91 and 120 then amount else 0 end) ac_120d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 121 and 180 then amount else 0 end) ac_180d,
sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 181 and 365 then amount else 0 end) ac_365d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate)>365 then amount else 0 end) ac_over365d,
 
 regexp_replace(${hiveconf:i_sdate},'-','') sdt 
 from b2b_tmp.temp_account_left a
 join (select distinct comp_code,comp_name from dim.dim_shop where edate='9999-12-31')b on a.comp_code=b.comp_code
 left join csx_ods.b2b_customer_new c on lpad(a.kunnr,10,'0')=lpad(c.cust_id,10,'0')
 left join csx_dw.sap_account_type d on a.hkont=d.accunt_code
 group by c.sflag,a.hkont,d.account_name,a.comp_code,b.comp_name,a.prctr,a.shop_name,a.kunnr,c.cust_name,zterm,
 case when zterm like 'Y%' then concat('月结',diff) else concat('票到',diff) end;

 insert overwrite table csx_dw.account_age_dtl_fct_new partition (sdt) 
 select c.sflag,a.hkont,d.account_name,a.comp_code,b.comp_name,a.prctr,a.shop_name,a.kunnr,c.cust_name name,
 zterm,case when zterm like 'Y%' then concat('月结',diff) else concat('票到',diff) end diff,
 sum(case when datediff(${hiveconf:i_sdate}, a.sdate) >=0 then amount else 0 end) ac_all,
 sum(case when a.edate>${hiveconf:i_sdate} then amount else 0 end) ac_wdq,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 0 and 15 then amount else 0 end) ac_15d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 16 and 30 then amount else 0 end) ac_30d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 31 and 60 then amount else 0 end) ac_60d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 61 and 90 then amount else 0 end) ac_90d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 91 and 120 then amount else 0 end) ac_120d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 121 and 180 then amount else 0 end) ac_180d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 181 and 365 then amount else 0 end) ac_365d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate)between 366 and 730 then amount else 0 end) ac_2y,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate)between 731 and 1095 then amount else 0 end) ac_3y,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate)>1095 then amount else 0 end) ac_over3y,
 regexp_replace(${hiveconf:i_sdate},'-','') sdt 
 from b2b_tmp.temp_account_left a
 join (select distinct comp_code,comp_name from dim.dim_shop where edate='9999-12-31')b on a.comp_code=b.comp_code
 left join csx_ods.b2b_customer_new c on lpad(a.kunnr,10,'0')=lpad(c.cust_id,10,'0')
 left join csx_dw.sap_account_type d on a.hkont=d.accunt_code
 group by c.sflag,a.hkont,d.account_name,a.comp_code,b.comp_name,a.prctr,a.shop_name,a.kunnr,c.cust_name,zterm,
 case when zterm like 'Y%' then concat('月结',diff) else concat('票到',diff) end;


