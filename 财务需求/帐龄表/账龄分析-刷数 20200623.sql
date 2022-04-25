--20190820业务提出：不要利润中心的字段，增加信控固定额度，临时额度，销售员三个字段
--20191107'V7126','V7127','V7128','V7129','V7130','V7131','V7132'这些客户的账龄改为月结45天

--首先出表过去让业务对比，一致后再刷。 
--sdt=月初第一天或者当天，根据下表1判断

select bukrs,sdt,substr(wtime,1,8),count(1)
from ods_ecc.ecc_ytbcustomer  
where sdt='20210106'and  budat<'20210101' and mandt='800'
and hkont ='1122010000'
group by bukrs,sdt,substr(wtime,1,8);


select bukrs,hkont,sum(dmbtr) dmbtr
from ods_ecc.ecc_ytbcustomer  
where sdt='20210106'and  budat<'20210101' and mandt='800'  
and hkont like'1122%'
group by bukrs,hkont;


select bukrs,prctr,hkont,sum(dmbtr) dmbtr
from ods_ecc.ecc_ytbcustomer  
where sdt='20210106'and  budat<'20210101' and mandt='800'  
and hkont like'1122%'
group by bukrs,prctr,hkont;

--某个客户
select regexp_replace(kunnr ,'(^0*)',''),bukrs,hkont,sum(dmbtr) dmbtr
from ods_ecc.ecc_ytbcustomer  
where sdt='20210106'and  budat<'20210101' and mandt='800'  
and hkont like'1122%'
and regexp_replace(kunnr ,'(^0*)','')='S9961'
group by regexp_replace(kunnr ,'(^0*)',''),bukrs,hkont;


-- 刷完数据后，需执行同步到mysql的任务，刷新完成


--每次都是刷上个月月末的数据，把i_sdate=上个月月末。 第一个框框是刷数据当天的，第二个是i_sdate+1
--set mapreduce.job.queuename=caishixian;

set i_sdate = '2020-12-31';

drop table csx_tmp.csx_hepecc_bsid_1;
CREATE table csx_tmp.csx_hepecc_bsid_1
as
select a.hkont,a.bukrs comp_code,case when length(a.kunnr)<3 then a.lifnr else a.kunnr end kunnr,a.budat,
'A'prctr,''shop_name,
a.dmbtr,
case when kunnr in ('V7126','V7127','V7128','V7129','V7130','V7131','V7132','V7000') then 'Y004' else c.zterm end zterm,
case when kunnr in ('V7126','V7127','V7128','V7129','V7130','V7131','V7132','V7000') then 45 else c.diff end diff,
concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)) sdate,
case when kunnr in ('V7126','V7127','V7128','V7129','V7130','V7131','V7132','V7000')
then date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),45)
when c.zterm like 'Y%' then 
date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),COALESCE(c.diff,0))
else date_add(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)),COALESCE(c.diff,0)) end edate
from (select * from ods_ecc.ecc_ytbcustomer  
where sdt='20210106' and  budat<'20210101' and mandt='800'  --框 月初刷数据改
and (substr(hkont,1,3)<>'139' or (substr(hkont,1,3)='139' and budat>='20190201'))
and 
-- 剔除利润调整凭证 科目+年度+凭证号+公司代码
concat_ws('-',hkont ,gjahr,belnr,bukrs) not in (
	'1122010000-2020-0090526358-1933',
    '1122010000-2020-0090526357-1933',
    '1122010000-2020-0090446438-1933',
    '1122010000-2020-0090446437-1933',
    '1122010000-2020-0090446436-1933',
    '1122010000-2020-0101042210-2200',
    '1122010000-2020-0100794408-2121',
    '1122010000-2020-0100794407-2121',
    '1122010000-2020-0100698829-2121',
    '1122010000-2020-0100698828-2121',
    '1122010000-2020-0100698815-2121',
    '1122010000-2020-0100698814-2121',
    '1122010000-2020-0100698811-2121',
    '1122010000-2020-0100698810-2121',
    '1122010000-2020-0100698807-2121',
    '1122010000-2020-0100698806-2121',
    '1122010000-2020-0100599788-2202',
    '1122010000-2020-0100387789-2400',
    '1122010000-2020-0100384016-2300',
    '1122010000-2020-0100343582-2403',
    '1122010000-2020-0100343559-2403',
    '1122010000-2020-0100343558-2403',
    '1122010000-2020-0100339686-2402',
    '1122010000-2020-0100245041-2303',
    '1122010000-2020-0100154283-2700',
    '1122010000-2020-0100066952-2105',
    '1122010000-2020-0100004543-2800',
 '1122010000-2020-0100183238-2700',
 '1122010000-2020-0100404461-2402',
 '1122010000-2020-0100467273-2400',
 '1122010000-2020-0100468834-2300',
 '1122010000-2020-0100755372-2202',
 '1122010000-2020-0100873656-2121',
 '1122010000-2020-0101263298-2200',
 '1122010000-2020-0090572072-1933')
) a 
left join 
( select customer_number,company_code,payment_terms zterm,cast(payment_days as int) diff 
 from csx_dw.dws_crm_r_a_customer_account_day a 
 where sdt='current' and customer_number<>'')c 
on (lpad(a.kunnr,10,'0')=lpad(c.customer_number,10,'0') and a.bukrs=c.company_code)
;
 
 


drop table csx_tmp.temp_account_out;
CREATE temporary table csx_tmp.temp_account_out
as
select a.*, 
row_number() OVER(PARTITION BY hkont,comp_code,kunnr,prctr ORDER BY budat asc)rno,
sum(amount)over(PARTITION BY hkont,comp_code,kunnr,prctr order by budat asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ) sum_sq,
sum(amount)over(PARTITION BY hkont,comp_code,kunnr,prctr order by budat asc)sum_bq
from (select comp_code,kunnr,hkont,cast(budat as int)budat,prctr,shop_name,sdate,edate,zterm,diff,sum(dmbtr)amount
 from csx_tmp.csx_hepecc_bsid_1 a where dmbtr>=0 
 group by comp_code,kunnr,hkont,budat,prctr,shop_name,sdate,edate,zterm,diff)a;
 
 drop table csx_tmp.temp_account_in;
 CREATE temporary table csx_tmp.temp_account_in 
 as
 select hkont,comp_code,kunnr,prctr,sum(dmbtr)amount
 from csx_tmp.csx_hepecc_bsid_1 a where dmbtr<0 
 group by hkont,comp_code,kunnr,prctr;

 
 
 --已收账款不足应收账款 
 drop table csx_tmp.temp_account_left;
 CREATE temporary table csx_tmp.temp_account_left
 as
 select a.comp_code,a.prctr,a.shop_name,a.kunnr,a.hkont,a.budat,a.sdate,a.edate,zterm,diff,
 case when coalesce(a.sum_sq,0)+b.amount<0 then a.sum_bq+b.amount else a.amount end amount
 ,a.rno,a.sum_bq+b.amount amount_left
 from csx_tmp.temp_account_out a 
 join csx_tmp.temp_account_in b on (a.hkont=b.hkont and a.comp_code=b.comp_code and a.kunnr=b.kunnr and a.prctr=b.prctr)
 where a.sum_bq+b.amount>=0
 --已收账款超过应收账款
  union all 
  select a.comp_code,a.prctr,a.shop_name,a.kunnr,a.hkont,a.budat,a.sdate,a.edate,zterm,diff,
  a.sum_bq+b.amount amount
 ,a.rno,a.sum_bq+b.amount amount_left
 from csx_tmp.temp_account_out a 
 join (select hkont,comp_code,kunnr,prctr,max(rno)rno_max from csx_tmp.temp_account_out group by hkont,comp_code,kunnr,prctr)c 
 on (a.hkont=c.hkont and a.comp_code=c.comp_code and a.kunnr=c.kunnr and a.rno=c.rno_max and a.prctr=c.prctr)
 join csx_tmp.temp_account_in b on (a.hkont=b.hkont and a.comp_code=b.comp_code and a.kunnr=b.kunnr and a.prctr=b.prctr)
where a.sum_bq+b.amount<0 
--只有应收没有收款
 union all 
 select a.comp_code,a.prctr,a.shop_name,a.kunnr,a.hkont,a.budat,a.sdate,a.edate,zterm,diff,
 a.amount
 ,a.rno,a.sum_bq amount_left
 from csx_tmp.temp_account_out a 
 left join csx_tmp.temp_account_in b on (a.hkont=b.hkont and a.comp_code=b.comp_code and a.kunnr=b.kunnr and a.prctr=b.prctr)
 where b.amount is null
union all 
--只有预付没有收款
select 
a.comp_code,a.prctr,a.shop_name,a.kunnr,a.hkont,a.budat,a.sdate,a.edate,zterm,diff,
  a.amount amount
 ,null rno,a.amount amount_left
from 
(select comp_code,kunnr,hkont,cast(budat as int)budat,prctr,shop_name,sdate,edate,zterm,diff,sum(dmbtr)amount
 from csx_tmp.csx_hepecc_bsid_1 a where  dmbtr<0
 group by comp_code,kunnr,hkont,budat,prctr,shop_name,sdate,edate,zterm,diff)a 
left join (select hkont,comp_code,kunnr,prctr,sum(amount)amount from csx_tmp.temp_account_out group by hkont,comp_code,kunnr,prctr)c 
on (a.hkont=c.hkont and a.comp_code=c.comp_code and a.kunnr=c.kunnr and a.prctr=c.prctr)
where c.amount is null;



set hive.exec.parallel=true;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.account_age_dtl_fct partition (sdt) 
 select NULL as sflag,a.hkont,d.account_name,a.comp_code,b.comp_name,a.prctr,a.shop_name,a.kunnr,NULL as name,
 zterm,case when zterm like 'Y%' then concat('月结',diff) else concat('票到',diff) end diff,
 sum(case when datediff(${hiveconf:i_sdate}, a.sdate) >=0 then amount else 0 end) ac_all,
 sum(case when a.edate>=${hiveconf:i_sdate} then amount else 0 end) ac_wdq,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 1 and 15 then amount else 0 end) ac_15d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 16 and 31 then amount else 0 end) ac_30d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 32 and 60 then amount else 0 end) ac_60d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 61 and 90 then amount else 0 end) ac_90d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 91 and 120 then amount else 0 end) ac_120d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 121 and 180 then amount else 0 end) ac_180d,
sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 181 and 365 then amount else 0 end) ac_365d,
 sum(case when datediff(${hiveconf:i_sdate}, a.edate)>365 then amount else 0 end) ac_over365d,
 
 regexp_replace(${hiveconf:i_sdate},'-','') sdt 
 from csx_tmp.temp_account_left a
 join (select code as comp_code, name as comp_name from csx_dw.dws_basic_w_a_company_code where sdt = 'current')b on a.comp_code = b.comp_code
-- left join csx_ods.b2b_customer_new c on lpad(a.kunnr,10,'0')=lpad(c.cust_id,10,'0')
 left join csx_dw.sap_account_type d on a.hkont=d.accunt_code
 group by a.hkont,d.account_name,a.comp_code,b.comp_name,a.prctr,a.shop_name,a.kunnr,zterm,
 case when zterm like 'Y%' then concat('月结',diff) else concat('票到',diff) end;



-- 账龄表（新增字段）
insert overwrite table csx_dw.account_age_dtl_fct_new partition (sdt) 
select 
  NULL as sflag,
  a.hkont, -- 科目代码
  d.account_name, -- 科目名称
  a.comp_code, -- 公司代码
  b.comp_name,
  a.prctr, -- 利润中心
  a.shop_name,
  a.kunnr, -- 客户编码
  c.customer_name as name,
  zterm, -- 账期类型
  case when zterm like 'Y%' then concat('月结', diff) else concat('票到', diff) end as diff, -- 账期
  sum(case when datediff(${hiveconf:i_sdate}, a.sdate) >= 0 then amount else 0 end) as ac_all, -- 全部账款
  sum(case when a.edate >= ${hiveconf:i_sdate} then amount else 0 end) as ac_wdq, -- 未到期账款
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 1 and 15 then amount else 0 end) as ac_15d, -- 15天内账款
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 16 and 31 then amount else 0 end) as ac_30d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 32 and 60 then amount else 0 end) as ac_60d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 61 and 90 then amount else 0 end) as ac_90d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 91 and 120 then amount else 0 end) ac_120d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 121 and 180 then amount else 0 end) as ac_180d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 181 and 365 then amount else 0 end) as ac_365d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 366 and 730 then amount else 0 end) as ac_2y,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 731 and 1095 then amount else 0 end) as ac_3y,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) > 1095 then amount else 0 end) as ac_over3y,
  e.last_sales_date,
  e.last_to_now_days,
  e.customer_active_sts_code,
  regexp_replace(${hiveconf:i_sdate}, '-', '') as sdt  -- 过账日期分区
from csx_tmp.temp_account_left a
join 
(
  select code as comp_code, name as comp_name 
  from csx_dw.dws_basic_w_a_company_code
  where sdt = 'current'
)b on a.comp_code = b.comp_code
left join
(
  select distinct customer_no,customer_name
  from csx_dw.dws_crm_w_a_customer_m_v1
  where sdt = 'current'
) c on lpad(a.kunnr,10,'0')=lpad(c.customer_no,10,'0')
left join csx_dw.sap_account_type d 
  on a.hkont = d.accunt_code
left join
(
  select distinct customer_no,sign_company_code,
  last_sales_date,
  last_to_now_days,
  customer_active_sts_code 
  from csx_tmp.ads_sale_w_d_customer_company_sales_date
  where sdt = 'current'
) e on lpad(a.kunnr,10,'0')=lpad(e.customer_no,10,'0') and a.comp_code = e.sign_company_code
group by a.hkont, d.account_name, a.comp_code, b.comp_name, a.prctr, a.shop_name, a.kunnr, c.customer_name,
   zterm, case when zterm like 'Y%' then concat('月结', diff) else concat('票到', diff) end, e.last_sales_date,
   e.last_to_now_days, e.customer_active_sts_code;
   

-- 跑完验证总数   
select hkont, -- 科目代码
sum(ac_all) as ac_all
from csx_dw.account_age_dtl_fct_new
where sdt='20201231' 
and hkont like '1122%'
group by  hkont;



