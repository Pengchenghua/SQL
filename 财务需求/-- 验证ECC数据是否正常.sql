-- 验证ECC数据是否正常
select bukrs,prctr,hkont,kunnr,sum(dmbtr) dmbtr
from ods_ecc.ecc_ytbcustomer  
where sdt=regexp_replace(date_sub(current_date,0),'-','')
and  budat<=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')
and mandt='800'  
and hkont like'1122%'
and bukrs='2207'
group by bukrs,prctr,hkont,kunnr;


-- 验证ECC数据是否正常
select comp_code,customer_no,customer_name,sum(ac_all) from csx_tmp.ads_fr_r_d_account_receivables_scar  where sdt='20211231' and comp_code='2207'
group by comp_code,customer_no,customer_name;
