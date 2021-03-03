select a.hkont,a.bukrs comp_code,case when length(a.kunnr)<3 then a.lifnr else a.kunnr end kunnr,a.budat,
'A'prctr,''shop_name,
a.dmbtr,
case when kunnr in ('V7126','V7127','V7128','V7129','V7130','V7131','V7132','V7000') then 'Y004' else coalesce(c.zterm,d.zterm) end zterm,
case when kunnr in ('V7126','V7127','V7128','V7129','V7130','V7131','V7132','V7000') then 45 else coalesce(c.diff,d.diff) end diff,
concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)) sdate,
case when kunnr in ('V7126','V7127','V7128','V7129','V7130','V7131','V7132','V7000')
then date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),45)
when coalesce(c.zterm,d.zterm) like 'Y%' then 
date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),COALESCE(c.diff,d.diff,0))
else date_add(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)),COALESCE(c.diff,d.diff,0)) end edate
from (select * from ods_ecc.ecc_ytbcustomer  
where sdt='20200408' and  budat<'20200408' and mandt='800'
and (substr(hkont,1,3)<>'139' or (substr(hkont,1,3)='139' and budat>='20190201'))) a 
left join 
( select customer_number,company_code,payment_terms zterm,cast(payment_days as int) diff 
 from csx_dw.customer_account_day a 
 where sdt='current' and customer_number<>'')c 
on (lpad(a.kunnr,10,'0')=lpad(c.customer_number,10,'0') and a.bukrs=c.company_code)
left join 
 (select customer_no,
payment_terms zterm,cast(payment_days as int) diff from csx_dw.customer_m 
 where sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') and customer_no<>'')d 
 on lpad(a.kunnr,10,'0')=lpad(d.customer_no,10,'0')
;
 REFRESH ODS_ECC.ecc_ytbcustomer;
select * from csx_dw.CSX_SHOP where sdt='current';

select MIN(budat) from ods_ecc.ecc_ytbcustomer  
where sdt='20200408' and  budat<'20200408' and mandt='800'
and (substr(hkont,1,3)<>'139' or (substr(hkont,1,3)='139' and budat>='20190201'));


select SUM(sales_value ) from csx_dw.dws_sale_r_d_customer_sale a
join 
(select DISTINCT customer_no from (
select DISTINCT customer_no 
 customer_no from csx_dw.csx_partner_list where sdt='202005'
 union ALL 
 select customer_no from csx_dw.dws_crm_w_a_customer_m where sdt='20200531' and `attribute` like '%合伙%')a
 )b on a.customer_no=b.customer_no
where sdt>='20200501' and sdt<'20200601' ;

select * from csx_dw.csx_shop where sdt='current' 
;

select SUM(sales_value ) from csx_dw.dws_sale_r_d_customer_sale a
join 
(
select DISTINCT customer_no as 
 customer_no from csx_dw.csx_partner_list where sdt >='202001' )b on a.customer_no=b.customer_no
where sdt>='20200501' and sdt<'20200601' ;




select sdt,dc_code,dc_name,sum(sales_value )sale,
sum(sales_cost )cost,sum(inventory_amt )inventory_amt ,sum(inventory_amt )/sum(sales_cost ) as trunc_days,
SUM(case when sdt=regexp_replace(to_date(last_day(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))),'-','') then inventory_amt end ) as end_amt
from csx_dw.account_age_dtl_fct_new where sdt>='20200101' and sdt<'20200601'
and bd_id ='12'
and  dc_code ='W0A5'
group by dc_code,dc_name,substring(sdt,1,6)
;