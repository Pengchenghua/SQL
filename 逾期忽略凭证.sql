-- 更新日期：20200506 将 belnr 凭证 开头 以belnr 中1、01 、009开头为回款
set mapreduce.job.queuename=caishixian;
set i_sdate                = '${START_DATE}';
set i_date                 =date_add(${hiveconf:i_sdate},1);
drop table b2b.csx_hepecc_bsid
;

CREATE table b2b.csx_hepecc_bsid as
select
	a.hkont          ,
	a.bukrs comp_code,
	case
		when length(a.kunnr)<3
			then a.lifnr
			else a.kunnr
	end kunnr,
	a.budat  ,
	'A'prctr    ,
	''shop_name,
	a.dmbtr  ,
	case
		when kunnr in ('V7126',
					   'V7127',
					   'V7128',
					   'V7129',
					   'V7130',
					   'V7131',
					   'V7132',
					   'V7000')
			then 'Y004'
			else coalesce(c.zterm,d.zterm)
	end zterm,
	case
		when kunnr in ('V7126',
					   'V7127',
					   'V7128',
					   'V7129',
					   'V7130',
					   'V7131',
					   'V7132',
					   'V7000')
			then 45
			else coalesce(c.diff,d.diff)
	end                                                                         diff ,
	concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)) sdate,
	case
		when kunnr in ('V7126',
					   'V7127',
					   'V7128',
					   'V7129',
					   'V7130',
					   'V7131',
					   'V7132',
					   'V7000')
			then date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),45)
		when coalesce(c.zterm,d.zterm) like 'Y%'
			then date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),COALESCE(c.diff,d.diff,0))
			else date_add(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)),COALESCE(c.diff,d.diff,0))
	end edate
from
	(
		select *
		from
			ods_ecc.ecc_ytbcustomer
		where
			sdt      =regexp_replace(${hiveconf:i_date},'-','')
			and budat<regexp_replace(${hiveconf:i_date},'-','')
			and mandt='800'
			and belnr not in ('0100004543',
				  '0100066952',
				  '0100154283',
				  '0100245041',
				  '0100339686',
				  '0100343558',
				  '0100343559',
				  '0100343582',
				  '0100384003',
				  '0100384014',
				  '0100384015',
				  '0100384016',
				  '0100387789',
				  '0100599788',
				  '0100698806',
				  '0100698807',
				  '0100698808',
				  '0100698810',
				  '0100698811',
				  '0100698814',
				  '0100698815',
				  '0100698826',
				  '0100698828',
				  '0100698829',
				  '0101042210',
				  '0090446436',
				  '0090446437',
				  '0090446438',
				  '0090446444') 
			and
			(
				substr(hkont,1,3)<>'139'
				or
				(
					substr(hkont,1,3)='139'
					and budat       >='20190201'
				)
			)
	)
	a
	left join
		(
			select
				customer_number    ,
				company_code       ,
				payment_terms             zterm,
				cast(payment_days as int) diff
			from
				csx_dw.customer_account_day a
			where
				sdt                 ='current'
				and customer_number<>''
		)
		c
		on
			(
				lpad(a.kunnr,10,'0')=lpad(c.customer_number,10,'0')
				and a.bukrs         =c.company_code
			)
	left join
		(
			select
				customer_no        ,
				payment_terms             zterm,
				cast(payment_days as int) diff
			from
				csx_dw.customer_m
			where
				sdt             =regexp_replace(date_sub(current_date,1),'-','')
				and customer_no<>''
		)
		d
		on
			lpad(a.kunnr,10,'0')=lpad(d.customer_no,10,'0')

;
-- 应收额
drop table b2b_tmp.temp_account_out
;

CREATE temporary table b2b_tmp.temp_account_out as
select
	a.*,
	row_number() OVER(PARTITION BY hkont,comp_code,kunnr,prctr ORDER BY
					  budat asc)rno,
	sum(amount)over(PARTITION BY hkont,comp_code,kunnr,prctr order by
					budat asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ) sum_sq,
	sum(amount)over(PARTITION BY hkont,comp_code,kunnr,prctr order by
					budat asc)sum_bq
from
	(
		select
			comp_code              ,
			kunnr                  ,
			hkont                  ,
			cast(budat as int)budat,
			prctr                  ,
			shop_name              ,
			sdate                  ,
			edate                  ,
			zterm                  ,
			diff                   ,
			sum(dmbtr)amount
		from
			b2b.csx_hepecc_bsid a
		where
			dmbtr>=0
		group by
			comp_code,
			kunnr    ,
			hkont    ,
			budat    ,
			prctr    ,
			shop_name,
			sdate    ,
			edate    ,
			zterm    ,
			diff
	)
	a
;

drop table b2b_tmp.temp_account_in
;

CREATE temporary table b2b_tmp.temp_account_in as
select
	hkont    ,
	comp_code,
	kunnr    ,
	prctr    ,
	sum(dmbtr)amount
from
	b2b.csx_hepecc_bsid a
where
	dmbtr<0
group by
	hkont    ,
	comp_code,
	kunnr    ,
	prctr
;

--已收账款不足应收账款
drop table b2b_tmp.temp_account_left
;

CREATE temporary table b2b_tmp.temp_account_left as
select
	a.comp_code,
	a.prctr    ,
	a.shop_name,
	a.kunnr    ,
	a.hkont    ,
	a.budat    ,
	a.sdate    ,
	a.edate    ,
	zterm      ,
	diff       ,
	case
		when coalesce(a.sum_sq,0)+b.amount<0
			then a.sum_bq        +b.amount
			else a.amount
	end amount ,
	a.rno      ,
	a.sum_bq+b.amount as amount_left
from
	b2b_tmp.temp_account_out a
	join
		b2b_tmp.temp_account_in b
		on
			(
				a.hkont        =b.hkont
				and a.comp_code=b.comp_code
				and a.kunnr    =b.kunnr
				and a.prctr    =b.prctr
			)
where
	a.sum_bq+b.amount>=0
--已收账款超过应收账款
union all
select
	a.comp_code              ,
	a.prctr                  ,
	a.shop_name              ,
	a.kunnr                  ,
	a.hkont                  ,
	a.budat                  ,
	a.sdate                  ,
	a.edate                  ,
	zterm                    ,
	diff                     ,
	a.sum_bq+b.amount as amount ,
	a.rno                    ,
	a.sum_bq+b.amount as amount_left
from
	b2b_tmp.temp_account_out a
	join
		(
			select
				hkont    ,
				comp_code,
				kunnr    ,
				prctr    ,
				max(rno) as rno_max
			from
				b2b_tmp.temp_account_out
			group by
				hkont    ,
				comp_code,
				kunnr    ,
				prctr
		)
		c
		on
			(
				a.hkont        =c.hkont
				and a.comp_code=c.comp_code
				and a.kunnr    =c.kunnr
				and a.rno      =c.rno_max
				and a.prctr    =c.prctr
			)
	join
		b2b_tmp.temp_account_in b
		on
			(
				a.hkont        =b.hkont
				and a.comp_code=b.comp_code
				and a.kunnr    =b.kunnr
				and a.prctr    =b.prctr
			)
where
	a.sum_bq+b.amount<0
--只有应收没有收款
union all
select
	a.comp_code,
	a.prctr    ,
	a.shop_name,
	a.kunnr    ,
	a.hkont    ,
	a.budat    ,
	a.sdate    ,
	a.edate    ,
	zterm      ,
	diff       ,
	a.amount   ,
	a.rno      ,
	a.sum_bq amount_left
from
	b2b_tmp.temp_account_out a
	left join
		b2b_tmp.temp_account_in b
		on
			(
				a.hkont        =b.hkont
				and a.comp_code=b.comp_code
				and a.kunnr    =b.kunnr
				and a.prctr    =b.prctr
			)
where
	b.amount is null
union all
--只有预付没有收款
select
	a.comp_code     ,
	a.prctr         ,
	a.shop_name     ,
	a.kunnr         ,
	a.hkont         ,
	a.budat         ,
	a.sdate         ,
	a.edate         ,
	zterm           ,
	diff            ,
	a.amount amount ,
	null     rno    ,
	a.amount amount_left
from
	(
		select
			comp_code              ,
			kunnr                  ,
			hkont                  ,
			cast(budat as int)budat,
			prctr                  ,
			shop_name              ,
			sdate                  ,
			edate                  ,
			zterm                  ,
			diff                   ,
			sum(dmbtr)amount
		from
			b2b.csx_hepecc_bsid a
		where
			dmbtr<0
		group by
			comp_code,
			kunnr    ,
			hkont    ,
			budat    ,
			prctr    ,
			shop_name,
			sdate    ,
			edate    ,
			zterm    ,
			diff
	)
	a
	left join
		(
			select
				hkont    ,
				comp_code,
				kunnr    ,
				prctr    ,
				sum(amount)amount
			from
				b2b_tmp.temp_account_out
			group by
				hkont    ,
				comp_code,
				kunnr    ,
				prctr
		)
		c
		on
			(
				a.hkont        =c.hkont
				and a.comp_code=c.comp_code
				and a.kunnr    =c.kunnr
				and a.prctr    =c.prctr
			)
where
	c.amount is null
;

-- 将30更改为31账期 20200330
-- set hive.exec.parallel              =true;
-- set hive.exec.parallel              =true;
-- set hive.exec.dynamic.partition     =true;
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- insert overwrite table csx_dw.account_age_dtl_fct partition
-- 	(sdt
-- 	)
drop table csx_dw.peng_temp_account_data_01 ;
CREATE table csx_dw.peng_temp_account_data_01 as 
--INSERT overwrite table csx_dw.peng_temp_account_data_01 
select
	c.sflag         ,
	a.hkont         ,
	d.account_name  ,
	a.comp_code     ,
	b.comp_name     ,
	a.prctr         ,
	a.shop_name     ,
	a.kunnr         ,
	c.cust_name name,
	zterm           ,
	case
		when zterm like 'Y%'
			then concat('月结',diff)
			else concat('票到',diff)
	end diff,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.sdate) >=0
					then amount
					else 0
			end
		)
	ac_all,
	sum
		(
			case
				when a.edate>=${hiveconf:i_sdate}
					then amount
					else 0
			end
		)
	ac_wdq,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 1 and 15
					then amount
					else 0
			end
		)
	ac_15d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 16 and 31
					then amount
					else 0
			end
		)
	ac_30d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 32 and 60
					then amount
					else 0
			end
		)
	ac_60d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 61 and 90
					then amount
					else 0
			end
		)
	ac_90d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 91 and 120
					then amount
					else 0
			end
		)
	ac_120d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 121 and 180
					then amount
					else 0
			end
		)
	ac_180d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 181 and 365
					then amount
					else 0
			end
		)
	ac_365d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate)>365
					then amount
					else 0
			end
		)
	                                           ac_over365d,
	regexp_replace(${hiveconf:i_sdate},'-','') sdt
from
	b2b_tmp.temp_account_left a
	join
		(
			select distinct
				comp_code,
				comp_name
			from
				dim.dim_shop
			where
				edate='9999-12-31'
		)
		b
		on
			a.comp_code=b.comp_code
	left join
		csx_ods.b2b_customer_new c
		on
			lpad(a.kunnr,10,'0')=lpad(c.cust_id,10,'0')
	left join
		csx_dw.sap_account_type d
		on
			a.hkont=d.accunt_code
group by
	c.sflag       ,
	a.hkont       ,
	d.account_name,
	a.comp_code   ,
	b.comp_name   ,
	a.prctr       ,
	a.shop_name   ,
	a.kunnr       ,
	c.cust_name   ,
	zterm         ,
	case
		when zterm like 'Y%'
			then concat('月结',diff)
			else concat('票到',diff)
	end
;


drop table csx_dw.peng_temp_account_data_01 ;
CREATE table csx_dw.peng_temp_account_data_01 as 
-- insert overwrite table csx_dw.account_age_dtl_fct_new partition
-- 	(sdt
-- 	)
select
		c.channel       ,
	a.hkont       ,
	d.account_name,
	a.comp_code   ,
	b.comp_name   ,
	a.prctr       ,
	a.shop_name   ,
	a.kunnr       ,
	c.customer_name   ,
	a.zterm           ,
	case
		when a.zterm like 'Y%'
			then concat('月结',diff)
			else concat('票到',diff)
	end diff,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.sdate) >=0
					then amount
					else 0
			end
		)
	ac_all,
	sum
		(
			case
				when a.edate>=${hiveconf:i_sdate}
					then amount
					else 0
			end
		)
	ac_wdq,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 1 and 15
					then amount
					else 0
			end
		)
	ac_15d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 16 and 31
					then amount
					else 0
			end
		)
	ac_30d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 32 and 60
					then amount
					else 0
			end
		)
	ac_60d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 61 and 90
					then amount
					else 0
			end
		)
	ac_90d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 91 and 120
					then amount
					else 0
			end
		)
	ac_120d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 121 and 180
					then amount
					else 0
			end
		)
	ac_180d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 181 and 365
					then amount
					else 0
			end
		)
	ac_365d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate)between 366 and 730
					then amount
					else 0
			end
		)
	ac_2y,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate)between 731 and 1095
					then amount
					else 0
			end
		)
	ac_3y,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate)>1095
					then amount
					else 0
			end
		)
	                                           ac_over3y,
	regexp_replace(${hiveconf:i_sdate},'-','') sdt
from
	b2b_tmp.temp_account_left a
	join
		(
			select distinct
				comp_code,
				comp_name
			from
				dim.dim_shop
			where
				edate='9999-12-31'
		)
		b
		on
			a.comp_code=b.comp_code
	left join
	(select * from 	csx_dw.dws_crm_w_a_customer_m where sdt='20200512')c
		on
			lpad(a.kunnr,10,'0')=lpad(c.customer_no,10,'0')
	left join
		csx_dw.sap_account_type d
		on
			a.hkont=d.accunt_code
			where a.hkont like '1122%'
group by
	c.channel       ,
	a.hkont       ,
	d.account_name,
	a.comp_code   ,
	b.comp_name   ,
	a.prctr       ,
	a.shop_name   ,
	a.kunnr       ,
	c.customer_name   ,
	a.zterm         ,
	case
		when a.zterm like 'Y%'
			then concat('月结',diff)
			else concat('票到',diff)
	end
;