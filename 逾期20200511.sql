-- 更新日期：20200506 将 belnr 凭证 开头 以belnr 中1、01 、009开头为回款
-- 具体：2202010000/1398030000/1398040000/1399020000凭证号开头是1、01、009开头的是付款，
-- 其他凭证是采购。另外所有科目的规则和1122开头的科目一样 102998 客户验证
-- 应收金额=销售额+回款额  ；逾期
set mapreduce.job.queuename=caishixian;
set i_sdate                = '${START_DATE}';
set i_date                 =date_add(${hiveconf:i_sdate},1);
drop table if exists csx_dw.temp_ecc_ytbcustomer
;

CREATE  table if NOT EXISTS csx_dw.temp_ecc_ytbcustomer as
select
	a.hkont          ,
	a.bukrs comp_code,
	case
		when length(a.kunnr)<3
			then regexp_replace(a.lifnr,'^0*','')
			else regexp_replace(a.kunnr,'^0*','')
	end kunnr,
	a.budat  ,
	'A'prctr    ,
	'A'shop_name,
	a.dmbtr  ,
	sale,
	income,
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
		select *,
		-- 新增逻辑 sale 为销售应收额
		case when (substr(a.belnr,1,1)<>'1' and substr(a.belnr,1,2)<>'01' and substr(a.belnr,1,3)<>'009') then dmbtr else 0 end sale,
		-- 新增逻辑 income 为回款金额 
        case when (substr(a.belnr,1,1)='1' or substr(a.belnr,1,2)='01' or substr(a.belnr,1,3)='009')then dmbtr else 0 end income
		from
			ods_ecc.ecc_ytbcustomer a
		where
			sdt      =regexp_replace(${hiveconf:i_date},'-','')
			and budat<regexp_replace(${hiveconf:i_date},'-','')
			and mandt='800'
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
				regexp_replace(a.kunnr,'^0*','')=c.customer_number
				and a.bukrs         =c.company_code
			)
	left join
		(
			select
				customer_no        ,
				customer_name,
				channel,
				channel_code,
				payment_terms             zterm,
				cast(payment_days as int) diff
			from
				csx_dw.dws_crm_w_a_customer_m
			where
				sdt             =regexp_replace(date_sub(current_date,1),'-','')
				and customer_no<>''
		)
		d
		on
			regexp_replace(a.kunnr,'^0*','')=d.customer_no

;

--select * from  csx_dw.temp_ecc_ytbcustomer where kunnr='105150';

-- 销售应收金额
drop table if exists csx_dw.temp_account_out_01
;
CREATE temporary table if NOT EXISTS csx_dw.temp_account_out_01 as
select
	a.*,
	row_number() OVER(PARTITION BY hkont,comp_code,kunnr,prctr ORDER BY
					  budat asc)rno,
	sum(amount)over(PARTITION BY hkont,comp_code,kunnr,prctr order by
					budat asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ) sum_sq,
	sum(amount)over(PARTITION BY hkont,comp_code,kunnr,prctr order by
					budat asc)sum_bq,
	sum(accounts_amt)over(PARTITION BY hkont,comp_code,kunnr,prctr order by
					budat asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ) acc_sum_sq,
	sum(accounts_amt)over(PARTITION BY hkont,comp_code,kunnr,prctr order by
					budat asc)acc_sum_bq
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
			sum(a.sale)amount,
			sum(case when dmbtr>0 then dmbtr end ) as accounts_amt
		from
			csx_dw.temp_ecc_ytbcustomer  a
		where
		1=1
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
-- select * from  csx_dw.temp_account_out_01 where kunnr='102853';

-- select * from  csx_dw.temp_account_in_01 where kunnr='102853';
-- 回款金额 income计算
drop table csx_dw.temp_account_in_01;
 CREATE temporary table csx_dw.temp_account_in_01
 as
 select a.hkont,a.comp_code,kunnr,prctr    ,
 sum(a.income)amount,
 sum(case when sale<=0 then sale+income end )as write_amt,
 sum(case when (dmbtr<=0 and dmbtr!=income) then dmbtr+income end )as negative_amt,
 sum(case when (dmbtr<=0) then dmbtr end )as return_amt -- 以原来的逻辑
 from csx_dw.temp_ecc_ytbcustomer  a 
 where
 1=1
 group by a.hkont,a.comp_code,prctr    ,kunnr;
 


-- 以dmbtr 字段计算应收
drop table csx_dw.temp_account_left_01
;

CREATE temporary table csx_dw.temp_account_left_01 as

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
		when coalesce(a.acc_sum_sq,0)+b.return_amt<0
			then a.acc_sum_bq        +b.return_amt
			else a.amount
	end amount ,
	a.rno      ,
	a.sum_bq+b.amount as amount_left
from
	csx_dw.temp_account_out_01 a
	join
		csx_dw.temp_account_in_01 b
		on
			(
				a.hkont        =b.hkont
				and a.comp_code=b.comp_code
				and a.kunnr    =b.kunnr
				and a.prctr    =b.prctr
			)
where
	a.acc_sum_bq+b.return_amt>=0
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
    a.acc_sum_bq        +b.return_amt as amount , --应收金额+回款金额
	a.rno                    ,
	a.acc_sum_bq+b.return_amt as amount_left    --应收金额+回款金额
from
	csx_dw.temp_account_out_01 a
	join
		(
			select
				hkont    ,
				comp_code,
				kunnr    ,
				prctr    ,
				max(rno) as rno_max
			from
				csx_dw.temp_account_out_01
			--	where  amount!=0
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
		csx_dw.temp_account_in_01 b
		on
			(
				a.hkont        =b.hkont
				and a.comp_code=b.comp_code
				and a.kunnr    =b.kunnr
				and a.prctr    =b.prctr
			)
where
	a.acc_sum_bq+b.return_amt<0
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
	a.accounts_amt   amount ,
	a.rno      ,
	a.acc_sum_bq amount_left
from
	csx_dw.temp_account_out_01 a
	left join
		csx_dw.temp_account_in_01 b
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
			sum(a.dmbtr)amount
		from
			csx_dw.temp_ecc_ytbcustomer a
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
				sum(accounts_amt)amount
			from
				csx_dw.temp_account_out_01
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

 -- 计算逾期数据 
drop table csx_dw.temp_account_left_02
;

CREATE temporary table csx_dw.temp_account_left_02 as

-- 销售款+回款 =总应收款
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
	csx_dw.temp_account_out_01 a
	join
		csx_dw.temp_account_in_01 b
		on
			(
				a.hkont        =b.hkont
				and a.comp_code=b.comp_code
				and a.kunnr    =b.kunnr
				and a.prctr    =b.prctr
			)
where
	a.sum_bq+b.amount>=0
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
    a.sum_bq        +b.amount as amount , --应收金额+回款金额
	a.rno                    ,
	a.sum_bq+b.amount as amount_left    --应收金额+回款金额
from
	csx_dw.temp_account_out_01 a
	join
		(
			select
				hkont    ,
				comp_code,
				kunnr    ,
				prctr    ,
				max(rno) as rno_max
			from
				csx_dw.temp_account_out_01
			--	where  amount!=0
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
		csx_dw.temp_account_in_01 b
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
	csx_dw.temp_account_out_01 a
	left join
		csx_dw.temp_account_in_01 b
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
			sum(a.sale+a.income)amount
		from
			csx_dw.temp_ecc_ytbcustomer a
		where
			1=1
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
				csx_dw.temp_account_out_01
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

drop table csx_dw.peng_temp_account_data_01;
create table csx_dw.peng_temp_account_data_01
as 
select
	c.channel_code,
	c.channel,
	a.hkont         ,
	d.account_name  ,
	a.comp_code     ,
	b.comp_name     ,
	a.prctr         ,
	a.shop_name     ,
	a.kunnr         ,
	c.customer_name,
	zterm           ,
	case
		when zterm like 'Y%'
			then concat('月结',diff)
			else concat('票到',diff)
	end diff,
	${hiveconf:i_sdate} as to_day,
 	a.sdate,
	 a.edate,
	 datediff(	${hiveconf:i_sdate},a.edate)as diff_days, --逾期天数
	sum
		(
		a.amount
		
		)as 	ac_all,
	                                           sum(j.amount) as in_amount,
	regexp_replace(${hiveconf:i_sdate},'-','') sdt
from
	csx_dw.temp_account_left_01 a
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
		(select * from csx_dw.dws_crm_w_a_customer_m where sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')) c
		on
			a.kunnr=c.customer_no
	left join
		csx_dw.sap_account_type d
		on
			a.hkont=d.accunt_code
	left join 
	 csx_dw.temp_account_in_01 j
	 on a.hkont=j.hkont
	 and a.kunnr=j.kunnr
	 and a.comp_code=j.comp_code
	 and a.prctr=j.prctr
group by
--	c.sflag       ,
${hiveconf:i_sdate},
 	a.sdate,
	 a.edate,
	a.hkont       ,
	d.account_name,
	a.comp_code   ,
	b.comp_name   ,
	a.prctr       ,
	a.shop_name   ,
	a.kunnr       ,
	c.customer_name   ,
	zterm         ,
	case
		when zterm like 'Y%'
			then concat('月结',diff)
			else concat('票到',diff)
	end
;


-- -- 将30更改为31账期 20200330
-- set hive.exec.parallel              =true;
-- set hive.exec.parallel              =true;
-- set hive.exec.dynamic.partition     =true;
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- insert overwrite table csx_dw.account_age_dtl_fct partition
-- 	(sdt
-- 	)
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
create temporary table b2b_tmp.temp_csx_ac_all
as 
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
    a.sum_bq        +b.amount as amount , --应收金额+回款金额
	a.rno                    ,
	a.sum_bq+b.amount as amount_left    --应收金额+回款金额
from
	csx_dw.temp_account_out_01 a
	join
		(
			select
				hkont    ,
				comp_code,
				kunnr    ,
				prctr    ,
				max(rno) as rno_max
			from
				csx_dw.temp_account_out_01
			--	where  amount!=0
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
		csx_dw.temp_account_in_01 b
		on
			(
				a.hkont        =b.hkont
				and a.comp_code=b.comp_code
				and a.kunnr    =b.kunnr
				and a.prctr    =b.prctr
			)
;
drop table csx_dw.peng_temp_account_data_02 ;
create table csx_dw.peng_temp_account_data_02 as 
select
	c.channel         ,
	a.hkont         ,
	d.account_name  ,
	a.comp_code     ,
	b.comp_name     ,
	a.prctr         ,
	a.shop_name     ,
	a.kunnr         ,
	c.customer_name,
	a.zterm           ,
	case
		when a.zterm like 'Y%'
			then concat('月结',a.diff)
			else concat('票到',a.diff)
	end diff,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, j.sdate) >=0
					then j.amount
					else 0
			end
		) as ac_all_01,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.sdate) >=0
					then a.amount
					else 0
			end
		)
	ac_all,
	sum
		(
			case
				when a.edate>=${hiveconf:i_sdate}
					then a.amount
					else 0
			end
		)
	ac_wdq,
case when 	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.sdate) >=0
					then a.amount
					else 0
			end
		)-	sum
		(
			case
				when a.edate>=${hiveconf:i_sdate}
					then a.amount
					else 0
			end
		)<0 then 0 else 	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.sdate) >=0
					then a.amount
					else 0
			end
		)-	sum
		(
			case
				when a.edate>=${hiveconf:i_sdate}
					then a.amount
					else 0
			end
		) end as ac_yq,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 1 and 15
					then a.amount
					else 0
			end
		)
	ac_15d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 16 and 31
					then a.amount
					else 0
			end
		)
	ac_30d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 32 and 60
					then a.amount
					else 0
			end
		)
	ac_60d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 61 and 90
					then a.amount
					else 0
			end
		)
	ac_90d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 91 and 120
					then a.amount
					else 0
			end
		)
	ac_120d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 121 and 180
					then a.amount
					else 0
			end
		)
	ac_180d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 181 and 365
					then a.amount
					else 0
			end
		)
	ac_365d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate)between 366 and 730
					then a.amount
					else 0
			end
		)
	ac_2y,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate)between 731 and 1095
					then a.amount
					else 0
			end
		)
	ac_3y,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate)>1095
					then a.amount
					else 0
			end
		)
	                                           ac_over3y,
	regexp_replace(${hiveconf:i_sdate},'-','') sdt
from
	csx_dw.temp_account_left_02 a
	join 
	csx_dw.temp_account_left_01 j
	on a.hkont=j.hkont
	and a.kunnr=j.kunnr
	and a.comp_code=j.comp_code
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
	(select * from 	csx_dw.dws_crm_w_a_customer_m where sdt=regexp_replace(${hiveconf:i_sdate},'-','')) c
		on
			a.kunnr=c.customer_no
	left join
		csx_dw.sap_account_type d
		on
			a.hkont=d.accunt_code
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
			then concat('月结',a.diff)
			else concat('票到',a.diff)
	end
;