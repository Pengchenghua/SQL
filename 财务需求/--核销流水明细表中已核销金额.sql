--核销流水明细表中已核销金额
	select close_bill_no,
	sum(case when regexp_replace(substr(paid_time,1,10),'-','')>=${hiveconf:i_sdate_22} 
			  and regexp_replace(substr(paid_time,1,10),'-','')< ${hiveconf:i_sdate_23}
			 then payment_amount end ) payment_amount_by,
	sum(case when regexp_replace(substr(paid_time,1,10),'-','')>=${hiveconf:i_sdate_25} 
			  and regexp_replace(substr(paid_time,1,10),'-','')< ${hiveconf:i_sdate_24}
			 then payment_amount end ) payment_amount_sy,			 
		sum(payment_amount) payment_amount
	from
		csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
	where regexp_replace(substr(posting_time,1,10),'-','') <=${hiveconf:i_sdate_23}
	and (regexp_replace(substr(happen_date,1,10),'-','')<=${hiveconf:i_sdate_23} or happen_date='' or happen_date is NULL)
	and regexp_replace(substr(paid_time,1,10),'-','') <=${hiveconf:i_sdate_23} 
	and is_deleted ='0'
	and money_back_id<>'0' --回款关联ID为0是微信支付、-1是退货系统核销
	group by close_bill_no