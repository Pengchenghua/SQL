
-- 昨日、昨日、昨日月1日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};
set i_sdate_1 =date_sub(current_date,1);
set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

	
set i_sdate_1 ='2020-11-30';
set i_sdate_11 ='20201130';
set i_sdate_12 ='20201101';


--set i_sdate                = '2020-11-30';
--set i_date                 =date_add(${hiveconf:i_sdate},1);

--订单应收金额、逾期日期、逾期天数
drop table csx_tmp.tmp_cust_order_overdue_dtl;
create table csx_tmp.tmp_cust_order_overdue_dtl
as
select
	c.channel,
	c.channel_code,	
	a.order_no,	-- 来源单号
	a.customer_no,	-- 客户编码
	c.customer_name,	-- 客户名称
	a.company_code,	-- 签约公司编码
	b.company_name,	-- 签约公司名称
	a.happen_date,	-- 发生时间		
	a.overdue_date,	-- 逾期时间	
	a.source_statement_amount,	-- 源单据对账金额
	a.money_back_status,	-- 回款状态
	a.unpaid_amount receivable_amount,	-- 应收金额
	a.account_period_code,	--账期编码 
	a.account_period_name,	--账期名称 
	a.account_period_val,	--账期值
	a.beginning_mark,	--是否期初
	a.bad_debt_amount,	
	a.over_days,	-- 逾期天数
	if(a.account_period_code like 'Y%', if(a.account_period_val = 31, 45, a.account_period_val + 15), a.account_period_val) as acc_val_calculation_factor,	-- 标准账期
	${hiveconf:i_sdate_11} sdt
from
	(
	select 
		source_bill_no as order_no,	-- 来源单号
		customer_code as customer_no,	-- 客户编码
		--customer_name,	-- 客户名称
		company_code,	-- 签约公司编码
		--company_name,	-- 签约公司名称
		happen_date,	-- 发生时间		
		overdue_date,	-- 逾期时间	
		source_statement_amount,	-- 源单据对账金额
		money_back_status,	-- 回款状态
		unpaid_amount,	-- 未回款金额
		account_period_code,	--账期编码 
		account_period_name,	--账期名称 
		account_period_val,	--账期值
		'否' as beginning_mark,	--是否期初
		bad_debt_amount,
		if((money_back_status<>'ALL' or (datediff(${hiveconf:i_sdate_1}, overdue_date)+1)>=1),datediff(${hiveconf:i_sdate_1}, overdue_date)+1,0) as over_days	-- 逾期天数
	--from csx_ods.source_sss_r_d_source_bill
	from csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201116  --销售单对账
	where sdt=${hiveconf:i_sdate_11}
	and date(happen_date)<=${hiveconf:i_sdate_1}
	--and beginning_mark='1'  	-- 期初标识 0-是 1-否
	--and money_back_status<>'ALL'
	union all
	select 
		id as order_no,	-- 来源单号
		customer_code as customer_no,	-- 客户编码
		--customer_name,	-- 客户名称
		company_code,	-- 签约公司编码
		--company_name,	-- 签约公司名称		
		'' happen_date,	-- 发生时间		
		overdue_date,	-- 逾期时间	
		beginning_amount source_statement_amount,	-- 源单据对账金额
		money_back_status,	-- 回款状态
		unpaid_amount,	-- 未回款金额
		account_period_code,	--账期编码 
		account_period_name,	--账期名称 
		account_period_val,	--账期值
		'是' as beginning_mark,	--是否期初	
		bad_debt_amount,
		if((money_back_status<>'ALL' or (datediff(${hiveconf:i_sdate_1}, overdue_date)+1)>=1),datediff(${hiveconf:i_sdate_1}, overdue_date)+1,0) as over_days	-- 逾期天数
	--from csx_ods.source_sss_r_a_beginning_receivable
	from csx_dw.dwd_sss_r_a_beginning_receivable_20201116 
	where sdt=${hiveconf:i_sdate_11}
	--and money_back_status<>'ALL'
	)a
left join 
	(
	select 
		code as company_code,
		name as company_name 
	from csx_dw.dws_basic_w_a_company_code 
	where sdt = 'current'
	)b on a.company_code = b.company_code
left join
	(
	select 
		customer_no,
		customer_name,
		channel,
		channel_code 
	from csx_dw.dws_crm_w_a_customer_m_v1 
	where sdt=${hiveconf:i_sdate_11} 
	)c on a.customer_no=c.customer_no;


-- 查询结果集
--计算逾期系数
insert overwrite directory '/tmp/raoyanhua/yuqi01' row format delimited fields terminated by '\t'
select 
	b.sales_province,	-- 省区
	a.channel,	-- 渠道
	b.work_no,	-- 销售员工号
	b.sales_name,	-- 销售员
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	c.account_period_code,	-- 账期编码
	if(c.account_period_code like 'Y%',if(c.account_period_val=31,45,c.account_period_val+15),c.account_period_val) account_period_val,	-- 帐期天数
	c.account_period_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称
	case when a.receivable_amount>=0 then a.receivable_amount else 0 end receivable_amount,	-- 应收金额
	case when a.over_amt>=0 and a.receivable_amount>0 then a.over_amt else 0 end over_amt,	-- 逾期金额
	--case when a.over_amt>=0 then a.over_amt_s else 0 end over_amt_s,	-- 逾期金额*逾期天数
	--case when a.receivable_amount>=0 then a.receivable_amount_s else 0 end receivable_amount_s,	-- 应收金额*帐期天数
	case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end over_amt_s,	-- 逾期金额*逾期天数
	case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end receivable_amount_s,	-- 应收金额*帐期天数	
    coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
						else (coalesce(case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end,0)
						/(case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end)) end
		  , 6),0) over_rate 	-- 逾期系数
		    
from
	(select
		channel,
		customer_no,
		customer_name,
		--account_period_code,
		--COALESCE(account_period_val,0) account_period_val,
		--account_period_name,
		company_code,
		company_name,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from csx_tmp.tmp_cust_order_overdue_dtl  
	where channel = '大客户' and sdt = ${hiveconf:i_sdate_11} 
	--签呈客户不考核，不算提成,因此不算逾期
	and customer_no not in('111118','103717','102755','104023','105673','104402')
	and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
	--签呈客户仅4月不考核，不算提成，4-6月不算逾期
	--and customer_no not in('PF0320','105177')
	--5月签呈客户5月剔除逾期，前者剔除逾期，后者仅5月剔除逾期
	and customer_no not in('103883','103167','105673','104352','104662','104514','104746','104172')
	--and customer_no not in('104055','106463')
	--6月签呈客户仅6月剔除逾期，其中 106463 从8月开始不剔除，106765 从12月开始不剔除，105240一直剔除
	--and customer_no not in('105157','107570','106905','104484','109382','106463','106765')
	--and customer_no not in('106463','106765')
	and customer_no not in('106765')
	and customer_no not in('105240')
	--678共3个月因财务对账不考核逾期
	--and customer_no not in('105527', '104445', '102202','100563','PF1206','102751','104775','103309','104116','PF0424','103183','102890',
	--						'102890','PF0320','105618','103320','104725','PF0094','103374','103772','PF1205','103094','104430','104519',
	--						'104478','103876','103782','104335','102790')
	--7月签呈，7、8、9共3个月不算逾期
	--and customer_no not in('106626','106997','111383','105169','105672','106652','105543','110679','105254','108773')
	--7月签呈，第1行仅7月剔除，第2行8月看情况，第3行历史问题已断约不考核逾期，第4行仅7月剔除逾期且不算提成
	----and customer_no not in('105493','105758','105832','105994','107015','111417')
							--'104677','111865','105528','105381','104867','107986','105717',
							--'107065','108096','108452','107851','106811','105572',
							--'105493','105758','105832','105994','107015','111417',
							--'113108','113067','110656','111837','111296','105202')
	--7月坏账签呈中，105601客户8月逾期剔除
	----and customer_no not in('105601')
	--8月签呈，其中107181客户8-9月剔除逾期，其他仅剔除8月
	--and customer_no not in('107181','105669','111905','110661','110677','110682','107459')
	----and customer_no not in('107181')
	--9月签呈 四川 算到业务代理人，每月剔除逾期和销售
	and customer_no not in('104179','112092')
	--9月签呈 安徽 已断约每月剔除? '106997'、'105169'9月有销售
	and customer_no not in('104352','105493','105758','105832','105994','107015','106626','106997','111383','105169','106652','105254','108773')
	--9月签呈 重庆 合伙人客户，9月剔除逾期和销售
	--and customer_no not in('114265','114248','114401','111933','113080','113392')
	--9月签呈 重庆 剔除9月逾期，其中'109484'剔除9月的逾期和销售
	--and customer_no not in('109484','107790','110664')	
	--9月签呈 江苏、贵州、四川 剔除9月逾期，其中'104268'10月也剔除
	--and customer_no not in('104268')
	--and customer_no not in('107621','109342','109403','109671','113154','113486','113762','114145','114482','111553','111559','111589',
	--						'111617','111618','111621','111622','111630','111632','111641','111643','113934','108797','104268','103997')
	--10月签呈 当月剔除逾期系数
	--and customer_no not in('105090','111935','110664','109293','111534','111810','112199','112201','112361','112874')	
	--group by channel,customer_no,customer_name,account_period_code,account_period_val,account_period_name,company_code,company_name
	group by channel,customer_no,customer_name,company_code,company_name
	)a
join (select * from csx_dw.dws_crm_w_a_customer_m_v1 where sdt=${hiveconf:i_sdate_11} and attribute_code <> 5) b on b.customer_no=a.customer_no
left join
	(select
		customer_number,
		company_code,
		payment_terms account_period_code,
		case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
			 else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
		COALESCE(cast(payment_days as int),0) account_period_val
	from csx_dw.dws_crm_r_a_customer_account_day a
	where sdt='current'
	and customer_number<>''
	)c on (a.customer_no=c.customer_number and a.company_code=c.company_code)
;

	
	

--客户逾期系数
drop table csx_tmp.temp_cust_over_rate;
create table csx_tmp.temp_cust_over_rate
as
select 
	channel,	-- 渠道
	customer_no,	-- 客户编码
	customer_name,	-- 客户名称
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when over_amt>=0 and receivable_amount>0 then over_amt else 0 end) over_amt,	-- 逾期金额
	--sum(case when over_amt>=0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	--sum(case when receivable_amount>=0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数
	sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数	
    coalesce(round(case  when coalesce(SUM(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
				else coalesce(sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end), 0)
				/sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) end
		  , 6),0) over_rate 	-- 逾期系数
from
	(select
		channel,
		customer_no,
		customer_name,
		--account_period_code,
		--COALESCE(account_period_val,0) account_period_val,
		--account_period_name,
		company_code,
		company_name ,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from csx_tmp.tmp_cust_order_overdue_dtl a 
	where channel = '大客户' and sdt = ${hiveconf:i_sdate_11}
	--签呈客户不考核，不算提成,因此不算逾期
	and customer_no not in('111118','103717','102755','104023','105673','104402')
	and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
	--签呈客户仅4月不考核，不算提成，4-6月不算逾期
	--and customer_no not in('PF0320','105177')
	--5月签呈客户5月剔除逾期，前者剔除逾期，后者仅5月剔除逾期
	and customer_no not in('103883','103167','105673','104352','104662','104514','104746','104172')
	--and customer_no not in('104055','106463')
	--6月签呈客户仅6月剔除逾期，其中 106463 从8月开始不剔除，106765 从12月开始不剔除，105240一直剔除
	--and customer_no not in('105157','107570','106905','104484','109382','106463','106765')
	--and customer_no not in('106463','106765')
	and customer_no not in('106765')
	and customer_no not in('105240')
	--678共3个月因财务对账不考核逾期
	--and customer_no not in('105527', '104445', '102202','100563','PF1206','102751','104775','103309','104116','PF0424','103183','102890',
	--						'102890','PF0320','105618','103320','104725','PF0094','103374','103772','PF1205','103094','104430','104519',
	--						'104478','103876','103782','104335','102790')
	--7月签呈，7、8、9共3个月不算逾期
	--and customer_no not in('106626','106997','111383','105169','105672','106652','105543','110679','105254','108773')
	--7月签呈，第1行仅7月剔除，第2行8月看情况，第3行历史问题已断约不考核逾期，第4行仅7月剔除逾期且不算提成
	----and customer_no not in('105493','105758','105832','105994','107015','111417')
							--'104677','111865','105528','105381','104867','107986','105717',
							--'107065','108096','108452','107851','106811','105572',
							--'105493','105758','105832','105994','107015','111417',
							--'113108','113067','110656','111837','111296','105202')
	--7月坏账签呈中，105601客户8月逾期剔除
	----and customer_no not in('105601')
	--8月签呈，其中107181客户8-9月剔除逾期，其他仅剔除8月
	--and customer_no not in('107181','105669','111905','110661','110677','110682','107459')
	----and customer_no not in('107181')
	--9月签呈 四川 算到业务代理人，每月剔除逾期和销售
	and customer_no not in('104179','112092')
	--9月签呈 安徽 已断约每月剔除? '106997'、'105169'9月有销售
	and customer_no not in('104352','105493','105758','105832','105994','107015','106626','106997','111383','105169','106652','105254','108773')
	--9月签呈 重庆 合伙人客户，9月剔除逾期和销售
	--and customer_no not in('114265','114248','114401','111933','113080','113392')
	--9月签呈 重庆 剔除9月逾期，其中'109484'剔除9月的逾期和销售
	--and customer_no not in('109484','107790','110664')	
	--9月签呈 江苏、贵州、四川 剔除9月逾期，其中'104268'10月也剔除
	--and customer_no not in('104268')
	--and customer_no not in('107621','109342','109403','109671','113154','113486','113762','114145','114482','111553','111559','111589',
	--						'111617','111618','111621','111622','111630','111632','111641','111643','113934','108797','104268','103997')	
	--10月签呈 当月剔除逾期系数
	--and customer_no not in('105090','111935','110664','109293','111534','111810','112199','112201','112361','112874')	
	group by channel,customer_no,customer_name,company_code,company_name)a
group by channel,customer_no,customer_name;



--销售员逾期系数
drop table csx_tmp.temp_salesname_over_rate;
create table csx_tmp.temp_salesname_over_rate
as
select 
	a.channel,	-- 渠道
	b.work_no,	-- 销售员工号
	b.sales_name,	-- 销售员
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when over_amt>=0 and receivable_amount>0 then over_amt else 0 end) over_amt,	-- 逾期金额
	--sum(case when over_amt>=0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	--sum(case when receivable_amount>=0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数
	sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数	
    coalesce(round(case  when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
				else coalesce(sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end), 0)
				/(sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end)) end
		  , 6),0) over_rate 	-- 逾期系数
from
	(select
		channel,
		customer_no,
		customer_name,
		--account_period_code,
		--COALESCE(account_period_val,0) account_period_val,
		--account_period_name,
		company_code,
		company_name ,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from csx_tmp.tmp_cust_order_overdue_dtl a 
	where channel = '大客户' and sdt = ${hiveconf:i_sdate_11} 
	--签呈客户不考核，不算提成,因此不算逾期
	and customer_no not in('111118','103717','102755','104023','105673','104402')
	and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
	--签呈客户仅4月不考核，不算提成，4-6月不算逾期
	--and customer_no not in('PF0320','105177')
	--5月签呈客户5月剔除逾期，前者剔除逾期，后者仅5月剔除逾期
	and customer_no not in('103883','103167','105673','104352','104662','104514','104746','104172')
	--and customer_no not in('104055','106463')
	--6月签呈客户仅6月剔除逾期，其中 106463 从8月开始不剔除，106765 从12月开始不剔除，105240一直剔除
	--and customer_no not in('105157','107570','106905','104484','109382','106463','106765')
	--and customer_no not in('106463','106765')
	and customer_no not in('106765')
	and customer_no not in('105240')
	--678共3个月因财务对账不考核逾期
	--and customer_no not in('105527', '104445', '102202','100563','PF1206','102751','104775','103309','104116','PF0424','103183','102890',
	--						'102890','PF0320','105618','103320','104725','PF0094','103374','103772','PF1205','103094','104430','104519',
	--						'104478','103876','103782','104335','102790')
	--7月签呈，7、8、9共3个月不算逾期
	--and customer_no not in('106626','106997','111383','105169','105672','106652','105543','110679','105254','108773')
	--7月签呈，第1行仅7月剔除，第2行8月看情况，第3行历史问题已断约不考核逾期，第4行仅7月剔除逾期且不算提成
	----and customer_no not in('105493','105758','105832','105994','107015','111417')
							--'104677','111865','105528','105381','104867','107986','105717',
							--'107065','108096','108452','107851','106811','105572',
							--'105493','105758','105832','105994','107015','111417',
							--'113108','113067','110656','111837','111296','105202')
	--7月坏账签呈中，105601客户8月逾期剔除
	----and customer_no not in('105601')
	--8月签呈，其中107181客户8-9月剔除逾期，其他仅剔除8月
	--and customer_no not in('107181','105669','111905','110661','110677','110682','107459')
	----and customer_no not in('107181')
	--9月签呈 四川 算到业务代理人，每月剔除逾期和销售
	and customer_no not in('104179','112092')
	--9月签呈 安徽 已断约每月剔除? '106997'、'105169'9月有销售
	and customer_no not in('104352','105493','105758','105832','105994','107015','106626','106997','111383','105169','106652','105254','108773')
	--9月签呈 重庆 合伙人客户，9月剔除逾期和销售
	--and customer_no not in('114265','114248','114401','111933','113080','113392')
	--9月签呈 重庆 剔除9月逾期，其中'109484'剔除9月的逾期和销售
	--and customer_no not in('109484','107790','110664')	
	--9月签呈 江苏、贵州、四川 剔除9月逾期，其中'104268'10月也剔除
	--and customer_no not in('104268')
	--and customer_no not in('107621','109342','109403','109671','113154','113486','113762','114145','114482','111553','111559','111589',
	--						'111617','111618','111621','111622','111630','111632','111641','111643','113934','108797','104268','103997')
	--10月签呈 当月剔除逾期系数
	--and customer_no not in('105090','111935','110664','109293','111534','111810','112199','112201','112361','112874')	
	group by channel,customer_no,customer_name,company_code,company_name)a
join (select * from csx_dw.dws_crm_w_a_customer_m_v1 where sdt=${hiveconf:i_sdate_11} and attribute_code <> 5) b on b.customer_no=a.customer_no
group by a.channel,b.work_no,b.sales_name;





--截至某天的订单应收明细
insert overwrite directory '/tmp/raoyanhua/ysmx' row format delimited fields terminated by '\t'
select 
	b.sales_province,	-- 省区
	b.work_no,	-- 销售员工号
	b.sales_name,	-- 销售员
	c.account_period_code,	-- 最新账期编码
	if(c.account_period_code like 'Y%',if(c.account_period_val=31,45,c.account_period_val+15),c.account_period_val) account_period_val,	-- 最新帐期天数
	a.*,
	if(a.over_days>0,'逾期','未逾期') is_overdue	
from
	(select *
	from csx_tmp.tmp_cust_order_overdue_dtl  
	where channel = '大客户' and sdt = ${hiveconf:i_sdate_11} 
	--签呈客户不考核，不算提成
	and customer_no not in('111118','103717','102755','104023','105673','104402')
	and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
	)a 
join (select * from csx_dw.dws_crm_w_a_customer_m_v1 where sdt=${hiveconf:i_sdate_11} and attribute_code <> 5) b on b.customer_no=a.customer_no
left join
	(select
		customer_number,
		company_code,
		payment_terms account_period_code,
		case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
			 else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
		COALESCE(cast(payment_days as int),0) account_period_val
	from csx_dw.dws_crm_r_a_customer_account_day a
	where sdt='current'
	and customer_number<>''
	)c on (a.customer_no=c.customer_number and a.company_code=c.company_code)
;


