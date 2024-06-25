-- 财务客户损益模型表
-- 核心逻辑： 统计当月客户销售情况、应收金额、资金占用费（客户应收） 
-- 20210825 因运费取不到BBC，所以销售与应收都只要大客户的（期初订单都作为大客户）；单日资金占用费=（（当日应收+前一日应收）/2）*0.06/365
/*说明：
1、数据源为B端客户（因运费取不到BBC，所以各项均不含bbc业务，期初订单默认为B端业务）的销售、应收、运费数据，其中除应收金额外，均为未税金额
2、只取销售额大于0客户数据，剔除项目供应商
3、计算单日各项金额后，月度数据为每日数据汇总，其中 单日资金占用费=（（当日应收+前一日应收）/2）*0.06/365
4、未税运费：将物流运费根据车辆物流对应到订单，订单对应到客户，统计客户运费
5、履约利润=未税定价毛利额-未税运费
6、净利润=履约利润-资金占用费=未税毛利-未税运费-资金占用费
7、未税定价毛利率=未税定价毛利额/未税销售金额*100%
8、履约毛利率=履约利润/未税销售金额*100%
9、净利润率=（履约利润-资金占用费）/未税销售金额*100%


1、取数范围：
BBC快递配送运费+B端装车配送费（TMS中运单类型为BBC的装车运费），派车没办法区分业务类型，所以本次不在取数范围；
2、B端装车费用的取数口径：订单口径为：1；运单类型为BBC；
3、BBC快递配送运费取数口径：通过销售订单，去查原销售单；06销售出库单单号做运费聚合
应收数据取数逻辑：订单关联业务类型，关联不到去信控业务类型，取不到的时候放到日配；---历史数据中信控号对应业务类型与订单业务类型会存在不一致情况，所以历史的客户信控应收报表和损益报表取得应收金额存在差异，但是新的数据不会存在此类差异；
4、BBC数据费用底表落库：预计27号完成；
报损也是需要加在单客损益报表里面的，属于费用项目，需要在净利润里面剔除掉

单客损益加入运费指标：
1、单客层面的装车金额（可以直接取派车收入---对接葛堃）和未装车金额；合计金额是出库金额，销售金额是签收口径


1、装车金额、未装车金额按业态展示：分业态时只展示装车金额，不分业态时展示装车金额、未装车金额。取数口径与当前物流费比分析看板一致。
2、单客损益不需要展示串点，已与需求方达成一致。



select *
from csx_dwd.csx_dwd_csx_b2b_tms_tms_transport_bbc_jd_share_df 
where sdt = '20231128' 


order_type_code	INT 运单类型码 1-b端 2-m端
-- 20231222改动 第一个临时表排序加业务类型编码，csx_dws_sss_order_credit_invoice_bill_settle_detail_di
-- 20240221 调整：1、BBC快递费改取数字段，以账单结算结束时间为准；2、BBC快递费增加字段单独呈现；3、应收金额切数据源由中台变为SAP取数；
*/
-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=2000;
SET hive.optimize.sort.dynamic.partition=true;

-- 负载均衡
SET hive.groupby.skewindata=false;
SET hive.map.aggr = true;

-- 允许使用正则
SET hive.support.quoted.identifiers=none;

set hive.tez.container.size=8192;
set hive.merge.tezfiles=false;
set hive.cbo.enable=false;



-- 临时表1：销售表订单业务类型
drop table csx_analyse_tmp.tmp_sale_order_business_type;
create temporary table csx_analyse_tmp.tmp_sale_order_business_type
as
select 
  order_code_new,business_type_code,business_type_name,customer_code
from 
(
  select 
    order_code_new,business_type_code,business_type_name,customer_code,
    row_number() over(partition by order_code_new order by customer_code,business_type_code asc) as rank_num
  from 
  (
    select distinct -- order_code,
    	case when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-2)
    		when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-1)
    		else split(order_code,'-')[0]
    		end as order_code_new,
    	business_type_code,business_type_name,customer_code	
    from csx_dws.csx_dws_sale_detail_di
    where sdt>='20200101'
  )a
)a
where rank_num=1;		  


-- 临时表1：客户上上月至今每日的应收金额
drop table csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_1;
create temporary table csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_1
as
select
  sdt,
  customer_code,
  business_type_code,
  business_type_name,
  -- if(sum(receivable_amount)<0,0,sum(receivable_amount)) as receivable_amount,	-- 应收金额
  sum(receivable_amount) as receivable_amount,	-- 应收金额
  sum(residue_amt_sss) as residue_amt_sss	-- 剩余金额 认领未核销金额  
from 
(
select
  a.sdt,
  a.customer_code,
  coalesce(b.business_type_code,1) as business_type_code,
  coalesce(b.business_type_name,'日配业务') as business_type_name,
  -- a.company_code,	-- 签约公司编码
  sum(a.unpaid_amount) as receivable_amount,	-- 应收金额
  0 as residue_amt_sss	-- 剩余金额 认领未核销金额
from  
  (
    select 
		case when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-2)
			when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-1)
			else split(source_bill_no,'-')[0]
			end as source_bill_no_new,
    	-- source_bill_no as order_no,	-- 来源单号
    	customer_code,	-- 客户编码
    	company_code,	-- 签约公司编码
    	happen_date,	-- 发生时间		
    	overdue_date,	-- 逾期时间	
    	order_amt,	-- 源单据对账金额
    	money_back_status,	-- 回款状态
    	unpaid_amount,	-- 未回款金额
    	account_period_code,	-- 账期编码 
    	account_period_name,	-- 账期名称 
    	account_period_value,	-- 账期值
    	source_sys,	-- 来源系统 MALL b端销售 BBC bbc端 BEGIN 期初
    	bad_debt_amount,
    	sdt
    from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di  -- 销售结算对账开票结算详情表
    where sdt>=regexp_replace(last_day(add_months('${sdt_yes_date}',-2)),'-','')
    and regexp_replace(date(happen_date),'-','')<=sdt
  )a
left join csx_analyse_tmp.tmp_sale_order_business_type b on b.order_code_new=a.source_bill_no_new
-- where b.channel_code<>'7' or a.beginning_mark='0'  -- 剔除BBC订单 
	-- where source_sys<>'BBC'
group by a.sdt,a.customer_code,
  coalesce(b.business_type_code,1),
  coalesce(b.business_type_name,'日配业务')
  
-- int 业务类型编码 (1.日配业务,2.福利业务,3.批发内购,4.城市服务商,5.省区大宗,6.bbc,7.大宗一部,8.大宗二部,9.商超) 
-- string credit_business_attribute_code 信控业务属性编码 (1.日配,2.福利,3.大宗贸易,4.M端,5.BBC,6.内购) 

-- int 业务类型编码 (1.日配业务,2.福利业务,3.批发内购,4.城市服务商,5.省区大宗,6.bbc,7.大宗一部,8.大宗二部,9.商超) 
-- string credit_business_attribute_code 信控业务属性编码 (1.日配,2.福利,3.大宗贸易,4.M端,5.BBC,6.内购) 

union all
select 
sdt,
customer_code,
case credit_business_attribute_code
when null then 1
when '' then 1
when '1' then 1
when '2' then 2
when '3' then 3
when '4' then 9
when '5' then 6
when '6' then 3 else 1 end as business_type_code,

case credit_business_attribute_code
when null  then '日配业务'
when '' then '日配业务'
when '1' then '日配业务'
when '2' then '福利业务'
when '3' then '省区大宗'
when '4' then '商超'
when '5' then 'BBC'
when '6' then '批发内购' else '日配业务' end as business_type_name,
0-residue_amt_sss as receivable_amount,	-- 应收金额
residue_amt_sss as residue_amt_sss   -- 剩余金额 认领未核销金额
from csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
where sdt>=regexp_replace(last_day(add_months('${sdt_yes_date}',-2)),'-','')
and residue_amt_sss<>0
)a
group by sdt,customer_code,business_type_code,business_type_name;	





-- 临时表2：客户上上月至今每日的当日应收金额、昨日应收金额、资金占用费
drop table csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_2;
create temporary table csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_2
as
select sdt,customer_code,
  business_type_code,
  business_type_name,
  coalesce(sum(residue_amt_sss),0) as residue_amt_sss,	-- 剩余金额 认领未核销金额 
  coalesce(sum(receivable_amount),0) as receivable_amount,  -- 当日应收
  coalesce(sum(receivable_amount_last),0) as receivable_amount_last,   -- 前1日应收
  ((coalesce(if(sum(receivable_amount)>0,sum(receivable_amount),0),0)
		+coalesce(if(sum(receivable_amount_last)>0,sum(receivable_amount_last),0),0))/2)*0.06/365 capital_takes_up      -- 资金占用费
from
(
  select sdt,customer_code,business_type_code,business_type_name,receivable_amount,0 receivable_amount_last,residue_amt_sss
  from csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_1 
  union all
  select regexp_replace(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-1),'-','') as sdt,
    customer_code,business_type_code,business_type_name,0 receivable_amount,receivable_amount as receivable_amount_last,0 residue_amt_sss
  from csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_1
)a
where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and sdt<='${sdt_yes}'
group by sdt,customer_code,business_type_code,business_type_name;


-- 临时表3：客户上月至今每日的销售  不含税销售额 大客户不含BBC
drop table csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_3;
create temporary table csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_3
as
select 
  coalesce(b.performance_region_code,e.performance_region_code,'99') as performance_region_code,
  coalesce(b.performance_region_name,e.performance_region_name,'其他') as performance_region_name,
  coalesce(b.performance_province_code,e.performance_province_code,'99') as performance_province_code,
  coalesce(b.performance_province_name,e.performance_province_name,'其他') as performance_province_name,
  coalesce(b.performance_city_code,e.performance_city_code,'99') as performance_city_code,
  coalesce(b.performance_city_name,e.performance_city_name,'其他') as performance_city_name,  
  coalesce(b.channel_code,case when f.shop_name is not null then '2' end) as channel_code,
  coalesce(b.channel_name,case when f.shop_name is not null then '商超' end) as channel_name, 
  a.sdt,a.customer_code,
  coalesce(b.customer_name,f.shop_name) as customer_name,
  b.first_category_code,
  b.first_category_name,
  b.second_category_code,
  b.second_category_name,
  b.third_category_code,
  b.third_category_name, 
  a.business_type_code,a.business_type_name,  
  b.sales_user_id,
  b.sales_user_number,
  b.sales_user_name, 
  sum(sale_amt_no_tax) sale_amt_no_tax,
  sum(sale_cost_no_tax) sale_cost_no_tax,
  sum(profit_no_tax) profit_no_tax  
from 
  (
   select 
     sdt,customer_code,
	 business_type_code,business_type_name,
     case when channel_code='2' then channel_code end as channel_code,
     case when channel_code='2' then inventory_dc_code end as inventory_dc_code,
     sum(sale_amt_no_tax) sale_amt_no_tax,
	 sum(sale_cost_no_tax) sale_cost_no_tax,
     sum(profit_no_tax) profit_no_tax
   from csx_dws.csx_dws_sale_detail_di
   where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
   and sdt<='${sdt_yes}' 
   -- and channel_code in('1','9')
   -- and channel_code<>'7'   -- 剔除BBC订单 
   group by
     sdt,customer_code,
	 business_type_code,business_type_name,
     case when channel_code='2' then channel_code end,
  case when channel_code='2' then inventory_dc_code end
  )a
left join
  ( -- 获取客户信息
    select
      performance_region_code,
  	  performance_region_name,
  	  performance_province_code,
      performance_province_name,
  	  performance_city_code,
  	  performance_city_name,
      channel_code,
      channel_name,
      customer_code,
      customer_name,
      first_category_code,
      first_category_name,
      second_category_code,
      second_category_name,
      third_category_code,
      third_category_name,	  
      sales_user_id,
      sales_user_number,
      sales_user_name
    from csx_dim.csx_dim_crm_customer_info
    where sdt = 'current'
  ) b on a.customer_code = b.customer_code
left join
  ( -- 获取商超客户信息:省区、城市
    select 
      performance_region_code as performance_region_code,
      performance_region_name as performance_region_name,
      performance_province_code as performance_province_code,
      performance_province_name as performance_province_name,
      performance_city_code performance_city_code,
      performance_city_name performance_city_name,
      shop_code,
      shop_name 
    from csx_dim.csx_dim_shop a 
    where sdt='current'
  )e on a.inventory_dc_code= e.shop_code
left join
  ( -- 获取商超客户信息:客户名称、门店名称
    select 
      performance_region_code as performance_region_code,
      performance_region_name as performance_region_name,
      performance_province_code as performance_province_code,
      performance_province_name as performance_province_name,
      performance_city_code performance_city_code,
      performance_city_name performance_city_name,
  	  shop_code,
  	  shop_name 
    from csx_dim.csx_dim_shop a 
    where sdt='current'
  )f on substr(a.customer_code,2,4)= f.shop_code
group by 
  coalesce(b.performance_region_code,e.performance_region_code,'99'),
  coalesce(b.performance_region_name,e.performance_region_name,'其他'),
  coalesce(b.performance_province_code,e.performance_province_code,'99'),
  coalesce(b.performance_province_name,e.performance_province_name,'其他'),
  coalesce(b.performance_city_code,e.performance_city_code,'99'),
  coalesce(b.performance_city_name,e.performance_city_name,'其他'),  
  coalesce(b.channel_code,case when f.shop_name is not null then '2' end),
  coalesce(b.channel_name,case when f.shop_name is not null then '商超' end), 
  a.sdt,a.customer_code,
  coalesce(b.customer_name,f.shop_name),
  b.first_category_code,
  b.first_category_name,
  b.second_category_code,
  b.second_category_name,
  b.third_category_code,
  b.third_category_name,
  a.business_type_code,a.business_type_name,  
  b.sales_user_id,
  b.sales_user_number,
  b.sales_user_name;




-- 临时表3.5：各项指标
drop table csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_35;
create temporary table csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_35
as
    select 
	    sdt,customer_code,
		business_type_code,business_type_name,
	    sum(receivable_amount) receivable_amount,		 -- 应收金额
	    sum(receivable_amount_last) receivable_amount_last,		 -- 前1日应收
	    sum(capital_takes_up) capital_takes_up,      -- 资金占用费
		sum(residue_amt_sss) residue_amt_sss,	-- 剩余金额 认领未核销金额
	    sum(transport_amount) transport_amount,       -- 运费
		sum(bbc_express_amount) bbc_express_amount,      -- bbc快递费	单独字段
	    sum(frmloss_amt) frmloss_amt,      -- 报损金额
	    sum(order_amount_no_tax) order_amount_no_tax,      -- 装车金额（未税）
	    sum(exclude_order_amount_no_tax) exclude_order_amount_no_tax      -- 未装车的金额（未税）			
    from 
    (	  
        -- 客户上月至今每日的当日应收金额、昨日应收金额、资金占用费
      select
        sdt,customer_code,
		business_type_code,business_type_name,
        receivable_amount,		 -- 应收金额
        receivable_amount_last,   -- 前1日应收
		  capital_takes_up,      -- 资金占用费
		  residue_amt_sss,	-- 剩余金额 认领未核销金额
		  0 transport_amount,      -- 运费
		  0 as bbc_express_amount,      -- bbc快递费	单独字段
		  0 as frmloss_amt,      -- 报损金额	
	      0 as order_amount_no_tax,      -- 装车金额（未税）
	      0 as exclude_order_amount_no_tax      -- 未装车的金额（未税）	
      from csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_2
      -- 客户上月至今每日的运费
      union all
      select 
         a2.sdt,a2.customer_no,
         if(shipped_type_code = '4',6,if(access_caliber=2,99,coalesce(b.business_type_code,1))) as business_type_code,
         if(shipped_type_code = '4','BBC',if(access_caliber=2,'其他',coalesce(b.business_type_name,'日配业务'))) as business_type_name,		  
	      0 as receivable_amount,		 -- 应收金额
	      0 receivable_amount_last,   -- 前1日应收
	      0 as capital_takes_up,      -- 资金占用费
		  0 as residue_amt_sss,	-- 剩余金额 认领未核销金额
	      a2.transport_amount,      -- 运费
		  0 as bbc_express_amount,      -- bbc快递费	单独字段
	      0 as frmloss_amt,      -- 报损金额	
	      0 as order_amount_no_tax,      -- 装车金额（未税）
	      0 as exclude_order_amount_no_tax      -- 未装车的金额（未税）			  
      from
		(
		select regexp_replace(send_date,'-','') as sdt,
			customer_code as customer_no, -- dc_code,
			access_caliber,
			shipped_order_code,
			shipped_type_code,
			sum(total_amount_tax_encluded) as transport_amount      -- 未税运费
		from csx_dws.csx_dws_tms_entrucking_order_detail_di a
		left semi
		join 
			(
			select * 
			from csx_dwd.csx_dwd_tms_entrucking_order_di
			where status_code != 100
			)b on a.entrucking_code = b.entrucking_code             
		where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
		and regexp_replace(send_date,'-','')>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
		and regexp_replace(send_date,'-','')<='${sdt_yes}'
		and access_caliber<>3
		group by regexp_replace(send_date,'-',''),customer_code,access_caliber,shipped_order_code,shipped_type_code
		)a2 
	  left join csx_analyse_tmp.tmp_sale_order_business_type b on b.order_code_new=a2.shipped_order_code
	  
		-- 客户上月至今每日的运费 BBC  签收时间signing_time改 	账单所属期间结束bill_belongs_end
	  union all
	  select 
         regexp_replace(substr(bill_belongs_end,1,10),'-','') as sdt,customer_code,
         6 as business_type_code,
         'BBC' as business_type_name,		  
	      0 as receivable_amount,		 -- 应收金额
	      0 receivable_amount_last,   -- 前1日应收
	      0 as capital_takes_up,      -- 资金占用费
		  0 as residue_amt_sss,	-- 剩余金额 认领未核销金额
	      cast(settlement_amount as decimal(20,6))/1.06 as transport_amount,      -- 运费	结算金额
		  cast(settlement_amount as decimal(20,6))/1.06 as bbc_express_amount,      -- bbc快递费	单独字段
		  0 as frmloss_amt,      -- 报损金额
	      0 as order_amount_no_tax,      -- 装车金额（未税）
	      0 as exclude_order_amount_no_tax      -- 未装车的金额（未税）
	  from csx_dwd.csx_dwd_csx_b2b_tms_tms_transport_bbc_jd_share_df 
	  where sdt='${sdt_yes}'
	  and regexp_replace(substr(bill_belongs_end,1,10),'-','')>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
	  and regexp_replace(substr(bill_belongs_end,1,10),'-','')<='${sdt_yes}'
	  
		-- 客户上月至今每日的报损
	  union all
	  select 
         a.sdt,c.customer_code,
         c.business_type_code,
         c.business_type_name,		  
	      0 as receivable_amount,		 -- 应收金额
	      0 receivable_amount_last,   -- 前1日应收
	      0 as capital_takes_up,      -- 资金占用费
		  0 as residue_amt_sss,	-- 剩余金额 认领未核销金额
	      0 as transport_amount,      -- 运费
		  0 as bbc_express_amount,      -- bbc快递费	单独字段
		  frmloss_amt_no_tax as frmloss_amt,      -- 报损商品金额
	      0 as order_amount_no_tax,      -- 装车金额（未税）
	      0 as exclude_order_amount_no_tax      -- 未装车的金额（未税）	
	  from
	  (
	    select sdt,entry_order_code,
	    frmloss_amt,      -- 报损商品金额
	    frmloss_amt_no_tax      -- 不含税报损商品金额
	    from csx_dwd.csx_dwd_wms_frmloss_order_detail_di
	    where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')  -- sdt为报损单创建日期
	    and sdt<='${sdt_yes}'
	    -- and order_code = 'BS231129000386' 
	    and `status`!=2
		and frmloss_type_code='84'   -- frmloss_type_name='无实物退货报损'
	  )a
	  left join 
	  (
        select original_order_code,order_code,
    	  case when sale_channel='5' and substr(split(original_order_code,'-')[0],1,1)='B' and substr(split(original_order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(original_order_code,'-')[0],2,length(split(original_order_code,'-')[0])-2)
    		when sale_channel='5' and substr(split(original_order_code,'-')[0],1,1)='B' and substr(split(original_order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(original_order_code,'-')[0],2,length(split(original_order_code,'-')[0])-1)
    		else split(original_order_code,'-')[0]
    		end as original_order_code_new		
	    from csx_dwd.csx_dwd_wms_entry_order_header_di
	    where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-5),'-','')  -- sdt关单时间
		and sdt<='${sdt_yes}'
	  -- and order_code = 'RCO23112900000190' 
	  )b on a.entry_order_code=b.order_code
	  left join csx_analyse_tmp.tmp_sale_order_business_type c on b.original_order_code_new=c.order_code_new
	  where c.order_code_new is not null


		-- 客户上月至今每日的装车金额、未装车的金额	  
	union all
	select
         a.sdt,
		 a.customer_code,
         coalesce(c.business_type_code,d.business_type_code) as business_type_code,
         coalesce(c.business_type_name,d.business_type_name) as business_type_name,		  
	      0 as receivable_amount,		 -- 应收金额
	      0 receivable_amount_last,   -- 前1日应收
	      0 as capital_takes_up,      -- 资金占用费
		  0 as residue_amt_sss,	-- 剩余金额 认领未核销金额
	      0 as transport_amount,      -- 运费
		  0 as bbc_express_amount,      -- bbc快递费	单独字段
	      0 as frmloss_amt,      -- 报损商品金额
	      order_amount_no_tax,      -- 装车金额（未税）
	      exclude_order_amount_no_tax      -- 未装车的金额（未税）	
	from 
	(		
	    select sdt,order_code,customer_code,
			order_amount_no_tax,      -- 装车金额（未税）
			exclude_order_amount_no_tax      -- 未装车的金额（未税）
	    from csx_report.csx_report_tms_bangdan_customer_delivery_order_info_di	
	    where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')  
	    and sdt<='${sdt_yes}'		
	)a		
	  left join csx_analyse_tmp.tmp_sale_order_business_type c on a.order_code=c.order_code_new
	  left join 
	  (
		select distinct
			order_code,customer_code,
			case 
			when partner_type_code<>0 then 4
			when order_business_type_code=1 then 1
			when order_business_type_code=2 then 2
			when order_business_type_code=3 then 5
			when order_business_type_code=4 then 3 end as business_type_code,
			
			case 
			when partner_type_code<>0 then '项目供应商'
			when order_business_type_code=1 then '日配业务'
			when order_business_type_code=2 then '福利业务'
			when order_business_type_code=3 then '省区大宗'
			when order_business_type_code=4 then '批发内购' end as business_type_name
		from csx_dwd.csx_dwd_oms_sale_order_detail_di
	    where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-2),'-','')  
	    and sdt<='${sdt_yes}'
		and order_status_code<>-1  -- 订单状态 -1-已取消	
	  )d on a.order_code=d.order_code	
	  )a 
	  group by sdt,customer_code,business_type_code,business_type_name;
	  
	  
	  
	  
-- 临时表4：客户销售、应收、运费、不含客户等级
drop table csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_4;
create temporary table csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_4
as
select
  concat_ws('-',coalesce(a.sdt,b.sdt),coalesce(a.customer_code,b.customer_code),
		coalesce(a.performance_city_code,b.performance_city_code),
		cast(coalesce(a.business_type_code,b.business_type_code) as string)) as biz_id, 
  coalesce(a.performance_region_code,b.performance_region_code,'99') as performance_region_code,
  coalesce(a.performance_region_name,b.performance_region_name,'其他') as performance_region_name,
  coalesce(a.performance_province_code,b.performance_province_code,'99') as performance_province_code,
  coalesce(a.performance_province_name,b.performance_province_name,'其他') as performance_province_name,
  coalesce(a.performance_city_code,b.performance_city_code,'99') as performance_city_code,
  coalesce(a.performance_city_name,b.performance_city_name,'其他') as performance_city_name,  
  coalesce(a.channel_code,b.channel_code) as channel_code,
  coalesce(a.channel_name,b.channel_name) as channel_name,
  coalesce(a.customer_code,b.customer_code) customer_code,
  coalesce(a.customer_name,b.customer_name) as customer_name,
  coalesce(a.first_category_code,b.first_category_code) as first_category_code,
  coalesce(a.first_category_name,b.first_category_name) as first_category_name,
  coalesce(a.second_category_code,b.second_category_code) as second_category_code,
  coalesce(a.second_category_name,b.second_category_name) as second_category_name,
  coalesce(a.third_category_code,b.third_category_code) as third_category_code,
  coalesce(a.third_category_name,b.third_category_name) as third_category_name, 
  coalesce(a.sales_user_id,b.sales_user_id) sales_user_id,
  coalesce(a.sales_user_number,b.sales_user_number) sales_user_number,
  coalesce(a.sales_user_name,b.sales_user_name) sales_user_name,
  a.sale_amt_no_tax,
  a.sale_cost_no_tax,
  a.profit_no_tax,   
  coalesce(b.receivable_amount,0) receivable_amount,		 -- 应收金额
  coalesce(b.receivable_amount_last,0) receivable_amount_last,		 -- 前1日应收
  coalesce(b.capital_takes_up,0) capital_takes_up,      -- 资金占用费
  coalesce(residue_amt_sss) residue_amt_sss,	-- 剩余金额 认领未核销金额
  coalesce(b.transport_amount,0) transport_amount,      -- 未税运费
  coalesce(b.bbc_express_amount,0) bbc_express_amount,      -- bbc快递费	单独字段
  (coalesce(a.profit_no_tax,0)-coalesce(b.transport_amount,0)-coalesce(b.frmloss_amt,0)) performance_profit,
  (coalesce(a.profit_no_tax,0)-coalesce(b.transport_amount,0)-coalesce(b.frmloss_amt,0))-coalesce(b.capital_takes_up,0) net_profit,
  from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as update_time,
  coalesce(a.business_type_code,b.business_type_code) as business_type_code,
  coalesce(a.business_type_name,b.business_type_name) as business_type_name, 
  coalesce(b.frmloss_amt,0) frmloss_amt,      -- 报损金额  
  coalesce(b.order_amount_no_tax,0) order_amount_no_tax,      -- 装车金额（未税）
  coalesce(b.exclude_order_amount_no_tax,0) exclude_order_amount_no_tax,      -- 未装车的金额（未税）  
  coalesce(a.sdt,b.sdt) sdt -- 统计日期 
from csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_3 a
full join
(
  select a1.sdt,
	  coalesce(b.performance_region_code,'99') as performance_region_code,
	  coalesce(b.performance_region_name,'其他') as performance_region_name,
	  coalesce(b.performance_province_code,'99') as performance_province_code,
	  coalesce(b.performance_province_name,'其他') as performance_province_name,
	  coalesce(b.performance_city_code,'99') as performance_city_code,
	  coalesce(b.performance_city_name,'其他') as performance_city_name,  
	  b.channel_code,
	  b.channel_name, 	
	  a1.customer_code,
	  b.customer_name,
	  a1.business_type_code,a1.business_type_name,
      b.first_category_code,
      b.first_category_name,
      b.second_category_code,
      b.second_category_name,
      b.third_category_code,
      b.third_category_name,	  
	  b.sales_user_id,
	  b.sales_user_number,
	  b.sales_user_name,
	  a1.receivable_amount,		 -- 应收金额
	  a1.receivable_amount_last,		 -- 前1日应收
	  a1.capital_takes_up,      -- 资金占用费
	  a1.residue_amt_sss,	-- 剩余金额 认领未核销金额
	  a1.transport_amount,       -- 运费
	  a1.bbc_express_amount,      -- bbc快递费	单独字段
	  a1.frmloss_amt,      -- 报损金额 
	  a1.order_amount_no_tax,      -- 装车金额（未税）
	  a1.exclude_order_amount_no_tax      -- 未装车的金额（未税） 
  from csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_35 a1 
  left join
  ( -- 获取客户信息
    select
      performance_region_code,
  	  performance_region_name,
  	  performance_province_code,
      performance_province_name,
  	  performance_city_code,
  	  performance_city_name,
      channel_code,
      channel_name,
      customer_code,
      customer_name,
      first_category_code,
      first_category_name,
      second_category_code,
      second_category_name,
      third_category_code,
      third_category_name,
      sales_user_id,
      sales_user_number,
      sales_user_name
    from csx_dim.csx_dim_crm_customer_info
    where sdt = 'current'
	and customer_code <>''
  )b on a1.customer_code = b.customer_code
)b on a.customer_code=b.customer_code and a.sdt=b.sdt and a.business_type_code=b.business_type_code
;




-- 结果表
insert overwrite table csx_analyse.csx_analyse_report_sss_customer_capital_takes_up_di partition(sdt)
select
	a.biz_id,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.channel_code,
	a.channel_name,
	coalesce(c.sales_channel_name,a.channel_name) sales_channel_name,
	a.customer_code,
	a.customer_name,
	a.first_category_code,
	a.first_category_name,
	a.second_category_code,
	a.second_category_name,
	a.third_category_code,
	a.third_category_name,
	b.customer_large_level,
	b.customer_small_level,
	a.sales_user_id,
	a.sales_user_number,
	a.sales_user_name,
	a.sale_amt_no_tax,
	a.sale_cost_no_tax,
	a.profit_no_tax,
	a.receivable_amount,
	a.receivable_amount_last,
	a.capital_takes_up,
	a.transport_amount,
	a.performance_profit,
	a.net_profit,
	substr(a.sdt,1,6) smonth,
	d.week_of_year,
	a.update_time,
	a.business_type_code,
	a.business_type_name, 
	a.frmloss_amt,      -- 报损金额
	a.order_amount_no_tax,      -- 装车金额（未税）	
	a.exclude_order_amount_no_tax,      -- 未装车的金额（未税）
	a.bbc_express_amount,      -- bbc快递费	单独字段
	a.residue_amt_sss,	-- 剩余金额 认领未核销金额  	
	a.sdt
from csx_analyse_tmp.tmp_cust_sale_capital_takes_up_di_4 a
left join 
(
	select customer_no,customer_large_level,customer_small_level,month
	from csx_analyse.csx_analyse_report_customer_level_mf
	where month>=substr(regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-',''),1,6) 
)b on a.customer_code=b.customer_no and substr(a.sdt,1,6)=b.month 
-- left join 
-- (
-- 	select distinct customer_code,sales_channel_name,substr(sdt,1,6) smonth
-- 	from csx_analyse.csx_analyse_fr_sap_forecast_collection_report_df  -- 承华  预测回款金额-帆软
-- 	where sdt in(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),'${sdt_yes}')
-- 	and sales_channel_name in('项目供应商','城市服务商')
-- )c on a.customer_code=c.customer_code and substr(a.sdt,1,6)=c.smonth
left join
(
	select *
	from 
	(
		select customer_code,sign_company_code,sales_channel_name,max_sdt,
		row_number() over(partition by customer_code order by max_sdt desc) rno1
		from
		(
			select customer_code,sign_company_code,
				business_type_name as sales_channel_name,
				max(sdt) max_sdt
			from csx_dws.csx_dws_sale_detail_di
			where sdt>='20210101'
			and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			and business_type_code='4'
			group by customer_code,sign_company_code,business_type_name
		)a
	)a
	where rno1=1
)c on c.customer_code=a.customer_code
left join
(
	select calday,week_of_year from csx_dim.csx_dim_basic_date
	where calday>=regexp_replace(date_sub('${sdt_yes_date}',90), '-', '')
	and calday <= regexp_replace('${sdt_yes_date}', '-', '')
) d on a.sdt = d.calday
;


-- 结果表 财务客户损益表_月
------------------------------------------- 

insert overwrite table csx_analyse.csx_analyse_report_sss_customer_capital_takes_up_mi partition(smt)
select 
	concat_ws('-',substr(sdt,1,6),customer_code,performance_city_code,business_type_code) as biz_id,
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	channel_code,
	channel_name,
	customer_code,
	customer_name,
	first_category_code,
	first_category_name,
	second_category_code,
	second_category_name,
	third_category_code,
	third_category_name,
	customer_large_level,
	customer_small_level,
	sales_user_id,
	sales_user_number,
	sales_user_name,
	sum(sale_amt_no_tax) sale_amt_no_tax,
	sum(sale_cost_no_tax) sale_cost_no_tax,
	sum(profit_no_tax) profit_no_tax,
	sum(case when sdt in(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),'${sdt_yes}') then receivable_amount end) as receivable_amount,
	sum(case when sdt in(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),'${sdt_yes}') then receivable_amount_last end) as receivable_amount_last,
	sum(capital_takes_up) as capital_takes_up,
	sum(transport_amount) transport_amount,
	sum(performance_profit) performance_profit,
	sum(net_profit) net_profit,
	cast(min(sdt) as string) as period_start_date,
	cast(max(sdt) as string) as period_end_date,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') update_time,
	business_type_code,
	business_type_name, 
	sum(frmloss_amt) as frmloss_amt,      -- 报损金额
	sum(order_amount_no_tax) as order_amount_no_tax,      -- 装车金额（未税）
	sum(exclude_order_amount_no_tax) as exclude_order_amount_no_tax,      -- 未装车的金额（未税）
	sum(bbc_express_amount) as bbc_express_amount,      -- bbc快递费	单独字段
	sum(residue_amt_sss) as residue_amt_sss,	-- 剩余金额 认领未核销金额  	
	substr(sdt,1,6) smt
from csx_analyse.csx_analyse_report_sss_customer_capital_takes_up_di
where sdt>=regexp_replace(trunc(add_months('${sdt_yes_date}',-1),"MM"),'-','')
and sdt<='${sdt_yes}'
-- and channel_code in('1','9')
-- and sales_channel_name not in('项目供应商','城市服务商')
group by 	
	concat_ws('-',substr(sdt,1,6),customer_code,performance_city_code,business_type_code),
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	channel_code,
	channel_name,
	customer_code,
	customer_name,
	first_category_code,
	first_category_name,
	second_category_code,
	second_category_name,
	third_category_code,
	third_category_name,
	customer_large_level,
	customer_small_level,
	sales_user_id,
	sales_user_number,
	sales_user_name,
	business_type_code,
	business_type_name, 	
	substr(sdt,1,6)
having sum(sale_amt_no_tax)<>0 or sum(transport_amount)<>0 or sum(capital_takes_up)<>0;
	
	

-- 结果导数表 财务客户损益表_日
------------------------------------------- 

insert overwrite table csx_report.csx_report_sss_customer_capital_takes_up_di 	
select 
  biz_id, 
  performance_region_code, 
  performance_region_name, 
  performance_province_code, 
  performance_province_name, 
  performance_city_code, 
  performance_city_name, 
  channel_code, 
  channel_name, 
  customer_code, 
  customer_name, 
  first_category_code,
  first_category_name,
  second_category_code,
  second_category_name,
  third_category_code,
  third_category_name,
  customer_large_level,
  customer_small_level, 
  sales_user_id, 
  sales_user_number, 
  sales_user_name, 
  sale_amt_no_tax, 
  sale_cost_no_tax, 
  profit_no_tax, 
  receivable_amount, 
  receivable_amount_last, 
  capital_takes_up, 
  transport_amount,
  performance_profit,
  net_profit, 
  business_type_code,
  business_type_name, 
  frmloss_amt,      -- 报损金额
  order_amount_no_tax,      -- 装车金额（未税）
  exclude_order_amount_no_tax,      -- 未装车的金额（未税）
  bbc_express_amount,      -- bbc快递费	单独字段
  residue_amt_sss,	-- 剩余金额 认领未核销金额 
  smonth,
  week_of_year,
  update_time,   
  sdt
from  csx_analyse.csx_analyse_report_sss_customer_capital_takes_up_di
where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','');
-- where smt>='${month_bf1}';

	
-- 结果导数表 财务客户损益表_月
------------------------------------------- 

insert overwrite table csx_report.csx_report_sss_customer_capital_takes_up_mi 	
select 
  biz_id, 
  performance_region_code, 
  performance_region_name, 
  performance_province_code, 
  performance_province_name, 
  performance_city_code, 
  performance_city_name, 
  channel_code, 
  channel_name, 
  customer_code, 
  customer_name, 
  first_category_code,
  first_category_name,
  second_category_code,
  second_category_name,
  third_category_code,
  third_category_name,
  customer_large_level,
  customer_small_level, 
  sales_user_id, 
  sales_user_number, 
  sales_user_name, 
  sale_amt_no_tax, 
  sale_cost_no_tax, 
  profit_no_tax, 
  receivable_amount, 
  receivable_amount_last, 
  capital_takes_up, 
  transport_amount,
  performance_profit,
  net_profit, 
  period_start_date,
  period_end_date,
  update_time,
  business_type_code,
  business_type_name, 
  frmloss_amt,      -- 报损金额
  smt,  
  order_amount_no_tax,      -- 装车金额（未税）
  exclude_order_amount_no_tax,      -- 未装车的金额（未税）
  bbc_express_amount,      -- bbc快递费	单独字段
  residue_amt_sss	-- 剩余金额 认领未核销金额  
from  csx_analyse.csx_analyse_report_sss_customer_capital_takes_up_mi
where smt>='${month_bf1}';





/*
---------------------------------------------------------------------------------------------------------
---------------------------------------------hive 建表语句-----------------------------------------------
CREATE TABLE `csx_analyse.csx_analyse_report_sss_customer_capital_takes_up_di`(
  `biz_id` string COMMENT '唯一值',  
  `performance_region_code` string COMMENT '大区编码', 
  `performance_region_name` string COMMENT '大区', 
  `performance_province_code` string COMMENT '省区编码', 
  `performance_province_name` string COMMENT '省区', 
  `performance_city_code` string COMMENT '城市组编码', 
  `performance_city_name` string COMMENT '城市组', 
  `channel_code` string COMMENT '渠道编码', 
  `channel_name` string COMMENT '渠道名称',
  `sales_channel_name` string COMMENT '渠道名称-新',   
  `customer_code` string COMMENT '客户编号', 
  `customer_name` string COMMENT '客户名称', 
  `first_category_code` string COMMENT '一级客户分类编码',
  `first_category_name` string COMMENT '一级客户分类名称',
  `second_category_code` string COMMENT '二级客户分类编码',
  `second_category_name` string COMMENT '二级客户分类名称',
  `third_category_code` string COMMENT '三级客户分类编码',
  `third_category_name` string COMMENT '三级客户分类名称',
  `customer_large_level` string COMMENT '客户大分类',
  `customer_small_level` string COMMENT '客户小分类', 
  `sales_user_id` string COMMENT '销售员编码', 
  `sales_user_number` string COMMENT '销售员工号', 
  `sales_user_name` string COMMENT '销售员', 
  `sale_amt_no_tax` decimal(26,6) COMMENT '未税销售金额', 
  `sale_cost_no_tax` decimal(26,6) COMMENT '未税销售成本', 
  `profit_no_tax` decimal(26,6) COMMENT '未税定价毛利额', 
  `receivable_amount` decimal(26,6) COMMENT '应收金额', 
  `receivable_amount_last` decimal(26,6) COMMENT '前一日应收', 
  `capital_takes_up` decimal(26,6) COMMENT '资金占用费', 
  `transport_amount` decimal(26,6) COMMENT '未税运费',
  `performance_profit` decimal(26,6) COMMENT '履约利润',
  `net_profit` decimal(26,6) COMMENT '净利润',
  `smonth` string COMMENT '月',
  `week_of_year` string COMMENT '周',
  `update_time` timestamp COMMENT '更新时间'
) COMMENT '财务客户损益表_日'
PARTITIONED BY (`sdt` string COMMENT '日期分区')


CREATE TABLE `csx_analyse.csx_analyse_report_sss_customer_capital_takes_up_mi`(
  `biz_id` string COMMENT '唯一值', 
  `performance_region_code` string COMMENT '大区编码', 
  `performance_region_name` string COMMENT '大区', 
  `performance_province_code` string COMMENT '省区编码', 
  `performance_province_name` string COMMENT '省区', 
  `performance_city_code` string COMMENT '城市组编码', 
  `performance_city_name` string COMMENT '城市组', 
  `channel_code` string COMMENT '渠道编码', 
  `channel_name` string COMMENT '渠道编码', 
  `customer_code` string COMMENT '客户编号', 
  `customer_name` string COMMENT '客户名称', 
  `first_category_code` string COMMENT '一级客户分类编码',
  `first_category_name` string COMMENT '一级客户分类名称',
  `second_category_code` string COMMENT '二级客户分类编码',
  `second_category_name` string COMMENT '二级客户分类名称',
  `third_category_code` string COMMENT '三级客户分类编码',
  `third_category_name` string COMMENT '三级客户分类名称',
  `customer_large_level` string COMMENT '客户大分类',
  `customer_small_level` string COMMENT '客户小分类', 
  `sales_user_id` string COMMENT '销售员编码', 
  `sales_user_number` string COMMENT '销售员工号', 
  `sales_user_name` string COMMENT '销售员', 
  `sale_amt_no_tax` decimal(26,6) COMMENT '未税销售金额', 
  `sale_cost_no_tax` decimal(26,6) COMMENT '未税销售成本', 
  `profit_no_tax` decimal(26,6) COMMENT '未税定价毛利额', 
  `receivable_amount` decimal(26,6) COMMENT '应收金额', 
  `receivable_amount_last` decimal(26,6) COMMENT '前一日应收', 
  `capital_takes_up` decimal(26,6) COMMENT '资金占用费', 
  `transport_amount` decimal(26,6) COMMENT '未税运费',
  `performance_profit` decimal(26,6) COMMENT '履约利润',
  `net_profit` decimal(26,6) COMMENT '净利润', 
  `period_start_date` string COMMENT '月开始日期',
  `period_end_date` string COMMENT '月结束日期',
  `update_time` timestamp COMMENT '更新时间'
) COMMENT '财务客户损益表_月'
PARTITIONED BY (`smt` string COMMENT '日期分区')


CREATE TABLE `csx_analyse.csx_analyse_report_sss_customer_capital_takes_up_wi`(
  `biz_id` string COMMENT '唯一值', 
  `performance_region_code` string COMMENT '大区编码', 
  `performance_region_name` string COMMENT '大区', 
  `performance_province_code` string COMMENT '省区编码', 
  `performance_province_name` string COMMENT '省区', 
  `performance_city_code` string COMMENT '城市组编码', 
  `performance_city_name` string COMMENT '城市组', 
  `channel_code` string COMMENT '渠道编码', 
  `channel_name` string COMMENT '渠道编码', 
  `customer_code` string COMMENT '客户编号', 
  `customer_name` string COMMENT '客户名称', 
  `first_category_code` string COMMENT '一级客户分类编码',
  `first_category_name` string COMMENT '一级客户分类名称',
  `second_category_code` string COMMENT '二级客户分类编码',
  `second_category_name` string COMMENT '二级客户分类名称',
  `third_category_code` string COMMENT '三级客户分类编码',
  `third_category_name` string COMMENT '三级客户分类名称',
  `customer_large_level` string COMMENT '客户大分类',
  `customer_small_level` string COMMENT '客户小分类', 
  `sales_user_id` string COMMENT '销售员编码', 
  `sales_user_number` string COMMENT '销售员工号', 
  `sales_user_name` string COMMENT '销售员', 
  `sale_amt_no_tax` decimal(26,6) COMMENT '未税销售金额', 
  `sale_cost_no_tax` decimal(26,6) COMMENT '未税销售成本', 
  `profit_no_tax` decimal(26,6) COMMENT '未税定价毛利额', 
  `receivable_amount` decimal(26,6) COMMENT '应收金额', 
  `receivable_amount_last` decimal(26,6) COMMENT '前一日应收', 
  `capital_takes_up` decimal(26,6) COMMENT '资金占用费', 
  `transport_amount` decimal(26,6) COMMENT '未税运费',
  `performance_profit` decimal(26,6) COMMENT '履约利润',
  `net_profit` decimal(26,6) COMMENT '净利润', 
  `period_start_date` string COMMENT '周开始日期',
  `period_end_date` string COMMENT '周结束日期',
  `tb_week` string COMMENT '周',
  `update_time` timestamp COMMENT '更新时间'
) COMMENT '财务客户损益表_周'
PARTITIONED BY (`week` string COMMENT '日期分区')



---------------------------------------------------------------------------------------------------------
---------------------------------------------hive  report 建表语句-----------------------------------------------
CREATE TABLE `csx_report.csx_report_sss_customer_capital_takes_up_di`(
`biz_id` STRING  COMMENT '唯一值',
`performance_region_code` STRING  COMMENT '大区编码',
`performance_region_name` STRING  COMMENT '大区',
`performance_province_code` STRING  COMMENT '省区编码',
`performance_province_name` STRING  COMMENT '省区',
`performance_city_code` STRING  COMMENT '城市组编码',
`performance_city_name` STRING  COMMENT '城市组',
`channel_code` STRING  COMMENT '渠道编码',
`channel_name` STRING  COMMENT '渠道名称',
`customer_code` STRING  COMMENT '客户编号',
`customer_name` STRING  COMMENT '客户名称',
`first_category_code` STRING  COMMENT '一级客户分类编码',
`first_category_name` STRING  COMMENT '一级客户分类名称',
`second_category_code` STRING  COMMENT '二级客户分类编码',
`second_category_name` STRING  COMMENT '二级客户分类名称',
`third_category_code` STRING  COMMENT '三级客户分类编码',
`third_category_name` STRING  COMMENT '三级客户分类名称',
`customer_large_level` STRING  COMMENT '客户大分类',
`customer_small_level` STRING  COMMENT '客户小分类',
`sales_user_id` STRING  COMMENT '销售员编码',
`sales_user_number` STRING  COMMENT '销售员工号',
`sales_user_name` STRING  COMMENT '销售员',
`sale_amt_no_tax` DECIMAL (26,6) COMMENT '未税销售金额',
`sale_cost_no_tax` DECIMAL (26,6) COMMENT '未税销售成本',
`profit_no_tax` DECIMAL (26,6) COMMENT '未税定价毛利额',
`receivable_amount` DECIMAL (26,6) COMMENT '应收金额',
`receivable_amount_last` DECIMAL (26,6) COMMENT '前一日应收',
`capital_takes_up` DECIMAL (26,6) COMMENT '资金占用费',
`transport_amount` DECIMAL (26,6) COMMENT '未税运费',
`performance_profit` DECIMAL (26,6) COMMENT '履约利润',
`net_profit` DECIMAL (26,6) COMMENT '净利润',
`business_type_code` STRING  COMMENT '业务类型编码',
`business_type_name` STRING  COMMENT '业务类型名称',
`frmloss_amt` DECIMAL (20,6) COMMENT '报损金额',
`order_amount_no_tax` STRING  COMMENT '装车金额（未税）',
`exclude_order_amount_no_tax` STRING  COMMENT '未装车的金额（未税）',
`bbc_express_amount` DECIMAL (20,6) COMMENT 'bbc快递费',
`residue_amt_sss` DECIMAL (20,6) COMMENT '剩余金额', 
`smonth` STRING  COMMENT '月',
`week_of_year` STRING  COMMENT '周',
`update_time` timestamp COMMENT '更新时间',
`sdt` string COMMENT '日期' 
) COMMENT '财务客户损益表_日';



CREATE TABLE `csx_report.csx_report_sss_customer_capital_takes_up_mi`(
  `biz_id` string COMMENT '唯一值', 
  `performance_region_code` string COMMENT '大区编码', 
  `performance_region_name` string COMMENT '大区', 
  `performance_province_code` string COMMENT '省区编码', 
  `performance_province_name` string COMMENT '省区', 
  `performance_city_code` string COMMENT '城市组编码', 
  `performance_city_name` string COMMENT '城市组', 
  `channel_code` string COMMENT '渠道编码', 
  `channel_name` string COMMENT '渠道编码', 
  `customer_code` string COMMENT '客户编号', 
  `customer_name` string COMMENT '客户名称', 
  `first_category_code` string COMMENT '一级客户分类编码',
  `first_category_name` string COMMENT '一级客户分类名称',
  `second_category_code` string COMMENT '二级客户分类编码',
  `second_category_name` string COMMENT '二级客户分类名称',
  `third_category_code` string COMMENT '三级客户分类编码',
  `third_category_name` string COMMENT '三级客户分类名称',
  `customer_large_level` string COMMENT '客户大分类',
  `customer_small_level` string COMMENT '客户小分类', 
  `sales_user_id` string COMMENT '销售员编码', 
  `sales_user_number` string COMMENT '销售员工号', 
  `sales_user_name` string COMMENT '销售员', 
  `sale_amt_no_tax` decimal(26,6) COMMENT '未税销售金额', 
  `sale_cost_no_tax` decimal(26,6) COMMENT '未税销售成本', 
  `profit_no_tax` decimal(26,6) COMMENT '未税定价毛利额', 
  `receivable_amount` decimal(26,6) COMMENT '应收金额', 
  `receivable_amount_last` decimal(26,6) COMMENT '前一日应收', 
  `capital_takes_up` decimal(26,6) COMMENT '资金占用费', 
  `transport_amount` decimal(26,6) COMMENT '未税运费',
  `performance_profit` decimal(26,6) COMMENT '履约利润',
  `net_profit` decimal(26,6) COMMENT '净利润', 
  `period_start_date` string COMMENT '月开始日期',
  `period_end_date` string COMMENT '月结束日期',
  `update_time` timestamp COMMENT '更新时间',
  `smonth` string COMMENT '年月'
) COMMENT '财务客户损益表_月';


CREATE TABLE `csx_report.csx_report_sss_customer_capital_takes_up_wi`(
  `biz_id` string COMMENT '唯一值', 
  `performance_region_code` string COMMENT '大区编码', 
  `performance_region_name` string COMMENT '大区', 
  `performance_province_code` string COMMENT '省区编码', 
  `performance_province_name` string COMMENT '省区', 
  `performance_city_code` string COMMENT '城市组编码', 
  `performance_city_name` string COMMENT '城市组', 
  `channel_code` string COMMENT '渠道编码', 
  `channel_name` string COMMENT '渠道编码', 
  `customer_code` string COMMENT '客户编号', 
  `customer_name` string COMMENT '客户名称', 
  `first_category_code` string COMMENT '一级客户分类编码',
  `first_category_name` string COMMENT '一级客户分类名称',
  `second_category_code` string COMMENT '二级客户分类编码',
  `second_category_name` string COMMENT '二级客户分类名称',
  `third_category_code` string COMMENT '三级客户分类编码',
  `third_category_name` string COMMENT '三级客户分类名称',
  `customer_large_level` string COMMENT '客户大分类',
  `customer_small_level` string COMMENT '客户小分类', 
  `sales_user_id` string COMMENT '销售员编码', 
  `sales_user_number` string COMMENT '销售员工号', 
  `sales_user_name` string COMMENT '销售员', 
  `sale_amt_no_tax` decimal(26,6) COMMENT '未税销售金额', 
  `sale_cost_no_tax` decimal(26,6) COMMENT '未税销售成本', 
  `profit_no_tax` decimal(26,6) COMMENT '未税定价毛利额', 
  `receivable_amount` decimal(26,6) COMMENT '应收金额', 
  `receivable_amount_last` decimal(26,6) COMMENT '前一日应收', 
  `capital_takes_up` decimal(26,6) COMMENT '资金占用费', 
  `transport_amount` decimal(26,6) COMMENT '未税运费',
  `performance_profit` decimal(26,6) COMMENT '履约利润',
  `net_profit` decimal(26,6) COMMENT '净利润', 
  `period_start_date` string COMMENT '周开始日期',
  `period_end_date` string COMMENT '周结束日期',
  `tb_week` string COMMENT '周',
  `update_time` timestamp COMMENT '更新时间'
) COMMENT '财务客户损益表_周';

---------------------------------------------------------------------------------------------------------
---------------------------------------------mysql 建表语句-----------------------------------------------
CREATE TABLE `csx_report_sss_customer_capital_takes_up_di`(
  `biz_id` varchar(128) NOT NULL COMMENT '唯一值', 
  `performance_region_code` varchar(32) DEFAULT NULL COMMENT '大区编码', 
  `performance_region_name` varchar(32) DEFAULT NULL COMMENT '大区', 
  `performance_province_code` varchar(32) DEFAULT NULL COMMENT '省区编码', 
  `performance_province_name` varchar(32) DEFAULT NULL COMMENT '省区', 
  `performance_city_code` varchar(32) DEFAULT NULL COMMENT '城市组编码', 
  `performance_city_name` varchar(32) DEFAULT NULL COMMENT '城市组', 
  `channel_code` varchar(32) DEFAULT NULL COMMENT '渠道编码', 
  `channel_name` varchar(32) DEFAULT NULL COMMENT '渠道名称', 
  `customer_code` varchar(32) DEFAULT NULL COMMENT '客户编号', 
  `customer_name` varchar(128) DEFAULT NULL COMMENT '客户名称', 
  `first_category_code` varchar(32) DEFAULT NULL COMMENT '一级客户分类编码',
  `first_category_name` varchar(32) DEFAULT NULL COMMENT '一级客户分类名称',
  `second_category_code` varchar(32) DEFAULT NULL COMMENT '二级客户分类编码',
  `second_category_name` varchar(32) DEFAULT NULL COMMENT '二级客户分类名称',
  `third_category_code` varchar(32) DEFAULT NULL COMMENT '三级客户分类编码',
  `third_category_name` varchar(32) DEFAULT NULL COMMENT '三级客户分类名称',
  `customer_large_level` varchar(32) DEFAULT NULL COMMENT '客户大分类',
  `customer_small_level` varchar(32) DEFAULT NULL COMMENT '客户小分类', 
  `sales_user_id` varchar(32) DEFAULT NULL COMMENT '销售员编码', 
  `sales_user_number` varchar(32) DEFAULT NULL COMMENT '销售员工号', 
  `sales_user_name` varchar(32) DEFAULT NULL COMMENT '销售员', 
  `sale_amt_no_tax` decimal(26,6) COMMENT '未税销售金额', 
  `sale_cost_no_tax` decimal(26,6) COMMENT '未税销售成本', 
  `profit_no_tax` decimal(26,6) COMMENT '未税定价毛利额', 
  `receivable_amount` decimal(26,6) COMMENT '应收金额', 
  `receivable_amount_last` decimal(26,6) COMMENT '前一日应收', 
  `capital_takes_up` decimal(26,6) COMMENT '资金占用费', 
  `transport_amount` decimal(26,6) COMMENT '未税运费',
  `performance_profit` decimal(26,6) COMMENT '履约利润',
  `net_profit` decimal(26,6) COMMENT '净利润', 
  `business_type_code` varchar(32) DEFAULT NULL COMMENT '业务类型编码',
  `business_type_name` varchar(32) DEFAULT NULL COMMENT '业务类型名称',
  `frmloss_amt` decimal(20,6) DEFAULT NULL COMMENT '报损金额',
  `order_amount_no_tax` varchar(32) DEFAULT NULL COMMENT '装车金额（未税）',
  `exclude_order_amount_no_tax` varchar(32) DEFAULT NULL COMMENT '未装车的金额（未税） ',
  `bbc_express_amount` decimal(20,6) DEFAULT NULL COMMENT 'bbc快递费',
  `residue_amt_sss` decimal(20,6) DEFAULT NULL COMMENT '剩余金额',
  `smonth` varchar(32) DEFAULT NULL COMMENT '月', 
  `week_of_year` varchar(32) DEFAULT NULL COMMENT '周', 
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `sdt` varchar(32) DEFAULT NULL COMMENT '日期',
  PRIMARY KEY (`biz_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT= '财务客户损益表_日';


CREATE TABLE `csx_report_sss_customer_capital_takes_up_mi`(
  `biz_id` varchar(128) NOT NULL COMMENT '唯一值', 
  `performance_region_code` varchar(32) DEFAULT NULL COMMENT '大区编码', 
  `performance_region_name` varchar(32) DEFAULT NULL COMMENT '大区', 
  `performance_province_code` varchar(32) DEFAULT NULL COMMENT '省区编码', 
  `performance_province_name` varchar(32) DEFAULT NULL COMMENT '省区', 
  `performance_city_code` varchar(32) DEFAULT NULL COMMENT '城市组编码', 
  `performance_city_name` varchar(32) DEFAULT NULL COMMENT '城市组', 
  `channel_code` varchar(32) DEFAULT NULL COMMENT '渠道编码', 
  `channel_name` varchar(32) DEFAULT NULL COMMENT '渠道编码', 
  `customer_code` varchar(32) DEFAULT NULL COMMENT '客户编号', 
  `customer_name` varchar(128) DEFAULT NULL COMMENT '客户名称', 
  `first_category_code` varchar(32) DEFAULT NULL COMMENT '一级客户分类编码',
  `first_category_name` varchar(32) DEFAULT NULL COMMENT '一级客户分类名称',
  `second_category_code` varchar(32) DEFAULT NULL COMMENT '二级客户分类编码',
  `second_category_name` varchar(32) DEFAULT NULL COMMENT '二级客户分类名称',
  `third_category_code` varchar(32) DEFAULT NULL COMMENT '三级客户分类编码',
  `third_category_name` varchar(32) DEFAULT NULL COMMENT '三级客户分类名称',
  `customer_large_level` varchar(32) DEFAULT NULL COMMENT '客户大分类',
  `customer_small_level` varchar(32) DEFAULT NULL COMMENT '客户小分类', 
  `sales_user_id` varchar(32) DEFAULT NULL COMMENT '销售员编码', 
  `sales_user_number` varchar(32) DEFAULT NULL COMMENT '销售员工号', 
  `sales_user_name` varchar(32) DEFAULT NULL COMMENT '销售员', 
  `sale_amt_no_tax` decimal(26,6) COMMENT '未税销售金额', 
  `sale_cost_no_tax` decimal(26,6) COMMENT '未税销售成本', 
  `profit_no_tax` decimal(26,6) COMMENT '未税定价毛利额', 
  `receivable_amount` decimal(26,6) COMMENT '应收金额', 
  `receivable_amount_last` decimal(26,6) COMMENT '前一日应收', 
  `capital_takes_up` decimal(26,6) COMMENT '资金占用费', 
  `transport_amount` decimal(26,6) COMMENT '未税运费',
  `performance_profit` decimal(26,6) COMMENT '履约利润',
  `net_profit` decimal(26,6) COMMENT '净利润', 
  `period_start_date` varchar(32) DEFAULT NULL COMMENT '月开始日期',
  `period_end_date` varchar(32) DEFAULT NULL COMMENT '月结束日期',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `smonth` varchar(32) DEFAULT NULL COMMENT '年月',
  PRIMARY KEY (`biz_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT= '财务客户损益表_月';



CREATE TABLE `csx_report_sss_customer_capital_takes_up_wi`(
  `biz_id` varchar(128) NOT NULL COMMENT '唯一值', 
  `performance_region_code` varchar(32) DEFAULT NULL COMMENT '大区编码', 
  `performance_region_name` varchar(32) DEFAULT NULL COMMENT '大区', 
  `performance_province_code` varchar(32) DEFAULT NULL COMMENT '省区编码', 
  `performance_province_name` varchar(32) DEFAULT NULL COMMENT '省区', 
  `performance_city_code` varchar(32) DEFAULT NULL COMMENT '城市组编码', 
  `performance_city_name` varchar(32) DEFAULT NULL COMMENT '城市组', 
  `channel_code` varchar(32) DEFAULT NULL COMMENT '渠道编码', 
  `channel_name` varchar(32) DEFAULT NULL COMMENT '渠道编码', 
  `customer_code` varchar(32) DEFAULT NULL COMMENT '客户编号', 
  `customer_name` varchar(128) DEFAULT NULL COMMENT '客户名称', 
  `first_category_code` varchar(32) DEFAULT NULL COMMENT '一级客户分类编码',
  `first_category_name` varchar(32) DEFAULT NULL COMMENT '一级客户分类名称',
  `second_category_code` varchar(32) DEFAULT NULL COMMENT '二级客户分类编码',
  `second_category_name` varchar(32) DEFAULT NULL COMMENT '二级客户分类名称',
  `third_category_code` varchar(32) DEFAULT NULL COMMENT '三级客户分类编码',
  `third_category_name` varchar(32) DEFAULT NULL COMMENT '三级客户分类名称',
  `customer_large_level` varchar(32) DEFAULT NULL COMMENT '客户大分类',
  `customer_small_level` varchar(32) DEFAULT NULL COMMENT '客户小分类', 
  `sales_user_id` varchar(32) DEFAULT NULL COMMENT '销售员编码', 
  `sales_user_number` varchar(32) DEFAULT NULL COMMENT '销售员工号', 
  `sales_user_name` varchar(32) DEFAULT NULL COMMENT '销售员', 
  `sale_amt_no_tax` decimal(26,6) COMMENT '未税销售金额', 
  `sale_cost_no_tax` decimal(26,6) COMMENT '未税销售成本', 
  `profit_no_tax` decimal(26,6) COMMENT '未税定价毛利额', 
  `receivable_amount` decimal(26,6) COMMENT '应收金额', 
  `receivable_amount_last` decimal(26,6) COMMENT '前一日应收', 
  `capital_takes_up` decimal(26,6) COMMENT '资金占用费', 
  `transport_amount` decimal(26,6) COMMENT '未税运费',
  `performance_profit` decimal(26,6) COMMENT '履约利润',
  `net_profit` decimal(26,6) COMMENT '净利润', 
  `period_start_date` varchar(32) DEFAULT NULL COMMENT '周开始日期',
  `period_end_date` varchar(32) DEFAULT NULL COMMENT '周结束日期',
  `week` varchar(32) DEFAULT NULL COMMENT '周',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`biz_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT= '财务客户损益表_周';  



delete from csx_report_sss_customer_capital_takes_up_mi 
where smonth>='${smt_1bf}';

delete from csx_report_sss_customer_capital_takes_up_wi 
where period_start_date>='${sdt_30bf}';








