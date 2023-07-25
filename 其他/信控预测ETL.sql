-- 逻辑规则于20191112 更改
-- 增加大宗、供应链 万四舍五入 round(new_cr,-4), 大、商超 千四舍五入round(a,-3)
SET mapreduce.job.queuename =caishixian;
set hive.groupby.skewindata = true;
SET hive.exec.parallel      =TRUE;
drop table if exists temp.p_sale_01
;

create temporary table if not exists temp.p_sale_01 as
select
	'票到0' diff ,
	0     index_data
union all
select
	'票到15' diff ,
	15     index_data
union all
select
	'票到30' diff ,
	30     index_data
union all
select
	'票到45' diff ,
	45     index_data
union all
select
	'票到60' diff ,
	60     index_data
union all
select
	'票到7' diff ,
	7     index_data
union all
select
	'月结10' diff ,
	25     index_data
union all
select
	'月结15' diff ,
	30     index_data
union all
select
	'月结30' diff ,
	45     index_data
union all
select
	'月结45' diff ,
	60     index_data
union all
select
	'月结60' diff ,
	75     index_data
union all
select
	'月结90' diff ,
	105 index_data
;

--SET sdate=to_date(date_sub(current_timestamp(),1));
-- 资料
drop table if exists temp.p_sale_02
;

CREATE temporary table if NOT EXISTS temp.p_sale_02 as
SELECT
	channel                     ,
	sales_province_code         ,
	sales_province              ,
	sales_city_code             ,
	sales_city                  ,
	province_code               ,
	province_name               ,
	attribute                   ,
	city_code                   ,
	city_name                   ,
	sales_name                  ,
	work_no                     ,
	customer_no                 ,
	customer_name               ,
	credit_limit                ,
	temp_credit_limit           ,
	first_category              ,
	second_category             ,
	third_category              ,
	to_date(sign_time)sign_date ,
	company_code
FROM
	csx_dw.customer_m
WHERE
	sdt             =regexp_replace('${sdate}','-','')
	and customer_no<>''
;

drop table if exists temp.p_sale_03
;

CREATE temporary table if not EXISTS temp.p_sale_03 as
-- 帐龄数据
SELECT
	sflag                                 ,
	hkont                                 ,
	account_name                          ,
	comp_code                             ,
	comp_name                             ,
	regexp_replace(kunnr,'(^0*)','')kunnr ,
	name                                  ,
	zterm                                 ,
	diff                                  ,
	ac_all                                ,
	ac_wdq                                ,
	ac_15d                                ,
	ac_30d                                ,
	ac_60d                                ,
	ac_90d                                ,
	ac_120d                               ,
	ac_180d                               ,
	ac_365d                               ,
	ac_2y                                 ,
	ac_3y                                 ,
	ac_over3y
FROM
	csx_dw.account_age_dtl_fct_new
WHERE
	sdt=regexp_replace('${sdate}','-','')
;

-- regexp_replace(${hiveconf:sdate},'-','')
DROP table if exists temp.p_sale_04
;

CREATE temporary table if NOT EXISTS temp.p_sale_04 as
-- 销售数据
select
	channel        ,
	channel_name   ,
	customer_no    ,
	a.company_name ,
	a.company_code ,
	sale           ,
	case
		when channel in ('4' ,
						 '5' ,
						 '6')
			then coalesce(sale/60,0)
		when channel in ('1' ,
						 '2' ,
						 '3' ,
						 '7')
			then coalesce(sale/30,0)
	end as avg_sale ,
	min_sdt
from
	(
		SELECT
			channel                        ,
			a.channel_name                 ,
			customer_no                    ,
			a.dc_company_code company_code ,
			a.dc_company_name company_name ,
			sum
				(
					case
						when(
								channel in ('4' ,
											'5' ,
											'6')
								and sdt>=regexp_replace(date_sub('${sdate}',60),'-','')
							)
							then a.sales_value
						when (
								channel in ('1' ,
											'2' ,
											'3' ,
											'7')
								and sdt>=regexp_replace(date_sub('${sdate}',30),'-','')
							)
							then a.sales_value
					end
				)
			        sale ,
			min(sdt)min_sdt
		FROM
			csx_dw.customer_sales a
		where
			a.sdt>=regexp_replace(trunc(date_sub('${sdate}',30),'YY'),'-','')
		GROUP BY
			customer_no     ,
			dc_company_code ,
			dc_company_name ,
			channel         ,
			a.channel_name
	)
	a
;

-- 1、如果首单日期是空白的，且过去30个自然日日均销售也是空白的，那么信控降为0。
-- 2、如果首单日期不是空白的，但是过去30个自然日日均销售是空白的（代表销售未满30天），则信控和原固定额度保持一致。
-- 3、其他情况下都是对比（已使用信控额度+未来信控额度）和原固定额度，取较小值作为新的信控额度。如果对比下来较小值是个负数，则新信控额度为0。
-- 预测信控 20191022
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =1000;     --在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=1000;     --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错
INSERT overwrite table csx_dw.crm_credit_rating partition
	(sdt
	)
SELECT
	sflag                ,
	hkont                ,
	account_name         ,
	comp_code            ,
	comp_name            ,
	company_code         ,
	sale_company_code    ,
	sales_province_code  ,
	sales_province       ,
	sales_city_code      ,
	sales_city           ,
	province_code        ,
	province_name        ,
	city_code            ,
	city_name            ,
	sales_name           ,
	work_no              ,
	customer_no          ,
	customer_name        ,
	attribute            ,
	credit_limit         ,
	temp_credit_limit    ,
	channel              ,
	first_category       ,
	second_category      ,
	third_category       ,
	sign_date            ,
	zterm                ,
	diff                 , -- 帐期时长
	ac_wdq               ,
	ac_15d               ,
	ac_30d               ,
	ac_60d               ,
	ac_90d               ,
	ac_120d              ,
	ac_180d              ,
	ac_365d              ,
	ac_2y                ,
	ac_3y                ,
	ac_over3y            ,
	ac_all               , -- 应收金额=已使用额度
	overdue_account      , -- 逾期金额
	ac_all AS use_ac_all , -- 应收金额=已使用额度
	avg_sale             , -- 30日均销售额
	diff as diff_01      , --帐期
	index_data           , -- 转换
	unuse_ac             , -- 未使用额度
	new_credit_limit     , -- 调整后额度
	future_amount        , -- 未来额度,
	surplus_credit       , -- 剩余信控
	case
		when channel in ('供应链(食百)' ,
						 '供应链(生鲜)' ,
						 '大宗')
			and min_sdt>regexp_replace(date_sub('${sdate}',60),'-','')
			then round(credit_limit,-4)
		when channel in ('大'    ,
						 '企业购'    ,
						 '商超(对外)' ,
						 '商超(对内)')
			and min_sdt>regexp_replace(date_sub('${sdate}',30),'-','')
			then round(credit_limit,-3)
			-- when new_credit_limit=0 then 0
		when channel in ('大'    ,
						 '企业购'    ,
						 '商超(对外)' ,
						 '商超(对内)')
			and (
				min_sdt  <=regexp_replace(date_sub('${sdate}',30),'-','')
				or min_sdt=''
			)
			then
			--least(a.credit_limit,a.new_credit_limit) ELSE 0
			round(sort_array(array(credit_limit,new_credit_limit))[0],-3)
		when channel in ('供应链(食百)' ,
						 '供应链(生鲜)' ,
						 '大宗')
			and (
				min_sdt  <=regexp_replace(date_sub('${sdate}',60),'-','')
				or min_sdt=''
			)
			then round(sort_array(array(credit_limit,new_credit_limit))[0] ,-4)
	end credit_limit1 ,
	min_sdt           ,
	if
		(
			case
				when channel in ('供应链(食百)' ,
								 '供应链(生鲜)' ,
								 '大宗')
					and min_sdt>regexp_replace(date_sub('${sdate}',60),'-','')
					then round(credit_limit,-4)
				when channel in ('大'    ,
								 '企业购'    ,
								 '商超(对外)' ,
								 '商超(对内)')
					and min_sdt>regexp_replace(date_sub('${sdate}',30),'-','')
					then round(credit_limit,-3)
					-- when new_credit_limit=0 then 0
				when channel in ('大'    ,
								 '企业购'    ,
								 '商超(对外)' ,
								 '商超(对内)')
					and (
						min_sdt  <=regexp_replace(date_sub('${sdate}',30),'-','')
						or min_sdt=''
					)
					then round(sort_array(array(credit_limit,new_credit_limit))[0],-3)
				when channel in ('供应链(食百)' ,
								 '供应链(生鲜)' ,
								 '大宗')
					and (
						min_sdt  <=regexp_replace(date_sub('${sdate}',60),'-','')
						or min_sdt=''
					)
					then round(sort_array(array(credit_limit,new_credit_limit))[0] ,-4)
			end =credit_limit,'否','是'
		)
	                                  change_type ,
	regexp_replace('${sdate}','-','') sdt
FROM
	(
		SELECT
			sflag                                       ,
			hkont                                       ,
			account_name                                ,
			comp_code                                   ,
			comp_name                                   ,
			a.company_code                              ,
			c.company_code as sale_company_code         ,
			sales_province_code                         ,
			sales_province                              ,
			sales_city_code                             ,
			sales_city                                  ,
			province_code                               ,
			province_name                               ,
			city_code                                   ,
			city_name                                   ,
			sales_name                                  ,
			work_no                                     ,
			a.customer_no                               ,
			customer_name                               ,
			attribute                                   ,
			credit_limit                                ,
			temp_credit_limit                           ,
			a.channel                                   ,
			c.channel as channel_id                     ,
			c.channel_name                              ,
			first_category                              ,
			second_category                             ,
			third_category                              ,
			sign_date                                   ,
			zterm                                       ,
			ac_wdq                                      ,
			ac_15d                                      ,
			ac_30d                                      ,
			ac_60d                                      ,
			ac_90d                                      ,
			ac_120d                                     ,
			ac_180d                                     ,
			ac_365d                                     ,
			ac_2y                                       ,
			ac_3y                                       ,
			ac_over3y                                   ,
			ac_all                                      , -- 应收金额=已使用额度
			ac_all-ac_wdq        AS overdue_account     , -- 逾期金额
			ac_all               AS use_ac_all          , -- 应收金额=已使用额度
			coalesce(avg_sale,0)    avg_sale            , -- 大宗与供应链60日均销售额其他30日均
			d.diff                                      , --帐期
			d.index_data                                , -- 转换
			coalesce(b.ac_all*index_data,0) AS unuse_ac , -- 未使用额度
			case
				when (
						ac_all+coalesce(avg_sale*index_data,0)
					)
					<0
					then 0
					else (ac_all+coalesce(avg_sale*index_data,0))
			end                                  new_credit_limit , -- 新信控=ac_all+未来信控
			coalesce(avg_sale*d.index_data,0) AS future_amount    , -- 未来信控=sale-30*index_data
			case
				when coalesce(a.credit_limit-b.ac_all,0)<= 0
					then 0
					else coalesce(a.credit_limit-b.ac_all,0)
			end                    AS surplus_credit , -- 剩余信控
			coalesce(c.min_sdt,'')    min_sdt
		FROM
			temp.p_sale_02 AS a
			LEFT JOIN
				temp.p_sale_03 AS b
				ON
					a.customer_no=b.kunnr
			LEFT OUTER JOIN
				temp.p_sale_04 AS c
				ON
					b.kunnr        =c.customer_no
					AND b.comp_code=c.company_code
			LEFT OUTER JOIN
				temp.p_sale_01 AS d
				ON
					b.diff=d.diff
	)
	a
WHERE
	customer_no NOT LIKE 'S%'
;