-- BBC看板
SET hive.execution.engine=tez;
SET tez.queue.name=caishixian;
-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;
SET hive.optimize.sort.dynamic.partition=true;
-- 作业负责人
SET author = '王威';

-- BBC省区月维度销售统计
insert overwrite table csx_dw.ads_bbc_s_m_province_summary partition(month)
select
  concat_ws('&', COALESCE(a.month, c.month,e.month), COALESCE(a.province_code, c.province_code,e.province_code)) id,
  COALESCE(a.province_code, c.province_code,e.province_code) as province_code,
  COALESCE(a.province_name, c.province_name,e.province_name) as province_name,
  COALESCE(a.trans_cust_cnt, 0) as trans_cust_cnt,
  COALESCE(a.new_trans_cust_cnt, 0) as new_trans_cust_cnt,
  COALESCE(a.trans_order_cnt, 0) as trans_order_cnt,
  COALESCE(a.sales_qty, 0) as sales_qty,
  COALESCE(a.sales_value, 0) as sales_value,
  COALESCE(a.sales_cost, 0) as sales_cost,
  COALESCE(a.profit, 0) as profit,
  COALESCE(a.profit_rate, 0) as profit_rate,
  COALESCE(a.avg_cust_sales_value, 0) as avg_cust_sales_value,
  COALESCE(a.excluding_tax_sales, 0) as excluding_tax_sales,
  COALESCE(a.excluding_tax_cost, 0) as excluding_tax_cost,
  COALESCE(a.excluding_tax_profit, 0) as excluding_tax_profit,
  COALESCE(c.order_cust_cnt, 0)+COALESCE(e.order_cust_cnt_yhsh, 0) order_cust_cnt,
  COALESCE(c.order_cnt, 0) +COALESCE(e.order_cnt_yhsh, 0)order_cnt,
  COALESCE(c.order_value, 0)+COALESCE(e.order_value_yhsh, 0) order_value,
  COALESCE(b.sales_qty, 0) mth_on_mth_sales_qty,
  COALESCE(b.sales_value, 0) mth_on_mth_sales_value,
  COALESCE(b.sales_cost, 0) mth_on_mth_sales_cost,
  COALESCE(b.profit, 0) mth_on_mth_profit,
  COALESCE(b.profit_rate, 0) mth_on_mth_profit_rate,
  COALESCE(b.avg_cust_sales_value, 0) mth_on_mth_avg_cust_sales_value,
  COALESCE(b.trans_cust_cnt, 0) mth_on_mth_trans_cust_cnt,
  COALESCE(b.new_trans_cust_cnt, 0) mth_on_mth_new_trans_cust_cnt,
  COALESCE(b.trans_order_cnt, 0) mth_on_mth_trans_order_cnt,
  COALESCE(d.mth_on_mth_order_cust_cnt, 0)+COALESCE(f.mth_on_mth_order_cust_cnt_yhsh, 0) mth_on_mth_order_cust_cnt,
  COALESCE(d.mth_on_mth_order_cnt, 0)+COALESCE(f.mth_on_mth_order_cnt_yhsh, 0 )mth_on_mth_order_cnt,
  COALESCE(d.mth_on_mth_order_value, 0)+COALESCE(f.mth_on_mth_order_value_yhsh, 0) mth_on_mth_order_value,
  from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
  ${hiveconf:author} as create_by,
  from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
  COALESCE(trans_cust_cnt_jd,0)trans_cust_cnt_jd,
  COALESCE(trans_cust_cnt_yhsh,0)trans_cust_cnt_yhsh,
  COALESCE(trans_order_cnt_jd,0)trans_order_cnt_jd,
  COALESCE(trans_order_cnt_yhsh,0)trans_order_cnt_yhsh,
  COALESCE(order_cnt_jd,0)order_cnt_jd,
  COALESCE(order_cnt_yhsh,0)order_cnt_yhsh,
  COALESCE(c.order_cnt_zy, 0)order_cnt_zy,
  COALESCE(a.trans_order_cnt_zy,0)trans_order_cnt_zy,
  COALESCE(apply_voucher_cnt,0)apply_voucher_cnt,
  COALESCE(active_voucher_cnt,0)active_voucher_cnt,
  COALESCE(use_voucher_cnt,0)use_voucher_cnt,
  COALESCE(overdue_voucher_cnt,0)overdue_voucher_cnt,
  COALESCE(apply_voucher_value,0)apply_voucher_value,
  COALESCE(active_voucher_value,0)active_voucher_value,
  COALESCE(use_voucher_value,0)use_voucher_value,
  COALESCE(overdue_voucher_value,0)overdue_voucher_value,
  COALESCE(active_user_cnt,0)active_user_cnt,
  COALESCE(use_voucher_user_cnt,0)use_voucher_user_cnt,
  COALESCE(voucher_order_cnt,0)voucher_order_cnt,
  COALESCE(voucher_sales_value,0)voucher_sales_value,
  COALESCE(trans_cust_cnt_zy,0)trans_cust_cnt_zy,
  COALESCE(a.month, c.month,e.month) as month
from
(
  select
    province_code,
    province_name,
    count(distinct customer_no) trans_cust_cnt,
    count(distinct if(is_new_bbc_cust = 1, customer_no, null)) new_trans_cust_cnt,
    count(distinct if(return_flag = '', origin_order_no, null))  as trans_order_cnt, -- 成交订单数
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(sales_cost) sales_cost,
    sum(profit) profit,
    sum(profit)/sum(sales_value) profit_rate,
    sum(sales_value)/count(distinct customer_no) avg_cust_sales_value,
    sum(excluding_tax_sales) excluding_tax_sales,
    sum(excluding_tax_cost) excluding_tax_cost,
    sum(excluding_tax_profit) excluding_tax_profit,
	count(distinct if(operation_mode=1 and supplier_code in('20041445','20042791','20014942','20014947','20029170','20038271','20045100','20045763','20045764','20048713'),customer_no,null))trans_cust_cnt_jd,
	count(distinct if(b.order_n is not null,coalesce(customer_no,'102587'),null))trans_cust_cnt_yhsh,
	count(distinct if(operation_mode=1 and supplier_code in('20041445','20042791','20014942','20014947','20029170','20038271','20045100','20045763','20045764','20048713') and return_flag = '',order_no,null))trans_order_cnt_jd,
	count(distinct if(operation_mode=0,order_no,null))trans_order_cnt_zy,
	count(distinct if(b.order_n is not null and return_flag = '',order_no,null))trans_order_cnt_yhsh,
	count(distinct if(operation_mode=0,customer_no,null))trans_cust_cnt_zy,
    substr(sdt,1,6) month
  from
	(
	  select
	    province_code,
        province_name,
		customer_no,
		is_new_bbc_cust,
		origin_order_no,
		return_flag,
		sales_qty,
		sales_value,
		sales_cost,
		profit,
		excluding_tax_sales,
		excluding_tax_cost,
		excluding_tax_profit,
		operation_mode,
		supplier_code,
		order_no,
		sdt 
      from csx_dw.dws_bbc_r_d_sale_detail
      where sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -1), '-', '')  
        and sdt < regexp_replace(current_date, '-', '')
    )a left join
	(
	  select
	    order_no order_n
	  from csx_dw.dwd_bbc_r_d_yhsh_order_d
	  where sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -2), '-', '') 
        and sdt < regexp_replace(current_date, '-', '')
	  group by order_no
	)b on a.order_no=b.order_n
  group by province_code, province_name, substr(sdt,1,6)
) a left join
(
  select
    province_code,
    count(distinct customer_no) trans_cust_cnt,
    count(distinct if(is_new_bbc_cust = 1, customer_no, null)) new_trans_cust_cnt,
    count(distinct if(return_flag = '', origin_order_no, null))  as trans_order_cnt, -- 成交订单数
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(sales_cost) sales_cost,
    sum(profit) profit,
    sum(profit)/sum(sales_value) profit_rate,
    sum(sales_value)/count(distinct customer_no) avg_cust_sales_value,
    from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(concat(substr(sdt,1,6),'01'),
      'yyyyMMdd'), 'yyyy-MM-dd'), 1), 'yyyy-MM-dd'), 'yyyyMM') mth_on_mth
  from csx_dw.dws_bbc_r_d_sale_detail
  where sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -2), '-', '')-- 上上个月  and sdt < regexp_replace(current_date, '-', '')
  group by province_code, substr(sdt,1,6)
) b on a.province_code = b.province_code and a.month = b.mth_on_mth
full join
( -- 获取下单维度数据
  SELECT
    b.province_code,
    b.province_name,
    count(DISTINCT a.customer_no) AS order_cust_cnt,
    count(DISTINCT a.order_no) AS order_cnt,
	count(distinct if(supplier_codes regexp('.*20041445|20042791|20014942|20029170|20038271|20045100|20045763|20045764|20048713.*'),a.order_no,null))order_cnt_jd,
    sum(DISTINCT a.goods_total_price) AS order_value,
	count(DISTINCT if(e.order_no is null,a.order_no,null))order_cnt_zy,
    a.month
  from
  (
    SELECT customer_no, order_no, goods_total_price, substr(sdt, 1, 6) as month,supplier_codes
    FROM csx_dw.dwd_bbc_r_d_wshop_order
    where sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -1), '-', '') -- 上个月1号
      and sdt < regexp_replace(current_date, '-', '') and order_status in (2, 3, 4, 5, 7, 8, 9, 10)
  ) a LEFT JOIN
  (
    SELECT
      customer_no, if(province_code = '16', '15', province_code) as province_code,
      if(province_code = '16', '福建省', province_name) as province_name
    FROM csx_dw.dws_crm_w_a_customer
    WHERE sdt = 'current'
  ) b ON a.customer_no = b.customer_no
  join
  (
    SELECT 
	  distinct
	  goods_code,
	  order_no
    FROM csx_dw.dwd_bbc_r_d_wshop_order_d
    where sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -1), '-', '') -- 上个月1号
      and sdt < regexp_replace(current_date, '-', '') and order_status in (2, 3, 4, 5, 7, 8, 9, 10)
  )c on a.order_no=c.order_no
  left join
  (
    select
	  distinct
	  order_no,
	  goods_code
    from  csx_dw.dwd_bbc_r_d_wshop_supplier_order_d 
	where sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -1), '-', '') -- 上个月1号
      and sdt < regexp_replace(current_date, '-', '')
  )e on c.order_no=e.order_no and c.goods_code=e.goods_code
  GROUP BY b.province_code, b.province_name, a.month
) c on a.province_code = c.province_code and a.month = c.month
left join
( -- 获取下单维度数据环比
  SELECT
    b.province_code,
    count(DISTINCT a.customer_no) AS mth_on_mth_order_cust_cnt,
    count(DISTINCT a.order_no) AS mth_on_mth_order_cnt,
    sum(a.goods_total_price) AS mth_on_mth_order_value,
    a.mth_on_mth
  from
  (
    SELECT
      customer_no, order_no, goods_total_price,
      from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt,
        'yyyyMMdd'), 'yyyy-MM-dd'), 1), 'yyyy-MM-dd'), 'yyyyMM') mth_on_mth
	FROM csx_dw.dwd_bbc_r_d_wshop_order
    where sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -2), '-', '') -- 上上个月1号
    and sdt < regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '') and order_status in (2, 3, 4, 5, 7, 8, 9, 10)
  ) a LEFT JOIN
  (
    SELECT customer_no, if(sales_province_code = '16', '15', sales_province_code) as province_code
    FROM csx_dw.dws_crm_w_a_customer
    WHERE sdt = 'current'
  ) b ON a.customer_no = b.customer_no
  GROUP BY b.province_code, a.mth_on_mth
) d  on coalesce(a.province_code,c.province_code) = d.province_code and coalesce(a.month,c.month) = d.mth_on_mth  
full join
( -- 获取下单维度数据
  SELECT
    b.province_code,
    b.province_name,
    count(DISTINCT a.customer_no) AS order_cust_cnt_yhsh,
    count(DISTINCT a.order_no) AS order_cnt_yhsh,
    sum(a.goods_total_price) AS order_value_yhsh,
    a.month
  from
  (
    SELECT coalesce(customer_no,'102587')customer_no, order_no, sum(price*order_count) goods_total_price, substr(sdt, 1, 6) as month
    FROM csx_dw.dwd_bbc_r_d_yhsh_order_d
    where sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -1), '-', '') -- 上个月1号
      and sdt < regexp_replace(current_date, '-', '') 
	group by coalesce(customer_no,'102587'),order_no, substr(sdt, 1, 6)
  ) a LEFT JOIN
  (
    SELECT
      customer_no, if(province_code = '16', '15', province_code) as province_code,
      if(province_code = '16', '福建省', province_name) as province_name
    FROM csx_dw.dws_crm_w_a_customer
    WHERE sdt = 'current'
  ) b ON a.customer_no = b.customer_no
  GROUP BY b.province_code, b.province_name, a.month
)e  on coalesce(a.province_code,c.province_code) = e.province_code and coalesce(a.month,c.month) =e.month 
left join
( -- 获取下单维度数据环比
  SELECT
    b.province_code,
    count(DISTINCT a.customer_no) AS mth_on_mth_order_cust_cnt_yhsh,
    count(DISTINCT a.order_no) AS mth_on_mth_order_cnt_yhsh,
    sum(a.goods_total_price) AS mth_on_mth_order_value_yhsh,
    a.mth_on_mth
  from
  (
     SELECT coalesce(customer_no,'102587')customer_no, order_no, sum(price*order_count) goods_total_price, from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt,
        'yyyyMMdd'), 'yyyy-MM-dd'), 1), 'yyyy-MM-dd'), 'yyyyMM') mth_on_mth
    FROM csx_dw.dwd_bbc_r_d_yhsh_order_d
    where sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -2), '-', '') -- 上上个月1号
    and sdt < regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '') 
	group by customer_no,order_no, from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt,
        'yyyyMMdd'), 'yyyy-MM-dd'), 1), 'yyyy-MM-dd'), 'yyyyMM')
  ) a LEFT JOIN
  (
    SELECT customer_no, if(sales_province_code = '16', '15', sales_province_code) as province_code
    FROM csx_dw.dws_crm_w_a_customer
    WHERE sdt = 'current'
  ) b ON a.customer_no = b.customer_no
  GROUP BY b.province_code, a.mth_on_mth
) f on coalesce(a.province_code,c.province_code,e.province_code) = f.province_code and coalesce(a.month,c.month,e.month)= f.mth_on_mth
left join
(
      select
    province_code,
    count(distinct voucher_code) apply_voucher_cnt,
    count(distinct if(exchange_time is not null, voucher_code,null)) active_voucher_cnt,
  	count(distinct if(exchange_time is not null and voucher_value<>voucher_balance, voucher_code,null))use_voucher_cnt,
  	count(distinct if(voucher_status=2 and voucher_value=voucher_balance,voucher_code,null))overdue_voucher_cnt,
	sum(voucher_value)apply_voucher_value,
    sum(if(exchange_time is not null, voucher_value,0))active_voucher_value,
  	sum(if(exchange_time is not null,voucher_value-voucher_balance, 0))use_voucher_value,
  	sum(if(voucher_status=2 and voucher_value=voucher_balance, voucher_value,0))overdue_voucher_value,
	count(distinct if(exchange_time is not null, exchange_user_id,null))active_user_cnt,
	count(distinct if(exchange_time is not null and voucher_value<>voucher_balance, exchange_user_id,null))use_voucher_user_cnt,
	count(distinct coalesce(order_n,order_code))voucher_order_cnt,
    coalesce(sum(coalesce(sales_value,0)),0)voucher_sales_value,
	month
  from
  (
  select
    coalesce(z.province_code,f.province_code,15) province_code,
    voucher_code,
    exchange_user_id,
    exchange_time,
    voucher_value,
    voucher_balance,
    voucher_status,
    f.order_no,
    f.order_code,
    z.order_no order_n,
    row_number()over(partition by voucher_code order by  coalesce(sales_value,0) desc,coalesce(z.province_code,f.province_code,15) desc) rk,
	sales_value,
    month
  from 
  (
    select
      coalesce(t.province_code,15) province_code,
      voucher_code,
      exchange_user_id,
      exchange_time,
      voucher_value,
      voucher_balance,
      voucher_status,
      e.order_no,
      order_code,
      month
    from
      (
      select
        id,
        active_status,
        substr(regexp_replace(create_time,'-',''),1,6) month
      from csx_ods.source_bbc_r_a_wshop_exchange_activity
      where cost_center_code<>'' and create_time>= add_months(trunc(date_sub(current_date, 1), 'MM'), -1)
    )a join
    (
      select  
        exchange_id,
        voucher_code,
        exchange_user_id,
        voucher_value,
        voucher_balance,
        voucher_status,
        exchange_time
      from csx_ods.source_bbc_r_d_wshop_exchange_voucher 
      where sdt=regexp_replace(date_sub(current_date, 1), '-', '')
    )b on cast(a.id as string)=cast(b.exchange_id as string)
    join
    (
      select
        id,
        telephone
      from csx_ods.source_bbc_w_a_wshop_user
      where sdt=regexp_replace(date_sub(current_date, 1), '-', '')
     )c on cast(b.exchange_user_id as string)=cast(c.id as string)
    left join
    (
      select
        id,
        user_name,
        telephone,
        business_number,
        cust_name
      from csx_ods.source_bbc_r_d_wshop_user_credit
      where sdt=regexp_replace(date_sub(current_date, 1), '-', '')
    )d on c.telephone=d.telephone
    left join
    (
      select
        substr(order_code,1,16)order_code,
    	exchange_id,
    	user_id
      from csx_ods.source_bbc_r_d_wshop_exchange_log
      where sdt>=regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -1),'-','') and type=0
    )t1 on cast(a.id as string)=cast(t1.exchange_id as string) and cast(b.exchange_user_id as string)= cast(t1.user_id as string)
    left join
    (
      SELECT
        customer_no,
        province_code
      from csx_dw.dws_crm_w_a_customer
      where sdt='current' 
    )t on coalesce(d.business_number,'999999')=t.customer_no
    left join
    (
      select
        order_id,
        relate_code
      from csx_ods.source_bbc_w_a_wshop_voucher
    )g on b.voucher_code=g.relate_code
    left join
    (
      select
        distinct
        id,
        order_no
      from csx_dw.dwd_bbc_r_d_wshop_order
      where sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -6), '-', '')
    )e on g.order_id=e.id
      group by coalesce(t.province_code,15),voucher_code,exchange_time,voucher_value,voucher_balance,voucher_status,e.order_no,month,exchange_user_id,order_code
  )f left join
  (
   select
     province_code,
     order_no,
     sales_value,
     order_code
   from
    (
      select
      province_code,
      order_no,
      sum(sales_value)sales_value
      from csx_dw.dws_bbc_r_d_sale_detail
      where sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -6), '-', '')
      group by province_code,order_no
    )z left join     
    (
      select
      distinct 
      order_code,
      csx_order_code
     from csx_ods.source_bbc_r_d_wshop_yhsh_order_info_v1
     where sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -6), '-', '')
    )y  on z.order_no=y.csx_order_code 
   )z on coalesce(f.order_code,f.order_no)=coalesce(z.order_code,z.order_no)
  )z where rk=1
  group by province_code,month
)g on coalesce(a.province_code,c.province_code,e.province_code) = g.province_code and coalesce(a.month,c.month,e.month)= g.month;


