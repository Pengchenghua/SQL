-- 福利BBC销售+费用
--   drop table csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail ;
   create table csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail as 
   select
        '福利' group_flag,
        '福利' type_flag,
        '福利' as operation_mode_name,
        concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1) as sale_quarter,
        substr(sdt, 1, 6) month,
        sdt,
        order_code,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        business_type_code,
        business_type_name,
        customer_code,
        -- customer_name,
        -- inventory_dc_code,
        -- -- inventory_dc_name,
        -- delivery_type_name,
        -- -- 配送类型名称
        -- classify_large_code,
        -- classify_middle_code,
        -- classify_small_code,
        -- goods_code,
        -- goods_name,
        -- count(distinct sdt) count_day,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(sale_amt_no_tax) sale_amt_no_tax,
        sum(profit_no_tax) profit_no_tax
      from
        csx_dws.csx_dws_sale_detail_di
      where
        sdt between '20240801' and '20241031'
        and channel_code in ('1', '7', '9')
        and business_type_code in ('2') --  and performance_region_name in ('华南大区', '华北大区', '华西大区', '华东大区', '华中大区')
      group by   concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1) ,
        substr(sdt, 1, 6) ,
        sdt,
        order_code,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        business_type_code,
        business_type_name,
        customer_code
      union all
      select
        'BBC' group_flag,
        if(credit_pay_type_name = '餐卡'  or credit_pay_type_code = 'F11', '餐卡',  '福利') type_flag,
        operation_mode_name,
        concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1) as sale_quarter,
        substr(sdt, 1, 6) month,
        sdt,
        order_code,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        6 as business_type_code,
        'BBC' as business_type_name,
        customer_code,
        -- customer_name,
        -- inventory_dc_code,
        -- inventory_dc_name,
        -- delivery_type_name,
        -- -- 配送类型名称
        -- classify_large_code,
        -- classify_middle_code,
        -- classify_small_code,
        -- goods_code,
        -- goods_name,
        -- count(distinct sdt) count_day,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(sale_amt_no_tax) sale_amt_no_tax,
        sum(profit_no_tax) profit_no_tax
      from
        csx_dws.csx_dws_bbc_sale_detail_di
      where
        sdt between '20240801' and '20241031'
        and channel_code in ('1', '7', '9') -- and business_type_code in ('2','6')
        --	and performance_region_name in ('华南大区','华北大区','华西大区','华东大区','华中大区')
        group by     
        if(
          credit_pay_type_name = '餐卡'
          or credit_pay_type_code = 'F11',
          '餐卡',
          '福利'
        )  ,
        operation_mode_name,
        concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1) ,
        substr(sdt, 1, 6) ,
        sdt,
        order_code,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        customer_code
        -- customer_name,
        -- inventory_dc_code
 
    ;


-- 计算客户上月1号至今每日的当日应收金额、昨日应收金额、资金占用费
-- drop table  csx_analyse_tmp.csx_tmp_sss_customer_receivable_amount;
create table csx_analyse_tmp.csx_tmp_sss_customer_receivable_amount as
with tmp_sss_customer_receivable_amount as (
  -- 计算应收金额和剩余金额
  select
    sdt,
    customer_code,
    business_type_code,
    type_flag,
    operation_mode_name,
    sum(coalesce(receivable_amount, 0)) as receivable_amount
    -- 应收金额
    -- sum(coalesce(residue_amt_sss, 0)) as residue_amt_sss -- 剩余金额 认领未核销金额
  from
    (
      select
        -- regexp_replace(to_date(a .happen_date),'-','') as sdt,
        sdt,
        a .customer_code,
        type_flag,
        operation_mode_name,
        coalesce(b.business_type_code, 1) as business_type_code,
        sum(coalesce(a .unpaid_amount, 0)) as receivable_amount,
        -- 应收金额
        0 as residue_amt_sss -- 剩余金额 认领未核销金额
      from
        (
          -- 20230214日客户扩展到信控切表 历史得用客户表 因此3月后用新表
          select
            source_bill_no,
            -- 来源单号
             case when substr(source_bill_no,1,1) ='B' then substr(source_bill_no, 2,length(source_bill_no)-2)  -- 涉及 BBC单号有字母开头的处理
              else source_bill_no end as new_source_bill_no,
            customer_code,
            -- 客户编码
            happen_date,
            -- 发生时间
            unpaid_amount,
            -- 未回款金额
            source_sys,
            -- 来源系统 MALL b端销售 BBC bbc端 BEGIN 期初
            sdt
          from
            csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di -- 销售结算对账开票结算详情表（新表）
          where
            sdt >= '20240801'
            and sdt<='20241111'
            and regexp_replace(date(happen_date), '-', '') <= sdt
           -- and source_bill_no='R2408010410590817A'
         
        ) a
        left join 
        (select order_code ,
            business_type_code,
            type_flag,
            operation_mode_name
        from csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail 
            group by order_code ,
            business_type_code,
            type_flag,
            operation_mode_name
        ) b on a .source_bill_no = b.order_code
        and if(a .source_sys <> 'BEGIN', true, false)
        and b.business_type_code is not null 
      group by
        --  regexp_replace(to_date(a .happen_date),'-',''),
        sdt,
        a .customer_code ,
        type_flag,
        operation_mode_name,
        coalesce(b.business_type_code, 1) 

    ) a
  group by
    sdt,
    customer_code,
    business_type_code,
    type_flag,
    operation_mode_name
)
select
  sdt,
  customer_code,
  business_type_code,
  type_flag,
  operation_mode_name,
--   sum(coalesce(residue_amt_sss, 0)) as residue_amt_sss,
  -- 剩余金额 认领未核销金额
  sum(coalesce(receivable_amount, 0)) as receivable_amount,
  -- 当日应收
  if(
      sum(receivable_amount) > 0,
      sum(receivable_amount),
      0
    )  * 0.06 / 365 as capital_takes_up -- 资金占用费
from
  (
    select
      sdt,
      customer_code,
      business_type_code,
      type_flag,
      operation_mode_name,
      receivable_amount 
    from
      tmp_sss_customer_receivable_amount 
    
  ) a
where
  business_type_code in ('2','6')
  and sdt >='20240801'
group by
  sdt,
  customer_code,
  business_type_code,
  type_flag,
  operation_mode_name;
  
with tmp_flbbc_sale as 
(select
        a.group_flag,
        a.type_flag,
        a.operation_mode_name,
        a.sale_quarter,
        a.month,
        a.sdt,
        a.order_code,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(sale_amt_no_tax) sale_amt_no_tax,
        sum(profit_no_tax) profit_no_tax
      from  csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail a 
      group by a.group_flag,
        a.type_flag,
        a.operation_mode_name,
        a.sale_quarter,
        a.month,
        a.sdt,
        a.order_code,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code
      ),
    tmp_sale_expense as (
      select a.group_flag,
        a.type_flag,
        a.operation_mode_name,
        a.sale_quarter,
        a.month,
        -- a.sdt,
        -- a.order_code,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(sale_amt_no_tax) sale_amt_no_tax,
        sum(profit_no_tax) profit_no_tax,
        sum(bbc_express_amount) bbc_express_amount,
        sum(transport_amount) transport_amount
    from tmp_flbbc_sale a 
      left join 
       csx_analyse_tmp.csx_analyse_tmp_bbc_expense_detail  b on a.order_code=b.order_code and a.operation_mode_name=b.operation_mode_name and a.type_flag=b.type_flag
       left join 
       csx_analyse_tmp.csx_analyse_tmp_tms_entrucking_order c on a.order_code=c.order_code and a.operation_mode_name=c.operation_mode_name and a.type_flag=c.type_flag
     group by 
      a.group_flag,
        a.type_flag,
        a.operation_mode_name,
        a.sale_quarter,
        a.month,
        -- a.sdt,
        -- a.order_code,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code
    ),
   tmp_sss_detail as 
    (     
        select
          substr(sdt,1,6) smonth,
          customer_code,
          business_type_code,
          operation_mode_name,
          -- 剩余金额 认领未核销金额
          sum(if(sdt = max_calday, receivable_amount, 0)) as receivable_amount,
          -- 当日应收
          if(
              sum(receivable_amount) > 0,
              sum(receivable_amount),
              0
            )  * 0.06 / 365 as capital_takes_up -- 资金占用费
    from csx_analyse_tmp.csx_analyse_tmp_sss_order_detal a 
    join (
          select
            month_of_year,
            max(calday) max_calday
          from
            csx_dim.csx_dim_basic_date
          where
            calday >= '20240101'
            and calday <= regexp_replace(date_sub(current_date, 1), '-', '')
          group by
            month_of_year
        ) d on substr(a.sdt, 1, 6) = d.month_of_year
        -- where a.customer_code='116521'
        group by    substr(sdt,1,6),
          customer_code,
          business_type_code,
          operation_mode_name
    ),
tmp_customer_income_statement as (
  SELECT
    COALESCE (a.smonth, b.smt) AS smonth,
    COALESCE (a.region_code, b.performance_region_code) AS region_code,
    COALESCE (a.region_name, b.performance_region_name) AS region_name,
    COALESCE (a.province_code, b.performance_province_code) AS province_code,
    COALESCE (a.province_name, b.performance_province_name) AS province_name,
    COALESCE (a.city_group_code, b.performance_city_code) AS city_group_code,
    COALESCE (a.city_group_name, b.performance_city_name) AS city_group_name,
    COALESCE (a.customer_code, b.customer_code) AS customer_code,
    COALESCE (a.business_type_name, b.business_type_name) AS business_type_name,
    COALESCE (b.other_expenses, 0) AS other_expenses,
    COALESCE(b.other_expenses, 0) / (a.sales_value) as sales_ratio
  FROM
    (
      SELECT
        substr(sdt, 1, 6) smonth,
        performance_region_code AS region_code,
        performance_region_name AS region_name,
        performance_province_code AS province_code,
        performance_province_name AS province_name,
        performance_city_code AS city_group_code,
        performance_city_name AS city_group_name,
        customer_code ,
        business_type_name,
        sum(sale_amt_no_tax) sales_value
      FROM
        csx_report.csx_report_sss_customer_income_statement_di
      WHERE
        sdt >= '20240801'
        and sdt <= '20241031'
      GROUP BY
        substr(sdt, 1, 6),
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        customer_code,
        business_type_name

    ) a
    LEFT JOIN (
      SELECT
        smt,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        a.customer_code,
        customer_name,
        business_type_name,
        SUM(
          IF (expense_type_name = '其他费用', expense_amt_no_tax, 0)
        ) AS other_expenses,
        SUM(
          IF (expense_type_name = '后端费用', expense_amt_no_tax, 0)
        ) AS back_end_loads,
        SUM(
          IF (expense_type_name = '后台净收支', expense_amt_no_tax, 0)
        ) AS background_net_income,
        SUM(
          IF (expense_type_name = '未税运费', expense_amt_no_tax, 0)
        ) AS transport_amount
      FROM
        csx_dim.csx_dim_sss_customer_income_write_expense_mi a
      WHERE
        smt >= '202408'
        and smt <= '202410'
      GROUP BY
        smt,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        a.customer_code,
        customer_name,
        business_type_name
    ) b ON a.smonth = b.smt
    AND a.province_code = b.performance_province_code
    AND a.customer_code = b.customer_code
    and a.business_type_name = b.business_type_name
  where
    coalesce(COALESCE(b.other_expenses, 0) / (a.sales_value), 0) <> 0
)
select a.group_flag,
        a.type_flag,
        a.operation_mode_name,
        a.sale_quarter,
        a.month,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        first_category_name,
        second_category_name,
        third_category_name,
        max_business_create_date,
        (sale_qty) sale_qty,
        (sale_cost) sale_cost,
        (sale_amt) sale_amt,
        (profit) profit,
        (sale_amt_no_tax) sale_amt_no_tax,
        (profit_no_tax) profit_no_tax,
        coalesce(receivable_amount,'')receivable_amount,
          -- 当日应收
        coalesce(capital_takes_up,'') capital_takes_up,
        coalesce(bbc_express_amount,'') bbc_express_amount,
        coalesce(transport_amount,'') transport_amount,
        coalesce(bbc_express_amount,0)+ coalesce( transport_amount,0) as express_amt,
        coalesce(sales_ratio,'') sales_ratio
from tmp_sale_expense a 
left join tmp_sss_detail b on a.customer_code=b.customer_code and a.month=b.smonth and a.operation_mode_name=b.operation_mode_name and a.business_type_code=b.business_type_code
left join 
    (select smonth,
        case when business_type_name='BBC' then '6' when business_type_name='福利业务' then '2' else business_type_name end business_type_code,
         customer_code,
         sales_ratio 
    from tmp_customer_income_statement ) c on a.customer_code=c.customer_code and a.business_type_code=c.business_type_code and a.month=c.smonth
 left join (
    select
      customer_code,
      business_type_code,
      max(create_time) max_business_create_date
    from
      csx_dim.csx_dim_crm_business_info
    where
      sdt = 'current'
      and business_type_code in (2, 6)
      and create_time >= '2024-08-01 00:00:00'
      and create_time < '2024-11-01 00:00:00' --  and customer_code='105561'
    group by
      customer_code,
      business_type_code
  ) e on a.customer_code = e.customer_code
  and a.business_type_code = e.business_type_code
  left join 
  (
      select
        customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        first_category_name,
        second_category_name,
        third_category_name
      from
        csx_dim.csx_dim_crm_customer_info
      where
        sdt = 'current'
    ) m on a.customer_code = m.customer_code

          

-- 客户损益运费、报损、装车等多项指标
-- drop table csx_analyse_tmp.csx_tmp_customer_capital_takes_up_index;
create table csx_analyse_tmp.csx_tmp_customer_capital_takes_up_index as 

with tmp_tms_customer_transport_amt as (
  -- 客户上月至今每日的运费
  select
    t1.sdt,
    t2.sdt as sale_sdt,
    type_flag,
    operation_mode_name,
    case when t3.customer_code is not null then t3.customer_code
      when t2.customer_code is not null then t2.customer_code
      else coalesce(t1.customer_code, '') end as customer_code,
    cast(
      coalesce(t2.business_type_code, t1.business_type_code, 1) as int
    ) as business_type_code,
    sum(t1.transport_amount) as transport_amount -- 运费
  from
    (
      select
        regexp_replace(send_date, '-', '') as sdt,
        customer_code,
        shipped_order_code,
        if(access_caliber = 2, 99, business_type_code) as business_type_code,
        sum(
            cast(
              excluding_tax_avg_transport_amount as decimal(20, 2)
            )
          ) as transport_amount -- 未税运费
      from
         csx_dws.csx_dws_tms_entrucking_order_detail_new_di
      where
        sdt >= regexp_replace( add_months(trunc('2024-11-12', 'MM'), -5), '-', ''  )
        and regexp_replace(send_date, '-', '') >= regexp_replace( add_months(trunc('2024-11-12', 'MM'), -5), '-', '')
        and regexp_replace(send_date, '-', '') <= regexp_replace(date_sub(current_date(), 1), '-', '')
        and supper_order_type_name in ('B端', 'M端', 'BBC')
      group by
        regexp_replace(send_date, '-', ''),
        customer_code,
        shipped_order_code,
        if(access_caliber = 2, 99, business_type_code)
    ) t1
     left join 
    (select order_code ,
            sdt,
            business_type_code,
            type_flag,
            operation_mode_name,
            customer_code
        from csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail 
            group by order_code ,
            business_type_code,
            type_flag,
            operation_mode_name,
            sdt,
            customer_code
        )  t2 on t2.order_code = t1.shipped_order_code
    left join
    (
      select
        customer_code
      from
        csx_dim.csx_dim_crm_customer_info
      where
        sdt = 'current'
    ) t3 on t1.customer_code = t3.customer_code
    where t2.customer_code is not null 
  group by
    t1.sdt,
    t2.sdt,
    type_flag,
    operation_mode_name,
    case when t3.customer_code is not null then t3.customer_code
      when t2.customer_code is not null then t2.customer_code
      else coalesce(t1.customer_code, '') end,
    cast(
      coalesce(t2.business_type_code, t1.business_type_code, 1) as int
    )
),
  tmp_transport_bbc_expense_detail as 
  (select
      regexp_replace(substr(bill_belongs_end, 1, 10), '-', '') as happen_date,
      b.sdt as sale_sdt,
      type_flag,
      operation_mode_name,
      a.customer_code,
      -- 运费
      coalesce(sum(
        cast(
          cast(settlement_amount as decimal(20, 6)) / 1.06 as decimal(20, 6)
        )
      ), 0) as bbc_express_amount
    from
       csx_report.csx_report_tms_transport_bbc_expense_detail a 
     left join 
    (select order_code ,
            customer_code,
            business_type_code,
            sdt,
            type_flag,
            operation_mode_name
        from csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail 
            group by order_code ,
            business_type_code,
            customer_code,
            sdt,
            type_flag,
            operation_mode_name
    ) b on  a.merchant_order_number=b.order_code 
    where
      a.sdt = regexp_replace(date_sub(current_date(), 1), '-', '')
      and regexp_replace(substr(bill_belongs_end, 1, 10), '-', '') >= regexp_replace(
        add_months(trunc('2024-11-12', 'MM'), -5),
        '-',
        ''
      )
      and regexp_replace(substr(bill_belongs_end, 1, 10), '-', '') <= regexp_replace(date_sub(current_date(), 1), '-', '')
      and  b.sdt is not null 
    group by
      regexp_replace(substr(bill_belongs_end, 1, 10), '-', ''),
      a.customer_code,
      b.sdt,
      type_flag,
      operation_mode_name
  
  )
select
  sdt,
  customer_code,
  business_type_code,
  type_flag,
  operation_mode_name,
  sum(receivable_amount) receivable_amount,
  -- 应收金额
 
  sum(capital_takes_up) capital_takes_up,
  -- 资金占用费
  sum(transport_amount) transport_amount,
  -- 运费
  sum(bbc_express_amount) bbc_express_amount
  -- bbc快递费	单独字段 
from
  (
   
    select
      sale_sdt as sdt,
      customer_code,
      business_type_code,
      type_flag,
      operation_mode_name,
      0 as receivable_amount,
      -- 应收金额
      0 as capital_takes_up,
      -- 资金占用费
      coalesce(transport_amount, 0) as transport_amount,
      -- 运费
      0 as bbc_express_amount
    from
      tmp_tms_customer_transport_amt 
    union all  
    -- 客户上月至今每日的运费 BBC  签收时间signing_time改 	账单所属期间结束bill_belongs_end
    select
    --   regexp_replace(substr(bill_belongs_end, 1, 10), '-', '') as happen_date,
      sale_sdt as sdt,
      a.customer_code,
      6 as business_type_code,
      type_flag,
      operation_mode_name,
      0 as receivable_amount,
      -- 应收金额

      0 as capital_takes_up,
      -- 资金占用费
      0 as transport_amount,
      -- 运费
      sum(bbc_express_amount) bbc_express_amount
    from
       tmp_transport_bbc_expense_detail a 

    group by
    --   regexp_replace(substr(bill_belongs_end, 1, 10), '-', ''),
      a.customer_code,
      sale_sdt,
      type_flag,
      operation_mode_name
     union all  
      -- 客户上月至今每日的当日应收金额、昨日应收金额、资金占用费
    select
      sdt,
      customer_code,
      business_type_code,
      type_flag,
      operation_mode_name,
      receivable_amount,
      -- 应收金额
      capital_takes_up,
      -- 资金占用费
      0 transport_amount,
      -- 运费
      0 as bbc_express_amount
      -- bbc快递费	单独字段
    from
      csx_analyse_tmp.csx_tmp_sss_customer_receivable_amount
      ) a 
      group by sdt,
  customer_code,
  business_type_code,
  type_flag,
  operation_mode_name
  ;

-- 结果导数表

  with tmp_customer_income_statement as (
SELECT COALESCE (a.smonth, b.smt) AS smonth,
    COALESCE (a.region_code, b.performance_region_code) AS region_code,
    COALESCE (a.region_name, b.performance_region_name) AS region_name,
    COALESCE (a.province_code, b.performance_province_code) AS province_code,
    COALESCE (a.province_name, b.performance_province_name) AS province_name,
    COALESCE (a.city_group_code, b.performance_city_code) AS city_group_code,
    COALESCE (a.city_group_name, b.performance_city_name) AS city_group_name,
    a.channel_code,
    a.channel_name,
    COALESCE (a.customer_no, b.customer_code) AS customer_no,
    COALESCE (a.customer_name, b.customer_name) AS customer_name,
    a.second_category_name,
    COALESCE ( a.business_type_name, b.business_type_name ) AS business_type_name,

    COALESCE (b.other_expenses, 0) AS other_expenses,
    COALESCE(b.other_expenses,0)/(a.sales_value) as sales_ratio
FROM (
        SELECT substr(sdt, 1, 6) smonth,
            performance_region_code AS region_code,
            performance_region_name AS region_name,
            performance_province_code AS province_code,
            performance_province_name AS province_name,
            performance_city_code AS city_group_code,
            performance_city_name AS city_group_name,
            channel_code,
            channel_name,
            customer_code AS customer_no,
            customer_name,
            second_category_name,
            business_type_name,
            sales_user_id AS sales_id,
            sales_user_number AS work_no,
            sales_user_name AS sales_name,
            sum(sale_amt_no_tax) sales_value,
            sum(profit_no_tax) profit,

            sum(
                IF (
                    business_type_name IN ('前置仓', '项目供应商'),
                    0,
                    capital_takes_up
                )
            ) AS capital_takes_up,
            sum(transport_amount) transport_amount,
            sum(
                cast(
                    frmloss_amt AS DECIMAL (20, 6)
                )
            ) AS frmloss_amt,
            sum(
                cast(
                    order_amount_no_tax AS DECIMAL (20, 6)
                )
            ) AS order_amount_no_tax,
            sum(
                cast(
                    exclude_order_amount_no_tax AS DECIMAL (20, 6)
                )
            ) AS exclude_order_amount_no_tax,
            sum(bbc_express_amount) AS bbc_express_amount,
            sum(sale_amt) sale_amt,
            sum(delivery_sale_amt) delivery_sale_amt,
            sum(direct_sale_amt) direct_sale_amt,
            sum(delivery_sale_amt_no_tax) delivery_sale_amt_no_tax,
            sum(direct_sale_amt_no_tax) direct_sale_amt_no_tax,
            sum(delivery_profit_no_tax) delivery_profit_no_tax,
            sum(direct_profit_no_tax) direct_profit_no_tax
        FROM csx_report.csx_report_sss_customer_income_statement_di
        WHERE sdt >= '20240801'
            and sdt <= '20241031'
        GROUP BY substr(sdt, 1, 6),
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
            second_category_name,
            business_type_name,
            sales_user_id,
            sales_user_number,
            sales_user_name
    ) a
    LEFT JOIN (
        SELECT smt,
            performance_region_code,
            performance_region_name,
            performance_province_code,
            performance_province_name,
            performance_city_code,
            performance_city_name,
            a.customer_code,
            customer_name,
            business_type_name ,
            SUM(
                IF (
                    expense_type_name = '其他费用',
                    expense_amt_no_tax,
                    0
                )
            ) AS other_expenses,
            SUM(
                IF (
                    expense_type_name = '后端费用',
                    expense_amt_no_tax,
                    0
                )
            ) AS back_end_loads,
            SUM(
                IF (
                    expense_type_name = '后台净收支',
                    expense_amt_no_tax,
                    0
                )
            ) AS background_net_income,
            SUM(
                IF (
                    expense_type_name = '未税运费',
                    expense_amt_no_tax,
                    0
                )
            ) AS transport_amount
        FROM csx_dim.csx_dim_sss_customer_income_write_expense_mi a 
    
        WHERE smt >= '202408'
            and smt <= '202410'
        GROUP BY smt,
            performance_region_code,
            performance_region_name,
            performance_province_code,
            performance_province_name,
            performance_city_code,
            performance_city_name,
            a.customer_code,
            customer_name,
            business_type_name
    ) b ON a.smonth = b.smt
    AND a.province_code = b.performance_province_code
    AND a.customer_no = b.customer_code
    and a.business_type_name =b.business_type_name
    where 
    coalesce(COALESCE(b.other_expenses,0)/(a.sales_value) ,0) <>0 
) ,
tmp_flbbc_sale
as (select a.group_flag,
        a.type_flag,
        a.operation_mode_name,
        a.sale_quarter,
        a.month,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        first_category_name,
        second_category_name,
        third_category_name,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(sale_amt_no_tax) sale_amt_no_tax,
        sum(profit_no_tax) profit_no_tax,
        sum(receivable_amount) as receivable_amount,
        sum(capital_takes_up) as capital_takes_up,
        sum(transport_amount) as transport_amount,
        sum(bbc_express_amount) as bbc_express_amount,
        sum(all_transport_amt) all_transport_amt
    from  csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail a 
    join
    (
       
      select customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        first_category_name,
        second_category_name,
        third_category_name
      from
        csx_dim.csx_dim_crm_customer_info
      where
        sdt = 'current'
    ) m on a.customer_code=m.customer_code
    left join 
 ( select substr(a.sdt, 1, 6) as smt,
    customer_code,
    business_type_code,
    type_flag,
    operation_mode_name,
    sum(if(sdt = max_calday, receivable_amount, 0)) as receivable_amount,
    sum(capital_takes_up) as capital_takes_up,
    sum(transport_amount) as transport_amount,
    sum(bbc_express_amount) as bbc_express_amount,
    sum(transport_amount)+sum(bbc_express_amount) all_transport_amt
  from   csx_analyse_tmp.csx_tmp_customer_capital_takes_up_index a 
  join 
  
    (select
      month_of_year,
      max(calday) max_calday
    from
      csx_dim.csx_dim_basic_date
    where
      calday >= '20190101'
      and calday <= regexp_replace(date_sub(current_date, 1), '-', '')
    group by
      month_of_year
  ) d on substr(a.sdt, 1, 6) = d.month_of_year 
  group by  substr(a.sdt, 1, 6),
    customer_code,
    business_type_code,
    type_flag,
    operation_mode_name
    ) b  on a.customer_code=b.customer_code 
        and a.month=b.smt 
        and a.type_flag=b.type_flag
        and a.business_type_code=b.business_type_code 
        and a.operation_mode_name=b.operation_mode_name
    group by  a.group_flag,
        a.type_flag,
        a.operation_mode_name,
        a.sale_quarter,
        a.month,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        first_category_name,
        second_category_name,
        third_category_name
        )
        select a.*,b.sales_ratio from tmp_flbbc_sale a 
        left join 
        (select smonth,
            customer_no,
            business_type_name,
            case when business_type_name ='福利业务' then '2'
                when business_type_name='BBC' then '6'
                else business_type_name end business_type_code,
                sales_ratio
        from tmp_customer_income_statement ) b on a.customer_code=b.customer_no and a.business_type_code=b.business_type_code and a.month=b.smonth