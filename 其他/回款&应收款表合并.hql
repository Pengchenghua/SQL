drop table csx_dw.receivables_collection;
CREATE TABLE IF NOT EXISTS csx_dw.receivables_collection
(
years string comment '年度',
quarters string comment '季度',
months string comment '月度',
cash_date string comment '回款日期',
subject_code string comment '科目类型',
--subject_name string comment '科目名称',
`company_code` string COMMENT '公司代码', 
 company_name string comment '公司代码名称',
`profit_center_code` string COMMENT '利润中心', 
`profit_center_name` string COMMENT '利润名称', 
`customer_no` string COMMENT '客户编码', 
`customer_name` string COMMENT '客户名称', 
channel string comment'渠道',
province_code string comment'省区编码',
province_name string comment'省区名称',
sales_province_code string comment'客户省区编码',
sales_province string comment'客户省区名称',
sales_name      string comment '销售员',
work_no         string comment '销售员工号',
customer_type   string comment '01长期客户；02临时客户',
sign_date       string comment '签约日期',
contract_begin_time string comment '合同开始时间',
`contract_end_time` string COMMENT '合同结束时间', 
first_category  string comment '企业属性',
attribute       string comment '客户属性',
credit_limit    string comment '信控金额',
`cash_amt` decimal(26,4) COMMENT '回款金额',
  `ac_all` decimal(26,4) COMMENT '全部账款', 
`ac_wdq` decimal(26,4) COMMENT '未到期账款', 
  `ac_15d` decimal(26,4) COMMENT '15天内账款', 
  `ac_30d` decimal(26,4) COMMENT '30天内账款', 
  `ac_60d` decimal(26,4) COMMENT '60天内账款', 
  `ac_90d` decimal(26,4) COMMENT '90天内账款', 
  `ac_120d` decimal(26,4) COMMENT '120天内账款', 
  `ac_180d` decimal(26,4) COMMENT '半年内账款', 
  `ac_365d` decimal(26,4) COMMENT '1年内账款', 
  `ac_2y` decimal(26,4) COMMENT '2年内账款', 
  `ac_3y` decimal(26,4) COMMENT '3年内账款', 
  `ac_over3y` decimal(26,4) COMMENT '逾期3年账款'
  )comment '回款金额与应收帐款表'
partitioned by (sdt string comment '日期分区')
stored as parquet
;


SET mapreduce.job.queuename                 =caishixian;
set mapreduce.job.reduces                   =80;
set hive.map.aggr                           = true;
set hive.groupby.skewindata                 =true;
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =10000;    --在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=100000;   --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错

DROP TABLE csx_dw.recevibes_cash;
CREATE TEMPORARY TABLE csx_dw.recevibes_cash AS
SELECT
  substr(cash_date, 1, 4) as years,
  concat(
    substr(cash_date, 1, 4),
    floor(substr(cash_date, 5, 2) / 3.1) + 1
  ) as quarters,
  substr(cash_date, 1, 6) as months,
  cash_date,
  subject_code,
  company_code,
  company_name,
  profit_center_code,
  profit_center_name,
  regexp_replace(a.customer_no, '(^0*)', '') customer_no,
  customer_name,
  channel,
  province_code,
  province_name,
  sales_province_code,
  sales_province,
  sales_name,
  work_no,
  customer_type,
  sign_date,
  contract_begin_time,
  contract_end_time,
  first_category,
  attribute,
  credit_limit,
  sum(cash_amt) cash_amt,
  sum(ac_all) ac_all,
  sum(ac_wdq) ac_wdq,
  sum(ac_15d) ac_15d,
  sum(ac_30d) ac_30d,
  sum(ac_60d) ac_60d,
  sum(ac_90d) ac_90d,
  sum(ac_120d) ac_120d,
  sum(ac_180d) ac_180d,
  sum(ac_365d) ac_365d,
  sum(ac_2y) ac_2y,
  sum(ac_3y) ac_3y,
  sum(ac_over3y) ac_over3y
FROM (
    SELECT
      budat AS cash_date,
      hkont AS subject_code,
      bukrs AS comp_code,
      prctr AS shop_id,
      kunnr AS customer_no,
      cast(dmbtr AS decimal(26, 6)) AS cash_amt,
      0 ac_all,
      0 ac_wdq,
      0 ac_15d,
      0 ac_30d,
      0 ac_60d,
      0 ac_90d,
      0 ac_120d,
      0 ac_180d,
      0 ac_365d,
      0 ac_2y,
      0 ac_3y,
      0 ac_over3y
    FROM ods_ecc.ecc_ytbcustomer AS a
    WHERE
      hkont LIKE '1122%'
      AND sdt = regexp_replace(
        to_date(date_sub(current_timestamp(), 1)),
        '-',
        ''
      ) -- AND substr(budat,1,4) = '2019'
      AND a.budat = '?'
      --AND budat <= '20191130'
      AND substr(a.belnr, 1, 1) <> '6'
      AND mandt = '800'
      AND (
        substr(kunnr, 1, 1) NOT IN (
          'G',
          'L',
          'V',
          'S'
        )
        OR kunnr = 'S9961'
      )
    UNION ALL
    SELECT
      sdt AS cash_date,
      hkont AS subject_code,
      comp_code,
      prctr AS shop_id,
      kunnr AS customer_no,
      0 cash_amt,
      ac_all,
      ac_wdq,
      ac_15d,
      ac_30d,
      ac_60d,
      ac_90d,
      ac_120d,
      ac_180d,
      ac_365d,
      ac_2y,
      ac_3y,
      ac_over3y
    FROM csx_dw.account_age_dtl_fct_new
    WHERE
      sdt = '?'
     -- AND sdt <= '20191130'
  ) a
JOIN (
    SELECT
      customer_no,
      customer_name,
      channel,
      province_code,
      province_name,
      sales_province,
      sales_province_code,
      sales_name,
      work_no,
      customer_type,
      regexp_replace(to_date(sign_time), '-', '') AS sign_date,
      regexp_replace(to_date(contract_begin_time), '-', '') AS contract_begin_time,
      regexp_replace(to_date(contract_end_time), '-', '') AS contract_end_time,
      first_category,
      attribute,
      credit_limit
    FROM csx_dw.customer_m
    WHERE
      sdt = regexp_replace(
        to_date(date_sub(current_timestamp(), 1)),
        '-',
        ''
      )
  ) AS b ON regexp_replace(a.customer_no, '(^0*)', '') = b.customer_no
JOIN (
    SELECT
      profit_center_code,
      profit_center_name,
      company_code,
      company_name
    FROM csx_ods.source_basic_w_a_md_cost_center
    WHERE
      sdt = regexp_replace(
        to_date(date_sub(current_timestamp(), 1)),
        '-',
        ''
      )
  ) AS c ON regexp_replace(a.shop_id, '(^0*)', '') = profit_center_code
GROUP BY
  cash_date,
  subject_code,
  comp_code,
  profit_center_code,
  profit_center_name,
  regexp_replace(a.customer_no, '(^0*)', ''),
  customer_name,
  company_code,
  company_name,
  contract_begin_time,
  contract_end_time,
  sign_date,
  first_category,
  attribute,
  credit_limit,
  channel,
  province_code,
  province_name,
  sales_province,
  sales_province_code,
  sales_name,
  work_no,
  customer_type;

INSERT into table csx_dw.receivables_collection PARTITION (sdt)
select
  years,
  quarters,
  months,
  cash_date,
  subject_code,
  company_code,
  company_name,
  profit_center_code,
  profit_center_name,
  customer_no,
  customer_name,
  channel,
  province_code,
  province_name,
  sales_province_code,
  sales_province,
  sales_name,
  work_no,
  customer_type,
  sign_date,
  contract_begin_time,
  contract_end_time,
  first_category,
  attribute,
  credit_limit,
  cash_amt cash_amt,
  ac_all ac_all,
  ac_wdq ac_wdq,
  ac_15d ac_15d,
  ac_30d ac_30d,
  ac_60d ac_60d,
  ac_90d ac_90d,
  ac_120d ac_120d,
  ac_180d ac_180d,
  ac_365d ac_365d,
  ac_2y ac_2y,
  ac_3y ac_3y,
  ac_over3y ac_over3y,
  cash_date as sdt
from csx_dw.recevibes_cash;
