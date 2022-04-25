SET
  mapreduce.job.queuename = caishixian;
set
  mapreduce.job.reduces = 80;
set
  hive.map.aggr = true;
set
  hive.groupby.skewindata = true;
set
  hive.exec.parallel = true;
set
  hive.exec.dynamic.partition = true;
set
  hive.exec.dynamic.partition.mode = nonstrict;

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
      AND a.budat >= '20190101'
      AND budat <= '20191130'
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
      sdt >= '20190101'
      AND sdt <= '20191130'
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
