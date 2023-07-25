 -- 商机签约统计
 -- csx_data_market 集市查询 dws_crm_w_a_business_customer
 
 SELECT
  substr(sign_time, 1, 7) AS mth,
        substr(sign_time, 1, 10) AS dt,
  sales_region_name,
  province_name,
        business_number AS `商机号`,
        customer_no,
        CASE WHEN attribute = 1 THEN '日配'
          WHEN attribute = 2 THEN '福利'
                WHEN attribute = 3 THEN '大宗贸易'
                WHEN attribute = 5 THEN 'BBC'
                END AS `业务类型`,
 estimate_contract_amount AS `签约金额`
FROM csx_data_market.dws_crm_w_a_business_customer
WHERE business_stage = 5 AND sign_time >= '2021-01-01 00:00:00' AND sign_time <= '2021-02-28 23:59:59' AND attribute IN (1,2,3,5);


 SELECT
	substr(sign_time, 1, 7) AS mth,
	substr(sign_time, 1, 10) AS dt,
	sales_region_name,
	province_name,
	business_number AS `商机号`,
	a.customer_no,
	a.customer_name,
	sales_name,
	first_category_code,
	first_category_name,
	second_category_code,
	second_category_name,
	third_category_code,
	third_category_name,
	gross_profit_rate,
	contract_cycle,
	CASE
		WHEN attribute = 1 THEN '日配'
		WHEN attribute = 2 THEN '福利'
		WHEN attribute = 3 THEN '大宗贸易'
		WHEN attribute = 5 THEN 'BBC'
	END AS `业务类型`,
	estimate_contract_amount AS `签约金额`
FROM
	csx_data_market.dws_crm_w_a_business_customer a
WHERE
	business_stage = 5
	AND sign_time >= '2021-11-01 00:00:00'
	AND sign_time <= '2022-03-01 23:59:59'
	AND attribute IN (1, 2, 3, 5);
