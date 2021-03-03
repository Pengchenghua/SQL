 -- 管理分类树
SELECT  classify_large_code
       ,classify_large_name
       ,up
       ,fr_up
FROM 
(
	SELECT  DISTINCT classify_large_code
	       ,classify_large_name
	       ,'' AS up
	       ,'' fr_up
	FROM csx_dw.dws_basic_w_a_manage_classify_m
	WHERE sdt='current'  
	UNION ALL
	SELECT  DISTINCT classify_middle_code 
	       ,classify_middle_name 
	       ,'1'                 AS up
	       ,classify_large_code AS fr_up
	FROM csx_dw.dws_basic_w_a_manage_classify_m
	WHERE sdt='current'  
	UNION ALL
	SELECT  DISTINCT classify_small_code 
	       ,classify_small_name 
	       ,'2'                  AS up
	       ,classify_middle_code AS fr_up
	FROM csx_dw.dws_basic_w_a_manage_classify_m
	WHERE sdt='current'  
)a
ORDER BY up,fr_up ; sql("privileges_data","

SELECT  CASE WHEN au.sys_province_id IN ('35','36') THEN '35' ELSE au.sys_province_id END       AS id
       ,CASE WHEN au.sys_province_id IN ('35','36') THEN '平台-供应链' ELSE au.sys_province_name END AS name
FROM da_auth da
LEFT JOIN da_sale_province au
ON da.da_permission_id = au.sys_province_id
WHERE da.is_able = 1 
AND da.da_type_id = -2 
AND da.is_deleted = 0 
AND au.is_able = 1 
AND au.is_deleted = 0 
AND au.sys_province_id not IN ('-100','999') 
AND da.usr_user_id = "+$userId+";",2) 
;

-- ds_channel
select channel_code ,channel_name from csx_dw.dws_crm_w_a_customer_20200924 where sdt='current' and channel_code !='3' group by  channel_code ,channel_name order by channel_code