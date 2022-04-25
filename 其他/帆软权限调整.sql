SELECT DISTINCT  x.user_id , b.parent_code,b.code as id,b.name,uu.user_name ,uu.user_work_no FROM csx_b2b_data_center.sys_v2_user_data_auth x 
join csx_b2b_data_center.basic_region_province_citygroup b on x.data_auth_id =b.id 
join csx_b2b_data_center.usr_user uu on x.user_id =uu.user_id 
WHERE x.user_id = '1000000166011'  
and b.`level` =2
ORDER BY cast(b.code as signed int) asc ;