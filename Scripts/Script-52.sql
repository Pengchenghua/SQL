select
	case
		when au.sys_province_id in ('35',
		'36') then '35'
		else au.sys_province_id
	end as id,
	case
		when au.sys_province_id in ('35',
		'36') then '平台-供应链'
		else au.sys_province_name
	end as name
from
	da_auth da
left join da_sale_province au on
	da.da_permission_id = au.sys_province_id
where
	da.is_able = 1
	and da.da_type_id = -2
	and da.is_deleted = 0
	and au.is_able = 1
	and au.is_deleted = 0
	and au.sys_province_id not in ('-100',
	'999')
	 and da.usr_user_id = ${userId};
	
	
	;
	

select * from
	da_auth;
	select * from dwd_gss_r_d_settle_bill dgrdsb 