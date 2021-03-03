select distinct 
  au.sys_province_id as id,
  au.sys_province_name as name,
  length(au.sys_province_name)
 from
  da_auth da
left join
   da_sale_province au
on
  da.da_permission_id = au.sys_province_id
where
  da.is_able = 1
and
  da.da_type_id = -2
and
  da.is_deleted = 0
and
  au.is_able = 1
and
  au.is_deleted = 0
--  and
--  da.usr_user_id = "${userId}"
order by sys_province_id 
;

select distinct case when  au.sys_province_id in ('35','36') then '35' else  au.sys_province_id end  as id, 
case when  au.sys_province_id in ('35','36') then '平台-供应链' else  au.sys_province_name end as name 
from  da_auth da left join  da_sale_province au on   da.da_permission_id = au.sys_province_id 
where  da.is_able = 1 and  da.da_type_id = -2 and  da.is_deleted = 0 and  au.is_able = 1 
and  au.is_deleted = 0 and au.sys_province_id  not in ('-100','16','999')  
-- and  da.usr_user_id = "$userId"
;

select * from data_center_report.sale_warzone01_detail_dtl  ;




select  au.sys_province_id   as id,  au.sys_province_name  as name from  da_auth da left join  da_sale_province au on  da.da_permission_id = au.sys_province_id where  da.is_able = 1 and  da.da_type_id = -2 and  da.is_deleted = 0 and   au.is_able = 1 and   au.is_deleted = 0 and au.sys_province_id  not in ('-100','16','999')
-- and  da.usr_user_id = "+$userId+"  
order by sys_province_id ;

select * from fine_weixin_user_relation fwur ;

    select
            case
                when b.channel is null
                    then '其他'
                    else b.channel
            end sflag                        ,
            hkont                            ,
            a.account_name                   ,
            comp_code                        ,
            comp_name                        ,
           case when  (b.sales_province is null  and comp_code ='1933') then '福建省' 
                when (b.sales_province is null and comp_name like '%北京%' )then concat(substring(comp_name,1,2 ),'市')
                when (b.sales_province is null  )then concat(substring(comp_name,1,2 ),'省')
                else b.sales_province end  as  dist            ,
            b.sales_city                     ,
            replace(kunnr,'0000','') as kunnr,
            b.customer_name          as    name ,
            b.first_category                 ,
            b.second_category                ,
            b.third_category                 ,
            b.work_no                        ,
            b.sales_name                     ,
            b.first_supervisor_name          ,
            b.credit_limit                   ,
            b.temp_credit_limit              ,
            zterm                            ,
            diff                             ,
            ac_all                           ,
            case
                when ac_all<0
                    then ac_all
                    else ac_wdq
            end ac_wdq,
            case
                when ac_all<0
                    then 0
                    else ac_15d
            end ac_15d,
            case
                when ac_all<0
                    then 0
                    else ac_30d
            end ac_30d,
            case
                when ac_all<0
                    then 0
                    else ac_60d
            end ac_60d,
            case
                when ac_all<0
                    then 0
                    else ac_90d
            end ac_90d,
            case
                when ac_all<0
                    then 0
                    else ac_120d
            end ac_120d,
            case
                when ac_all<0
                    then 0
                    else ac_180d
            end ac_180d,
            case
                when ac_all<0
                    then 0
                    else ac_365d
            end ac_365d,
            case
                when ac_all<0
                    then 0
                    else ac_2y
            end ac_2y,
            case
                when ac_all<0
                    then 0
                    else ac_3y
            end ac_3y,
            case
                when ac_all<0
                    then 0
                    else ac_over3y
            end ac_over3y
        from
            (
                select
                    sflag       ,
                    hkont       ,
                    account_name,
                    comp_code   ,
                    comp_name   ,
                    prctr       ,
                    shop_name   ,
                    kunnr       ,
                    -- NAME        ,
                    zterm       ,
                    diff        ,
                    ac_all      ,
                    ac_wdq      ,
                    ac_15d      ,
                    ac_30d      ,
                    ac_60d      ,
                    ac_90d      ,
                    ac_120d     ,
                    ac_180d     ,
                    ac_365d     ,
                    ac_2y       ,
                    ac_3y       ,
                    ac_over3y   ,
                    sdt
                from
                    data_center_report.account_age_dtl_fct_new a
                where
                    sdt          ='${SDATE}'
                    and a.ac_all<>0
                    and kunnr   <>'0000910001'
                    and hkont not in ('1398030000',
                                      '1398040000',
                                      '1399020000',
                                      '2202010000',
                                      '1399010000',
                                      '1398020000',
                                      '1398010000')
            )
            a
            left join
                data_center_report.customer_m b
                on
                    lpad(a.kunnr,10,'0')=lpad(b.customer_no,10,'0');

                
                
                
select
    distinct
    case
        when au.sys_province_id in ('35', '36') then '35'
        else au.sys_province_id
    end as id,
    case
        when au.sys_province_id in ('35', '36') then '平台-供应链'
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
  --  and usr_user_id='1000000061359'
   -- and au.sys_province_id  in ('-1');
    
    
   ;
   
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
	and da.usr_user_id = "+$userId+"
order by
	sys_province_id;