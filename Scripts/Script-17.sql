select
    sflag ,
    hkont ,
    account_name ,
    comp_code ,
    comp_name ,
    dist ,
    sales_city ,
    kunnr ,
    name ,
    first_category ,
    second_category ,
    third_category ,
    work_no ,
    sales_name ,
    first_supervisor_name,
    credit_limit ,
    temp_credit_limit ,
    zterm ,
    diff ,
    ac_all ,
    ac_wdq ,
    ac_15d ,
    ac_30d ,
    ac_60d ,
    ac_90d ,
    ac_120d ,
    ac_180d ,
    ac_365d ,
    ac_2y ,
    ac_3y ,
    ac_over3y
from
    (
    select
        case
            when b.channel is null then '其他'
            else b.channel
        end sflag , hkont , a.account_name , comp_code , comp_name ,
        case
            when (b.sales_province is null
            and comp_code = '1933') then '福建省'
            when (b.sales_province is null
            and comp_name like '%北京%' )then concat(substring(comp_name, 1, 2 ), '市')
            when (b.sales_province is null )then concat(substring(comp_name, 1, 2 ), '省')
            else b.sales_province
        end as dist , b.sales_city , replace(kunnr, '0000', '') as kunnr, b.customer_name name , b.first_category , b.second_category , b.third_category , b.work_no , b.sales_name , b.first_supervisor_name , b.credit_limit , b.temp_credit_limit , zterm , diff , ac_all ,
        case
            when ac_all<0 then ac_all
            else ac_wdq
        end ac_wdq,
        case
            when ac_all<0 then 0
            else ac_15d
        end ac_15d,
        case
            when ac_all<0 then 0
            else ac_30d
        end ac_30d,
        case
            when ac_all<0 then 0
            else ac_60d
        end ac_60d,
        case
            when ac_all<0 then 0
            else ac_90d
        end ac_90d,
        case
            when ac_all<0 then 0
            else ac_120d
        end ac_120d,
        case
            when ac_all<0 then 0
            else ac_180d
        end ac_180d,
        case
            when ac_all<0 then 0
            else ac_365d
        end ac_365d,
        case
            when ac_all<0 then 0
            else ac_2y
        end ac_2y,
        case
            when ac_all<0 then 0
            else ac_3y
        end ac_3y,
        case
            when ac_all<0 then 0
            else ac_over3y
        end ac_over3y
    from
        (select
    sflag ,
    hkont ,
    account_name,
    comp_code ,
    comp_name ,
    prctr ,
    shop_name ,
    kunnr ,
    -- NAME        ,
    zterm ,
    diff ,
    ac_all ,
    ac_wdq ,
    ac_15d ,
    ac_30d ,
    ac_60d ,
    ac_90d ,
    ac_120d ,
    ac_180d ,
    ac_365d ,
    ac_2y ,
    ac_3y ,
    ac_over3y ,
    sdt
from
    data_center_report.account_age_dtl_fct_new a
where
    sdt = '${SDATE}'
    and a.ac_all <> 0
    and kunnr <> '0000910001'
    and hkont not in ('1398030000', '1398040000', '1399020000', '2202010000', '1399010000', '1398010000', '1398020000')) a
    left join data_center_report.customer_m b on
        lpad(a.kunnr, 10, '0')= lpad(b.customer_no, 10, '0')
union all
    select
        a.sflag , hkont , a.account_name, comp_code , comp_name ,
        case
            when substr(comp_name, 1, 2)in('上海', '北京', '重庆') then concat(substr(comp_name, 1, 2), '市')
            when substr(comp_name, 1, 2)= '永辉' then '福建省'
            else concat(substr(comp_name, 1, 2), '省')
        end dist , 
        substr(comp_name, 1, 2) sales_city, 
        replace(kunnr, '0000', '') as kunnr , 
        name , 
        '个人及其他' first_category , 
        '个人及其他' second_category , 
        '个人及其他' third_category , 
        '' work_no , 
        '' sales_name , 
        '' first_supervisor_name, 
        0 as credit_limit, 
        0 as temp_credit_limit, 
        zterm , 
        diff , 
        ac_all,
        case
            when ac_all<0 then ac_all
            else ac_wdq
        end ac_wdq,
        case
            when ac_all<0 then 0
            else ac_15d
        end ac_15d,
        case
            when ac_all<0 then 0
            else ac_30d
        end ac_30d,
        case
            when ac_all<0 then 0
            else ac_60d
        end ac_60d,
        case
            when ac_all<0 then 0
            else ac_90d
        end ac_90d,
        case
            when ac_all<0 then 0
            else ac_120d
        end ac_120d,
        case
            when ac_all<0 then 0
            else ac_180d
        end ac_180d,
        case
            when ac_all<0 then 0
            else ac_365d
        end ac_365d,
        case
            when ac_all<0 then 0
            else ac_2y
        end ac_2y,
        case
            when ac_all<0 then 0
            else ac_3y
        end ac_3y,
        case
            when ac_all<0 then 0
            else ac_over3y
        end ac_over3y
    from
        data_center_report.account_age_dtl_fct_new a
    where
        sdt = '${SDATE}'
        and a.ac_all <> 0
        and kunnr = '0000910001' ) x
where
    1 = 1 
    ${if(len(prov)== 0, "", " and ((case when dist in ('平台-生鲜采购','平台-食百采购') then '平台-供应链' else dist end ) in ('" + trim(prov)+ "'))")} ${if(len(cust)== 0, "", " and kunnr in ('" + cust + "') ")}
order by
    sflag ,
    comp_code,
    dist ,
    kunnr ;