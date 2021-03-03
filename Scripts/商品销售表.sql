select
${if(box==false,"","province_code ,province_name,")} 
    division_code,
    division_name,
    firm_id,
    firm_name,
    round(sum(sale)/10000,2) sale,
    round(sum(profit)/10000,2) profit,
    sum(profit)/sum(sale)*1.00 as prorate,
    sum(sale_sku)sale_sku,
    sum(sale_cust)sale_cust,
    sum(sale_no)sale_no,
    round(sum(sale)/sum(sale_no)/10000,2) as avg_sale_no,
    sum(sale_ratio)sale_ratio
    from (
select
    division_code,
    division_name,
    ${if(box==false,"","province_code ,province_name,")} 
    firm_id,
    firm_name,
    sale,
    profit,
    sale_sku,
    sale_cust,
    sale_no,
    sale/sum(sale)over(${if(box==false,"","partition by province_code,province_name ")} ) as sale_ratio
FROM (
select
    division_code,
    division_name,
    ${if(box==false,""," province_code ,province_name,")} 
    COALESCE(b.firm_id,
    division_code) as firm_id,
    COALESCE(b.firm_name,
    division_name)as firm_name,
    SUM(sales_value)sale,
    sum(profit)profit,
    ndv(DISTINCT customer_no)sale_cust,
    count(DISTINCT goods_code)sale_sku,
    cast (ndv(DISTINCT case when return_flag !='X' then order_no end) as bigint)  sale_no
from
    csx_dw.dws_sale_r_d_customer_sale a
left join (
    select
        category_small_code ,commercial_firm_level as firm_id,commercial_firm_name as firm_name
    from
        csx_dw.dws_basic_w_a_category_m 
    where
        sdt = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-',''))b on a.category_small_code=b.category_small_code
where
    sdt >= regexp_replace(to_date('${sdate}'),'-','')
    and sdt <= regexp_replace(to_date('${edate}'),'-','')
    ${if(len(prov)==0,"","and province_code in ('"+prov+"')")}
    ${if(len(chann)==0,"","and channel in ('"+chann+"')")}
    group by 
    division_code,
    division_name,
    ${if(box==false,""," province_code ,province_name,")} 
    COALESCE(b.firm_id,
    division_code) ,
    COALESCE(b.firm_name,
    division_name)
    )a
    where 1=1   
union all 
select
    div_id as  division_code,
    div_name as division_name,
    ${if(box==false,"","province_code ,province_name,")} 
    firm_id,
    firm_name,
    sale,
    profit,
    sale_sku,
    sale_cust,
    sale_no,
    sale/sum(sale)over(${if(box==false,"","partition by  province_code ,province_name")} ) as sale_ratio
FROM (
select
    CASE when division_code in ('10','11') then '10' when division_code in('12','13','14') then '12' else division_code end div_id,
    cASE when division_code in ('10','11') then '生鲜采购部' when division_code in('12','13','14') then '食百采购部' else division_name end div_name,
    ${if(box==false,""," province_code ,province_name,")} 
    '00' as firm_id,
    '小计' firm_name,
    SUM(sales_value)sale,
    sum(profit)profit,
    count(DISTINCT goods_code)sale_sku,
    ndv(DISTINCT customer_no)sale_cust,
    cast (ndv(DISTINCT case when return_flag !='X' then order_no end) as bigint)  sale_no
from
    csx_dw.dws_sale_r_d_customer_sale a
left join (
    select
        category_small_code ,commercial_firm_level as firm_id,commercial_firm_name as firm_name
    from
        csx_dw.dws_basic_w_a_category_m 
    where
        sdt = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-',''))b on a.category_small_code=b.category_small_code
where
    sdt >= regexp_replace(to_date('${sdate}'),'-','')
    and sdt <= regexp_replace(to_date('${edate}'),'-','')
${if(len(prov)==0,"","and province_code in ('"+prov+"')")}
${if(len(chann)==0,"","and channel in ('"+chann+"')")}
    group by 
    ${if(box==false,"","province_code ,province_name,")} 
     CASE when division_code in ('10','11') then '10' when division_code in('12','13','14') then '12' else division_code end   ,
    cASE when division_code in ('10','11') then '生鲜采购部' when division_code in('12','13','14') then '食百采购部' else division_name end
    )a
    where 1=1
    union all   
select
    '00'division_code,
    '总计'division_name,
    ${if(box==false,""," province_code ,province_name,")} 
    '' as firm_id,
    '' firm_name,
    SUM(sales_value)sale,
    sum(profit)profit,
    count(DISTINCT goods_code)sale_sku,
    ndv(DISTINCT customer_no)sale_cust, 
    cast (ndv(DISTINCT case when return_flag !='X' then order_no end) as bigint)  sale_no,
    1.0 sale_ratio
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >= regexp_replace(to_date('${sdate}'),'-','')
    and sdt <= regexp_replace(to_date('${edate}'),'-','')
${if(len(prov)==0,"","and province_code in ('"+prov+"')")}
${if(len(chann)==0,"","and channel in ('"+chann+"')")}
${if(box==false,"","group by province_code ,province_name")} 
    )a
    group by division_code,
    division_name,
    ${if(box==false,"","province_code ,province_name,")} 
    firm_id,
    firm_name
    order by 
    ${if(box==false,"","province_code, ")} division_code,firm_id;
    select * 
 from csx_dw.dws_basic_w_a_category_m where sdt='current';
 
 select
    location_uses_code,
    location_uses,
    level,
    upper_layer
from
    (
    select
        DISTINCT location_uses_code , 
        case
            when location_uses_code = '' then '未定义类型'
            else location_uses
        end location_uses, 0 level, '0' upper_layer
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and table_type = 1
union all
    select
        DISTINCT dist_code , dist_name , 1 level, location_uses_code as upper_layer
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and table_type = 1
union all
    select
       *
    from
        csx_dw.csx_shop
    where
        sdt = 'current' and location_uses  like '%合伙人%'
        and table_type = 1) a
order by
    location_uses_code,
    level ,
    upper_layer ;
    

select
    location_uses_code,
    location_uses,
    level,
    upper_layer
from
    (
    select
        DISTINCT case
            when location_uses_code = '' then '999'         
            else location_uses_code end location_uses_code , 
        case
            when location_uses_code = '' then '未定义类型'
            else location_uses
        end location_uses, 0 level, '0' upper_layer
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and table_type = 1
union all
    select
        DISTINCT dist_code , dist_name , 1 level, location_uses_code as upper_layer
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and table_type = 1
union all
    select
         location_code , shop_name , 2 level, dist_code as  upper_layer
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and table_type = 1) a where 1=1
order by
    location_uses_code,
    level ,
    upper_layer ;
    

select qdrno,diro,qdflag,dist,manage,
case when city_real like '福州%' then 1 
when city_real like '厦门%' then 2
when city_real='泉州' then 3 
when city_real='莆田' then 4
when city_real='南平' then 5
when city_real like '苏州%' then 6 
when city_real like '南京%' then 7
when city_real like '杭州%' then 8 
when city_real like '宁波%' then 9
when city_real in ('食百','生鲜') then 11 else 99 end city_no,
city_real,cityjob,
sum(cust_num)cust_num,sum(xse) xse,sum(mle) mle,
case when sum(xse)=0 then null else sum(mle)/sum(xse) end prorate,
sum(xse_lm)xse_lm,
case when sum(xse_lm)=0 then null else sum(xse)/sum(xse_lm)-1 end hb_sale 
from 
(select qdrno,coalesce(diro,9.1)diro,qdflag,dist,
manage,city_real,cityjob,
count(distinct cust_id)cust_num,sum(xse) xse,sum(mle) mle,0 xse_lm 
 from data_center_report.display_warzone02_res_dtl 
where sdt>='${SDATE}' and sdt<='${EDATE}'
group by qdrno,diro,qdflag,dist,5,city_real,7
union all 
select qdrno,coalesce(diro,9.1)diro,qdflag,dist,manage,city_real,
cityjob,
0 cust_num,0 xse,0 mle,sum(xse) xse_lm 
from data_center_report.display_warzone02_res_dtl 
where sdt>='${SBJ}' and sdt<='${EBJ}'
group by qdrno,diro,qdflag,dist,5,city_real,7
union all 
select case when cust_id not like 'S%' then 1 else 2 end qdrno,103 diro,
case when cust_id not like 'S%' then '大客户' else '商超' end qdflag,
case when qdflag='大宗' then '大宗' else 'S端' end dist,
case when qdflag='大宗' then'林恩平' else '-' end manage
,'Z-小计'city_real,'-'cityjob,count(distinct cust_id)cust_num,
sum(xse)/10000 xse,sum(mle)/10000 mle,0 xse_lm 
from data_center_report.sale_warzone01_detail_dtl 
where sdt>='${SDATE}' and sdt<='${EDATE}' 
and qdflag in ('大宗','供应链(S端)')
group by 1,3,4,5
union all 
select 
case when cust_id not like 'S%' then 1 else 2 end qdrno,103 diro,
case when cust_id not like 'S%' then '大客户' else '商超' end qdflag,
case when qdflag='大宗' then '大宗' else 'S端' end dist,
case when qdflag='大宗' then'林恩平' else '-' end manage
,'Z-小计'city_real,'-'cityjob,0 cust_num,0 xse,0 mle,sum(xse)/10000 xse_lm 
from data_center_report.sale_warzone01_detail_dtl 
where sdt>='${SBJ}' and sdt<='${EBJ}'
and qdflag in ('大宗','供应链(S端)') 
group by 1,3,4,5
union all 
select 
case when cust_id not like 'S%' then 1 else 2 end qdrno,103 diro,
case when cust_id not like 'S%' then '大客户' else '商超' end qdflag,
'S端'dist,
'-' manage,
bd_name city_real,'-'cityjob,count(distinct cust_id)cust_num
,sum(xse)/10000 xse,sum(mle)/10000 mle,0 xse_lm 
from data_center_report.sale_warzone01_detail_dtl 
where sdt>='${SDATE}' and sdt<='${EDATE}' 
and qdflag in ('供应链(S端)') and bd_name<>'其他'
group by 1,3,bd_name
union all 
select case when cust_id not like 'S%' then 1 else 2 end qdrno,103 diro,
case when cust_id not like 'S%' then '大客户' else '商超' end qdflag,
'S端'dist,
'-' manage,
bd_name city_real,'-'cityjob,0 cust_num,0 xse,0 mle,sum(xse)/10000 xse_lm 
from data_center_report.sale_warzone01_detail_dtl 
where sdt>='${SBJ}' and sdt<='${EBJ}'
and qdflag in ('供应链(S端)') and bd_name<>'其他'
group by 1,3,bd_name
)x
group by qdrno,diro,qdflag,dist,manage,city_real,cityjob
order by diro,6,qdrno;