
select *
from
    (select region_name,
        coalesce(province_name,'合计') as province_name,
        coalesce(city_group_name,'合计') as city_group_name,
        coalesce(channel_name_1,'合计') as channel_name_1,
        if(sale_group is null,'合计',third_supervisor_name) as third_supervisor_name,
        if(sale_group is null,'合计',first_supervisor_name) as first_supervisor_name,
        coalesce(sale_group,'合计') sale_group,
        old_Md_sales_value,
        old_M_sales_value,
        old_M_profit,
        old_H_sales_value,
        old_M_profit/old_M_sales_value as old_M_prorate,
        (old_M_sales_value/old_H_sales_value-1) as old_H_sale_rate,
        new_cust_count,
        new_Md_sales_value,
        new_M_sales_value,
        new_M_profit,
        new_H_sales_value,
        new_M_profit/new_M_sales_value as new_M_prorate,
        (new_M_sales_value/new_H_sales_value-1) as new_H_sale_rate,
        ALL_Md_sales_value,
        ALL_M_sales_value,
        ALL_M_profit,
        ALL_H_sales_value,
        ALL_M_profit/ALL_M_sales_value as ALL_M_prorate,
        (ALL_M_sales_value/ALL_H_sales_value-1) as ALL_H_sale_rate,
        case when city_group_name='-' and channel_name_1 is null then '是' else '否' end is_delete,
        GROUPING__ID 
    from
        (select region_name,
            province_name,
            city_group_name,
            channel_name_1,
            third_supervisor_name,
            first_supervisor_name,
            sale_group,
        sum(case when smonth='本月' and is_new_sale='否' then Md_sales_value end)/10000 as old_Md_sales_value, --老客-昨日销售额
        sum(case when smonth='本月' and is_new_sale='否' then sales_value end)/10000 as old_M_sales_value,  --老客-累计销售额
        sum(case when smonth='本月' and is_new_sale='否' then profit end)/10000 as old_M_profit,  --老客-累计毛利额
        sum(case when smonth='环比月' and is_new_sale='否' then sales_value end)/10000 as old_H_sales_value,  --老客-环比累计销售额
        count(distinct case when smonth='本月' and is_new_sale='是' then customer_no end)as new_cust_count,  --新客-累计客户数
        sum(case when smonth='本月' and is_new_sale='是' then Md_sales_value end)/10000 as new_Md_sales_value, --新客-昨日销售额
        sum(case when smonth='本月' and is_new_sale='是' then sales_value end)/10000 as new_M_sales_value,  --新客-累计销售额
        sum(case when smonth='本月' and is_new_sale='是' then profit end)/10000 as new_M_profit,  --新客-累计毛利额
        sum(case when smonth='环比月' and is_new_sale='是' then sales_value end)/10000 as new_H_sales_value,  --新客-环比累计销售额
        sum(case when smonth='本月' then Md_sales_value end)/10000 as ALL_Md_sales_value, --汇总-昨日销售额
        sum(case when smonth='本月' then sales_value end)/10000 as ALL_M_sales_value,  --汇总-累计销售额
        sum(case when smonth='本月' then profit end)/10000 as ALL_M_profit,  --汇总-累计毛利额
        sum(case when smonth='环比月' then sales_value end)/10000 as ALL_H_sales_value,  --汇总-环比累计销售额
        GROUPING__ID 
        from (select *,
                case when channel_name='商超' then 'M端'
                     when channel_name='大客户' or channel_name like '企业购%' then 'B端'
                     else '其他' end channel_name_1
                from csx_tmp.tmp_supervisor_day_detail)a
        group by region_name,
                 province_name,
                 city_group_name,
                 channel_name_1,
                 third_supervisor_name,
                 first_supervisor_name,
                 sale_group
        grouping sets((region_name),
                      (region_name,province_name),
                      (region_name,province_name,city_group_name),
                      (region_name,province_name,city_group_name,channel_name_1),
                      (region_name,province_name,city_group_name,channel_name_1,third_supervisor_name,first_supervisor_name,sale_group))
        )a
    )a  
where is_delete='否'
order by GROUPING__ID 
;

select province_code ,
    province_name,
    channel_name ,
    supervisor_name ,
    sales_name ,
    customer_no ,
    customer_name ,
    sum(sales_value)sales_value ,
    sum(profit)profit 
from csx_dw.dws_sale_r_d_customer_sale 
where sdt>='20200901' 
and sdt<='20200930' 
and province_code in ('32','23','24')
and channel in('1','7')
group by 
province_code ,
    province_name,
    supervisor_name ,
    customer_no ,
    customer_name ,
    channel_name,
    sales_name ;
    

refresh csx_dw.provinces_kanban_sales_top10;

select * from csx_dw.account_age_dtl_fct_new where sdt='20201003' and kunnr ='0000105165';
select * from ods_ecc.ecc_ytbcustomer where sdt='20201004' and kunnr ='0000111612';
select * from csx_dw.crm_credit_rating where customer_no ='111612' and sdt='20201003';



select customer_no,customer_name,sales_name,credit_limit,ac_all,overdue_account,credit_limit-unuse_ac as unuse_ac,full_comp
from (
select customer_no,
concat(customer_no,'_',customer_name)customer_name,
sales_name,
concat(comp_code,'_',comp_name)as full_comp,
round(cast(credit_limit as decimal(26,0)),0)credit_limit,
round(sum(ac_all),0)ac_all,round(sum(overdue_account),0)overdue_account,
--round(use_ac_all,0)use_ac_all,
round(sum(ac_all),0) unuse_ac
from csx_dw.crm_credit_rating
where sdt=regexp_replace(to_date('${edate}'),'-','')
and customer_no='${cust}'
and (case when sales_province_code in ('35','36') then '35' else sales_province_code end) in ('${prov}')
and comp_code='${comp}'
group by customer_no,customer_name,
sales_name,credit_limit,concat(comp_code,'_',comp_name)
)a where 1=1 ;

select * from csx_tmp.ads_fr_account_receivables  where customer_no ='105165' and sdt='20201003';
select * from csx_dw.dws_crm_r_a_customer_account_day where sdt='20201003' and payment_terms ='Y003';
