-- 波总需求（行业、企业属性新）
-- 按照新行业、企业属性查询21年合同、履约、成交
SELECT 
       sales_province_code,
       sales_province_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
        second_category_new ,
        customer_cn,
        estimate_contract_amount,
        cust_cn,
        estimate_amount,
        sale_cn,
        all_sale,
        daily_cust_cn,
        daily_sale,
        fuli_cust_cn,
        fuli_sale,
        pf_cust_cn,
        pf_sale,
        cs_cust_cn,
        cs_sale,
        sqdz_cust_cn,
        sqdz_sale,
        bbc_cust_cn,
        bbc_sale
    from 
(SELECT 
       sales_province_code,
       sales_province_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       case when second_category_name in ('事业单位','政府机关') then '政府/事业单位' 
         when second_category_name in ('部队','监狱') then '部队/监狱'
         when second_category_name in ('电力燃气水供应','金融业') then '电力/金融'
         when second_category_name in ('教育','医疗卫生') then '教育/医疗'
       else '制造业/其他'
       end as second_category_new ,
       count(a.customer_no) as customer_cn,
       sum(estimate_contract_amount) estimate_contract_amount,
       count(case when  substr(to_date(sign_time),1,4)='2021' then a.customer_no end) as cust_cn,
       sum(case when  substr(to_date(sign_time),1,4)='2021' then estimate_contract_amount end) as estimate_amount,
       count(case when  substr(to_date(sign_time),1,4)='2021'then b.customer_no end ) sale_cn,
       sum(case when  substr(to_date(sign_time),1,4)='2021' then sales end) all_sale,
       count(case when  substr(to_date(sign_time),1,4)='2021'and coalesce(daily_sale,0)!=0  then daily_sale end) as daily_cust_cn,
       sum(case when  substr(to_date(sign_time),1,4)='2021' then daily_sale end)daily_sale,
       count(case when  substr(to_date(sign_time),1,4)='2021'and coalesce(fuli_sale,0)!=0  then fuli_sale end) as fuli_cust_cn,
       sum(case when  substr(to_date(sign_time),1,4)='2021' then fuli_sale  end)fuli_sale,
       count(case when  substr(to_date(sign_time),1,4)='2021' and coalesce(pf_sale,0)!=0  then pf_sale end) as pf_cust_cn,
       sum(case when  substr(to_date(sign_time),1,4)='2021' then pf_sale    end)pf_sale,
       count(case when  substr(to_date(sign_time),1,4)='2021'and coalesce(cs_sale,0)!=0  then cs_sale end) as cs_cust_cn,
       sum(case when  substr(to_date(sign_time),1,4)='2021' then cs_sale    end)cs_sale,
       count(case when  substr(to_date(sign_time),1,4)='2021' and coalesce(sqdz_sale,0)!=0 then sqdz_sale end) as sqdz_cust_cn,
       sum(case when  substr(to_date(sign_time),1,4)='2021' then sqdz_sale  end)sqdz_sale,
       count(case when  substr(to_date(sign_time),1,4)='2021' and coalesce(bbc_sale,0)!=0 then bbc_sale end) as bbc_cust_cn,
       sum(case when  substr(to_date(sign_time),1,4)='2021' then bbc_sale   end)bbc_sale
FROM csx_dw.dws_crm_w_a_customer a 
LEFT JOIN
--   `business_type_code` string COMMENT '业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)', 
(select customer_no,sum(sales_value/10000) sales,
sum(case when business_type_code='1' then customer_no end ) daily_cust_cn,
sum(case when business_type_code='1' then sales_value/10000 end ) daily_sale,
sum(case when business_type_code='2' then customer_no end ) fuli_cust_cn,
sum(case when business_type_code='2' then sales_value/10000 end ) fuli_sale,
sum(case when business_type_code='3' then customer_no end ) pf_cust_cn,
sum(case when business_type_code='3' then sales_value/10000 end ) pf_sale,
sum(case when business_type_code='4' then customer_no end ) cs_cust_cn,
sum(case when business_type_code='4' then sales_value/10000 end ) cs_sale,
sum(case when business_type_code='5' then customer_no end ) sqdz_cust_cn,
sum(case when business_type_code='5' then sales_value/10000 end ) sqdz_sale,
sum(case when business_type_code='6' then customer_no end ) bbc_cust_cn,
sum(case when business_type_code='6' then sales_value/10000 end ) bbc_sale
from csx_dw.dws_sale_r_d_detail 
where sdt>='20210101' and sdt<'20220101' 
GROUP BY customer_no) b on a.customer_no=b.customer_no
WHERE sdt='current'
  AND a.customer_no!=''
  and a.channel_code in ('1','7','9')
  GROUP BY
   first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       channel_code,
        channel_name,
       sales_province_code,
        case when second_category_name in ('事业单位','政府机关') then '政府/事业单位' 
         when second_category_name in ('部队','监狱') then '部队/监狱'
         when second_category_name in ('电力燃气水供应','金融业') then '电力/金融'
         when second_category_name in ('教育','医疗卫生') then '教育/医疗'
         else '制造业/其他'
         end ,
       sales_province_name
       )a 
