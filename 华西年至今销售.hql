       
       
SELECT substr(sdt,1,6)mon,
       region_code,
       region_name,
       province_code,
       province_name,
       city_group_code,
       city_group_name,
       case  when customer_no in ('103097', '103903','104842') then '红旗/中百'
            when   a.channel_code ='2' and a.customer_no like 'S%' then  sales_belong_flag else business_type_name end business_type_name,
       sum(sales_value)sale,
       sum(sales_cost)cost
FROM csx_dw.dws_sale_r_d_detail a 
LEFT JOIN 
(select shop_id,sales_belong_flag from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.customer_no=concat('S',b.shop_id)
WHERE sdt>='20210101'
  AND region_code='3'
  
  GROUP BY substr(sdt,1,6),
       region_code,
       region_name,
       province_code,
       province_name,
       city_group_code,
       city_group_name,
      business_type_name