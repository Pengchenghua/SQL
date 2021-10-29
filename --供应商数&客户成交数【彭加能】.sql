  --供应商数&客户成交数【彭加能】
  --剔除云超配送、合伙人仓、联营仓
  select province_code,province_name,count(distinct supplier_code)  from csx_tmp.temp_order_entry where mon>='202106'
  group by province_code,province_name;
  
  

-- B端自营剔除城市服务商
  select province_code,province_name,count(distinct customer_no)  
  from csx_dw.dws_sale_r_d_detail
  where sdt>='20210601' and sdt<='20211028'
    and channel_code in ('1','7','9')
    AND business_type_code !='4'
  group by province_code,province_name;