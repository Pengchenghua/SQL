select sales_province_code,
    sales_province_name,
    customer_no,
    customer_name,
    sign_time,
    estimate_contract_amount,
    attribute_name,
    attribute_desc,
    b.new_classify_name
from csx_dw.dws_crm_w_a_customer a
    left join -- 新企业属性分类
    csx_tmp.new_customer_classify b on a.second_category_code = b.second_category
where sdt = 'current'
    and sign_time between '2021-01-01 00:00:00' and '2021-01-31 23:00:00'
    and sales_province_code in ('1', '6', '26');