select * from csx_dw.dws_basic_w_a_csx_product_info  a 
join 
(select * from csx_dw.dws_basic_w_a_manage_classify_m where sdt='current' and classify_middle_name like '冻品%' and classify_small_code not in ('B030401','B030404'))b on a.small_category_code =b.category_small_code
where a.sdt='current'
and a.des_specific_product_status not in ('7','6')
and a.shop_code in ('W0A3','W0Q9','W0P8','W0A8','W0F4','W0K1','W0K6','W0L3','W0A6','W0A7','W0Q2','W0N1','W0A5','W0R9','W0N0','W0W7','W0A2','W0D4','W080','W0T5','W048','W0R1','W053','W0E7','W0T3','W0Q1','W0S9','W0Q8','W088','W0R7','W0R8','W0S2','W0P6','W0K3','W079','W0M6','W0P3','W0W8','W039','W0T7','W0X1')
and (product_name like '冰%' or product_name like  '冻%' or product_name like '牛%子%' or product_name like '%一号肥牛卷%' or product_name like '%鸡腿肉%'  or product_name like '%肥牛卷%' or product_name like '%长鸡爪%')
