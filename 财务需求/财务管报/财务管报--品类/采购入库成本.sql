
-- 采购销售入库成本
drop table  csx_tmp.temp_frozen_purch_amt;
create table csx_tmp.temp_frozen_purch_amt as 
select 
    a.credential_no,
    channel_code,
    channel_name,
    business_type_code,
    business_type_name,
    a.goods_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    origin_order_no, 
    order_no, 
    dc_code,
    tax_rate,
    sales_cost,
    sales_qty,
    sales_value,
    profit,
    excluding_tax_cost,
    excluding_tax_profit,
    excluding_tax_sales,
    coalesce(purchase_qty,0 )purchase_qty,
    coalesce(purchase_amt,0)purchase_amt,
    coalesce(no_tax_purchase_amt,0 )no_tax_purchase_amt,
    coalesce(return_qty,0)return_qty,
    coalesce(return_amt,0)return_amt,
    coalesce(no_tax_return_amt,0)no_tax_return_amt
from 
 (select * from
    csx_tmp.temp_fina_sale_00
   -- where   channel_code in ('1','7','9')
) a 
  left join 
(
  select
    goods_code,
    credential_no,
    sum(case when move_type='107A' then qty end ) as purchase_qty,
    sum(case when move_type='107A' then price_no_tax*qty end ) as no_tax_purchase_amt,
    sum(case when move_type='107A' then price *qty end ) as purchase_amt,
    sum(case when move_type='108A' then qty end ) as return_qty,
    sum(case when move_type='108A' then price_no_tax *qty end ) as no_tax_return_amt,
    sum(case when move_type='108A' then price *qty end ) as return_amt
  from csx_dw.dws_wms_r_d_batch_detail
  where move_type in ('107A','108A')
    and source_order_type_code !='KN'
  group by goods_code, credential_no
) b on a.credential_no = b.credential_no and a.goods_code = b.goods_code

;
