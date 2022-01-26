-- 酒水销售关联回款
select  province_code,
    province_name,
    sales_name,
    customer_no,
    customer_name,
    order_no,
    qty,
    sales,
    profit,
    profit_rate,
    payment_amount
from
(select province_code,
    province_name,
    sales_name,
    order_no,
    customer_no,
    customer_name,
    sum(sales_qty) qty,
    sum(sales_value)/10000 sales,
    sum(profit)/10000 profit,
    sum(profit)/sum(sales_value) as profit_rate
from csx_dw.dws_sale_r_d_detail
where sdt>='20211001' 
    and sdt<'20220101'
    and goods_code in ( '8649','8708','8718','800682','909970','1017653','1316198',
                    '1316196','1128274','232454','1288902','13532','6939',
                    '13523','478192')
group by 
 province_code,
 province_name, 
 order_no,
 sales_name,
 customer_no,
 customer_name
 )a 
 left join
 (select source_bill_no,
    payment_amount,
    customer_code
 from csx_dw.dws_sss_r_d_order_kp_settle_detail
    where sdt='20220125' 
        and overdue_date_new<'2022-01-01'
    --     and source_bill_no='OM21102000011520' 
    ) j on a.order_no=j.source_bill_no and a.customer_no=j.customer_code
;
