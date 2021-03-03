--客户编码，客户名称，日期，退货金额，退货毛利.5月份的.等下有空，帮我这个大客户的刷一下.

select sdt,a.customer_no,customer_name,sales_user_name,region_province_name,sum(a.tax_sale_val)sale,sum(a.tax_profit)profit from csx_dw.big_customer_order a
join
(select customer_name,customer_number,region_province_name,sales_user_name from csx_dw.customer_simple_info_v2 where sdt='20190623') b
on a.customer_no=b.customer_number
and a.sdt>='20190501' and a.sdt<='20190531' and a.bill_type in('ZR4','ZR3','ZR2','ZR1')
group by sdt,a.customer_no,customer_name,sales_user_name,region_province_name;

select sdt,a.customer_no,customer_name,sales_user_name,region_province_name,a.material,goodsname,sum(a.tax_sale_val)sale,sum(a.tax_profit)profit from csx_dw.big_customer_order a
join
(select customer_name,customer_number,region_province_name,sales_user_name from csx_dw.customer_simple_info_v2 where sdt='20190623') b
on a.customer_no=b.customer_number
join
(select goodsid,goodsname from dim.dim_goods where edate='9999-12-31')c
on regexp_replace(a.material,'(^0*)','')=c.goodsid

and a.sdt>='20190501' and a.sdt<='20190531' and a.bill_type in('ZR4','ZR3','ZR2','ZR1')
group by sdt,a.customer_no,customer_name,sales_user_name,region_province_name,goodsname,material;


select * from csx_dw.big_customer_order limit 1
