
select * from csx_dw.customer_sale_m where sdt>='20200301' and goods_code='1218022';

select * from csx_dw.supple_goods_sale_dtl where sdt>='20200301' and goods_code='1218022'and dc_code='W0B6';

select SUBSTRING(sdt,1,6) mon,province_code,province_name,first_category,second_category,sum(sales_value)/10000 sale,sum(profit)/10000 profit from csx_dw.customer_sale_m where sdt>='20200101'
and province_name like '%福建%' and channel in ('1','3','7')
group by SUBSTRING(sdt,1,6) ,province_code,first_category,province_name,second_category
;
select SUBSTRING(sdt,1,6) mon,province_code,province_name,first_category,second_category,sum(sales_value)/10000 sale,sum(profit)/10000 profit from csx_dw.customer_sale_m where sdt>='20200101'
and channel in ('1','3','7')
group by SUBSTRING(sdt,1,6) ,province_code,first_category,province_name,second_category;