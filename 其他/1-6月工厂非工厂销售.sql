
-- 1-6月工厂非工厂销售
 select substr(a.sdt,1,6)mon, province_code ,province_name,
    sum(case when is_factory_goods_code =1 then sales_value end )as factory_sale,
    sum(case when is_factory_goods_code=0 then sales_value end )as no_sale,
    sum(sales_value )sales_all,
    sum(case when is_factory_goods_code =1 then profit end )as factory_profit,
    sum(case when is_factory_goods_code=0 then profit end )as no_profit,
    sum(profit )profit_all
 from csx_dw.dws_sale_r_d_customer_sale a
 left join 
 (select distinct sdt,customer_no from csx_dw.csx_partner_list where sdt>='202001') b on substr(a.sdt,1,6)=b.sdt and a.customer_no =b.customer_no
 where a.sdt>='20200101' and a.sdt<'20200701'
    and b.customer_no is null
    and channel ='1'
 group by  substr(a.sdt,1,6),
    province_code ,
    province_name;
	

 select substr(a.sdt,1,6)mon,  
    sum(case when is_factory_goods_code =1 then sales_value/10000 end )as factory_sale,
    sum(case when is_factory_goods_code=0 then sales_value/10000 end )as no_sale,
    sum(sales_value/10000 )sales_all,
    sum(case when is_factory_goods_code =1 then profit/10000 end )as factory_profit,
    sum(case when is_factory_goods_code=0 then profit/10000 end )as no_profit,
    sum(profit/10000 )profit_all
 from csx_dw.dws_sale_r_d_customer_sale a
 left join 
 (select distinct sdt,customer_no from csx_dw.csx_partner_list where sdt='202006') b on substr(a.sdt,1,6)=b.sdt and a.customer_no =b.customer_no
 where a.sdt>='20200601' and a.sdt<'20200701'
  and b.customer_no is null
    and channel ='1'
 group by  substr(a.sdt,1,6);