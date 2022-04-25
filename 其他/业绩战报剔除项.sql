	-------业绩战报剔除：贸易客户，订单<0.015
select 
     province_code, --省区
     case when channel in ('4')  then '大宗'
	      when channel in ('5','6') then '供应链'
		  else a.province_name end  province_name,
	 city_group_name,
     substr(sdt,1,6) smonth,
     case
        when channel=2 then 'M端'
        when channel=7 then 'BBC'
		when (customer_name like '%内购%' or customer_name like '%内%购%' or customer_name like '%临保%') 
		  or (channel in ('1','9') and attribute='贸易客户' and profit_rate<=0.015 ) then '批发内购'
		when channel in ('1','9') and attribute='贸易客户' and profit_rate>0.015 then '省区大宗'
        when channel in ('1','9') and attribute='合伙人客户' then '城市服务商'
        when channel in ('1','9')  and order_kind='WELFARE' then '福利单'
		when channel in ('4','5','6')  then '大宗&供应链'
       else  '日配单'
     end as sale_group --订单类型：NORMAL-普通单，WELFARE-福利单
	,sum(sales_value) sales_value
	,sum(profit) profit
	,sum(profit)/sum(sales_value) profit_rate
   from (
		  select 
		    province_code,
			channel,
			province_name,
		    city_group_name,
		    sdt,
			a.customer_no,
			customer_name,
			f.attribute,
			order_kind,
			order_no		
            ,sum(sales_value) sales_value
	        ,sum(profit) profit
	        ,sum(profit)/sum(sales_value) profit_rate
		  from ( 
                 select * from csx_dw.dws_sale_r_d_customer_sale
		         where sdt>= '20201201' and sdt<'20201203' 
				 and order_no not in ('OC20111000000022','OC20111000000023','OC20111000000021','OC20111000000024','OC20111000000025')
		         and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
				)a 
		   left join (select customer_no,attribute
						from csx_dw.dws_crm_w_a_customer_m_v1
						where sdt=regexp_replace(date_sub(current_date,1),'-','') ) f ON a.customer_no=f.customer_no
		group by province_code,
			channel,
			province_name,
		    city_group_name,
		    sdt,
			a.customer_no,
			customer_name,
			f.attribute,
			order_kind,
			order_no
		)a
   group by province_code,
   case when channel in ('4')  then '大宗'
	      when channel in ('5','6') then '供应链'
		  else a.province_name end,
	 city_group_name,
     substr(sdt,1,6),
            case
        when channel=2 then 'M端'
        when channel=7 then 'BBC'
		when (customer_name like '%内购%' or customer_name like '%内%购%' or customer_name like '%临保%') 
		  or (channel in ('1','9') and attribute='贸易客户' and profit_rate<=0.015 ) then '批发内购'
		when channel in ('1','9') and attribute='贸易客户' and profit_rate>0.015 then '省区大宗'
        when channel in ('1','9') and attribute='合伙人客户' then '城市服务商'
        when channel in ('1','9')  and order_kind='WELFARE' then '福利单'
		when channel in ('4','5','6')  then '大宗&供应链'
       ELSE  '日配单'
     end