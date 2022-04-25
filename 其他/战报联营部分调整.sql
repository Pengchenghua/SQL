drop table b2b_tmp.temp_sale;
CREATE temporary table b2b_tmp.temp_sale
as
select a.sdt,case when a.shop_id like 'E%' then '商超(对内)'
when shopid_orig='W0B6' then '企业购' 
when a.shop_id='W0H4' and a.cust_id like 'S%' then '供应链(S端)' 
when c.channel='M端' then '商超(对内)'
when c.channel like '供应链%' then '供应链(S端)' else c.channel end qdflag,

case when a.shop_id in ('W0M1','W0M4','W0J6','W0M6','W0K5')then '商超平台' 
when sales_province is not null and c.channel<>'M端' and c.channel not like '商超%' then substr(sales_province,1,2)
when a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽','广东') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end dist,fourth_supervisor_work_no region_no,fourth_supervisor_name region_manage,b.city_name,sales_city region_city,
--manage,manage_no,暂时先不添加，直接按照表格给的福州沈锋，泉州胡永水
a.cust_id,c.customer_name cust_name,bd_name,
sum(xse)xse,
sum(mle)mle
from 
(select shop_id,case when shop_id like 'E%' then concat('9',substr(shop_id,2,3)) else shop_id end shop_no,
customer_no cust_id,origin_shop_id shopid_orig,sdt,
       case when category_code in ('10','11') then '生鲜' when category_code in ('12','13') then '食百' else '其他' end bd_name
       ,sum(sales_value) xse
       ,sum(profit) mle
from  csx_dw.sale_b2b_item 
where sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-1),'-','') ,1,6),'01')
and sdt<=regexp_replace(date_sub(current_date,1),'-','')
and shop_id<>'W098'
and sales_type in('qyg','gc','anhui','sc') 
group by shop_id,
customer_no,origin_shop_id,sdt,
case when category_code in ('10','11') then '生鲜' when category_code in ('12','13') then '食百' else '其他' end)a
left join (select * from csx_dw.customer_m where sdt=regexp_replace(date_sub(current_date,1),'-','')) c 
on lpad(a.cust_id,10,'0')=lpad(c.customer_no,10,'0')
left join (select shop_id,prov_name from dim.dim_shop where edate='9999-12-31')d on a.shop_no=d.shop_id
left join
(select shop_id,case when shop_id in ('W055','W056') then '上海市' else prov_name end prov_name,
case when prov_name like '%市' then prov_name else city_name end city_name from dim.dim_shop where edate='9999-12-31' )b 
on a.cust_id=concat('S',b.shop_id)
group by a.sdt,case when a.shop_id like 'E%' then '商超(对内)'
when shopid_orig='W0B6' then '企业购' 
when a.shop_id='W0H4' and a.cust_id like 'S%' then '供应链(S端)' 
when c.channel='M端' then '商超(对内)'
when c.channel like '供应链%' then '供应链(S端)'else c.channel end,
case when a.shop_id in ('W0M1','W0M4','W0J6','W0M6','W0K5')then '商超平台' 
when sales_province is not null and c.channel<>'M端' and c.channel not like '商超%' then substr(sales_province,1,2)
when a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽','广东') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end ,fourth_supervisor_work_no,fourth_supervisor_name,b.city_name,sales_city,a.cust_id,c.customer_name,bd_name;
