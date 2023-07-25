--底层对应关系M端有100多家未提供关系，这部分数据如果门店所属以下6区用门店所属区，若不是用发出门店所属区
('重庆区','四川区','北京区','福建区','上海区','浙江区','江苏区','安徽区') 
--有两个B端未提供省区对应关系，使用发出门店所在省区进行调整；
drop table b2b_tmp.temp_sale;
CREATE temporary table b2b_tmp.temp_sale
as
select case when shopid_orig='W0B6' then 'BBC' when source_id_new='22' then 'M端' else 'B端' end qdflag,
case when c.dist is not null then substr(dist,1,2)
when c.dist is null and a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end dist,region_no,region_manage,b.city_name,region_city,
--manage,manage_no,暂时先不添加，直接按照表格给的福州沈锋，泉州胡永水
count(distinct a.cust_id)cust_num,
sum(xsl)xsl,sum(xse)xse,
sum(mle)mle,sum(wsxse)wsxse,sum(wsmle)wsmle
from 
(select shop_id,cust_id,shopid_orig,
       sum(qty)xsl
       ,sum(tax_salevalue) xse
       ,sum(tax_profit) mle
       ,sum(untax_salevalue) wsxse
       ,sum(untax_profit) wsmle
from  csx_ods.sale_b2b_dtl_fct	 
where sdt>='${SDATE}'
and sdt<='${EDATE}'
and sflag in('qyg','qyg_c','gc') and shop_id<>'W098'
group by shop_id,cust_id,shopid_orig)a
join csx_ods.b2b_customer_new c on lpad(a.cust_id,10,'0')=lpad(c.cust_id,10,'0')
left join (select shop_id,prov_name from dim.dim_shop where edate='9999-12-31')d on a.shop_id=d.shop_id
left join
(select shop_id,prov_name,case when prov_name like '%市' then prov_name else city_name end city_name from dim.dim_shop where edate='9999-12-31' )b 
on a.cust_id=concat('S',b.shop_id)
where (a.xse<>0 or a.xsl<>0 or a.mle<>0)
group by case when shopid_orig='W0B6' then 'BBC' when source_id_new='22' then 'M端' else 'B端' end,
 case when c.dist is not null then substr(dist,1,2)
when c.dist is null and a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end,region_no,region_manage,b.city_name,region_city;

--福建省的要划分到市，其余省份无该需求
drop table b2b_tmp.temp_sale01;
CREATE temporary table b2b_tmp.temp_sale01
as
select qdflag,dist,region_no,region_manage,case when qdflag='M端' and dist ='福建' then city_name 
when qdflag='B端' and dist ='福建' then region_city else '-' end region_city,
sum(cust_num)cust_num,
sum(xsl)xsl,sum(xse)xse,
sum(mle)mle,sum(wsxse)wsxse,sum(wsmle)wsmle
from b2b_tmp.temp_sale
group by qdflag,dist,region_no,region_manage,case when qdflag='M端' and dist ='福建' then city_name 
when qdflag='B端' and dist ='福建' then region_city else '-' end;


