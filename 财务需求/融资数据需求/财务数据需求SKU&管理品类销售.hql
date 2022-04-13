--融资数据需求 王夏塬 20211027


---月动销SKU 20211027
--省区SKU个数（B端自营）不含BBC
select case when province_name like '福建%' then 1
when province_name like '广东%' then 2
when province_name like '重庆%' then 3
when province_name like '四川%' then 4
when province_name like '贵州%' then 5
when province_name like '湖北%' then 6
when province_name like '北京%' then 7
when province_name like '河北%' then 8
when province_name like '陕西%' then 9
when province_name like '河南%' then 10
when province_name like '安徽%' then 11
when province_name like '上海%' then 12
when province_name like '江苏%' then 13
when province_name like '浙江%' then 14
else 15
end as aa,
province_name ,
mon,
    sale_sku
from ( 
select  case when province_name like '江苏%' then '江苏'
            when province_name like '上海%' then '上海'
            else province_name end province_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku
from csx_dw.dws_sale_r_d_detail where sdt>='20210601'
and channel_code in ('1','9')
and business_type_code!='4'
group by  province_name  ,
        concat(substr(sdt,1,4),'-',substr(sdt,5,2))
 
union all 
select  
    '全国'province_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku
from csx_dw.dws_sale_r_d_detail where sdt>='20210601'
and channel_code in ('1','9')
and business_type_code!='4'
group by concat(substr(sdt,1,4),'-',substr(sdt,5,2))
)a 
order by 
case when province_name like '福建%' then 1
when province_name like '广东%' then 2
when province_name like '重庆%' then 3
when province_name like '四川%' then 4
when province_name like '贵州%' then 5
when province_name like '湖北%' then 6
when province_name like '北京%' then 7
when province_name like '河北%' then 8
when province_name like '陕西%' then 9
when province_name like '河南%' then 10
when province_name like '安徽%' then 11
when province_name like '上海%' then 12
when province_name like '江苏%' then 13
when province_name like '浙江%' then 14
else 15
end ,
mon
;

--管理分类（B端自营）不含BBC

select 
    mon,
    classify_large_code,
    classify_large_name,
    sale_sku,
    no_tax_sales_value,
    no_tax_profit,
    no_tax_profit/no_tax_sales_value as no_profit_rate
from ( 
select  classify_large_code,
    classify_large_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku,
    sum(excluding_tax_sales)/10000 as no_tax_sales_value,
    sum(excluding_tax_profit)/10000 as no_tax_profit
from csx_dw.dws_sale_r_d_detail where sdt>='20210601'
and channel_code in ('1','9')
and business_type_code!='4'
group by  classify_large_code,
    classify_large_name,
        concat(substr(sdt,1,4),'-',substr(sdt,5,2))
 
union all 
select '00' classify_large_code,
    '全国'  classify_large_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku,
    sum(excluding_tax_sales)/10000 as no_tax_sales_value,
    sum(excluding_tax_profit)/10000 as no_tax_profit
from csx_dw.dws_sale_r_d_detail where sdt>='20210601'
and channel_code in ('1','9')
and business_type_code!='4'
group by concat(substr(sdt,1,4),'-',substr(sdt,5,2))
 
)a 

order by 
 
mon
;


--管理分类 BBC 销售SKU，未税销售额、毛利率

select 
    mon,
    classify_large_code,
    classify_large_name,
    sale_sku,
    no_tax_sales_value,
    no_tax_profit,
    no_tax_profit/no_tax_sales_value as no_profit_rate
from ( 
select  classify_large_code,
    classify_large_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku,
    sum(excluding_tax_sales)/10000 as no_tax_sales_value,
    sum(excluding_tax_profit)/10000 as no_tax_profit
from csx_dw.dws_sale_r_d_detail where sdt>='20210601'
and channel_code in ('7')
and business_type_code!='4'
group by  classify_large_code,
    classify_large_name,
        concat(substr(sdt,1,4),'-',substr(sdt,5,2))
 
union all 
select '00' classify_large_code,
    '全国'  classify_large_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku,
    sum(excluding_tax_sales)/10000 as no_tax_sales_value,
    sum(excluding_tax_profit)/10000 as no_tax_profit
from csx_dw.dws_sale_r_d_detail where sdt>='20210601'
and channel_code in ('7')
and business_type_code!='4'
group by concat(substr(sdt,1,4),'-',substr(sdt,5,2))
 
)a 

order by 
 
mon
;




---月动销SKU
--省区SKU个数 BBC
select case when province_name like '福建%' then 1
when province_name like '广东%' then 2
when province_name like '重庆%' then 3
when province_name like '四川%' then 4
when province_name like '贵州%' then 5
when province_name like '湖北%' then 6
when province_name like '北京%' then 7
when province_name like '河北%' then 8
when province_name like '陕西%' then 9
when province_name like '河南%' then 10
when province_name like '安徽%' then 11
when province_name like '上海%' then 12
when province_name like '江苏%' then 13
when province_name like '浙江%' then 14
else 15
end as aa,
province_name ,
mon,
    sale_sku
from ( 
select  case when province_name like '江苏%' then '江苏'
            when province_name like '上海%' then '上海'
            else province_name end province_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku
from csx_dw.dws_sale_r_d_detail where sdt>='20210601'
and channel_code in ('7')
and business_type_code!='4'
group by  province_name  ,
        concat(substr(sdt,1,4),'-',substr(sdt,5,2))
 
union all 
select  
    '全国'province_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku
from csx_dw.dws_sale_r_d_detail where sdt>='20210601'
and channel_code in ('7')
and business_type_code!='4'
group by concat(substr(sdt,1,4),'-',substr(sdt,5,2))
 
)a 

order by 
case when province_name like '福建%' then 1
when province_name like '广东%' then 2
when province_name like '重庆%' then 3
when province_name like '四川%' then 4
when province_name like '贵州%' then 5
when province_name like '湖北%' then 6
when province_name like '北京%' then 7
when province_name like '河北%' then 8
when province_name like '陕西%' then 9
when province_name like '河南%' then 10
when province_name like '安徽%' then 11
when province_name like '上海%' then 12
when province_name like '江苏%' then 13
when province_name like '浙江%' then 14
else 15
end ,
mon
;





--管理分类 BBC+B端+城市服务商 销售SKU，未税销售额、毛利率

select 
    mon,
    classify_large_code,
    classify_large_name,
    sale_sku,
    no_tax_sales_value,
    no_tax_profit,
    no_tax_profit/no_tax_sales_value as no_profit_rate
from ( 
select  classify_large_code,
    classify_large_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku,
    sum(excluding_tax_sales)/10000 as no_tax_sales_value,
    sum(excluding_tax_profit)/10000 as no_tax_profit
from csx_dw.dws_sale_r_d_detail where sdt>='20210601'
and channel_code in ('1','7','9')
-- and business_type_code!='4'
group by  classify_large_code,
    classify_large_name,
        concat(substr(sdt,1,4),'-',substr(sdt,5,2))
 
union all 
select '00' classify_large_code,
    '全国'  classify_large_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku,
    sum(excluding_tax_sales)/10000 as no_tax_sales_value,
    sum(excluding_tax_profit)/10000 as no_tax_profit
from csx_dw.dws_sale_r_d_detail where sdt>='20210601'
and channel_code in ('1','7','9')
-- and business_type_code!='4'
group by concat(substr(sdt,1,4),'-',substr(sdt,5,2))
 
)a 

order by 
 
mon
;



--省区 BBC+B端+城市服务商 销售SKU，未税销售额、毛利率

select case when province_name like '福建%' then 1
when province_name like '广东%' then 2
when province_name like '重庆%' then 3
when province_name like '四川%' then 4
when province_name like '贵州%' then 5
when province_name like '湖北%' then 6
when province_name like '北京%' then 7
when province_name like '河北%' then 8
when province_name like '陕西%' then 9
when province_name like '河南%' then 10
when province_name like '安徽%' then 11
when province_name like '上海%' then 12
when province_name like '江苏%' then 13
when province_name like '浙江%' then 14
else 15
end as aa,
province_code,province_name ,mon,
    sale_sku,
    avg_sku,
    days
from (
select province_code,province_name ,mon,
    sale_sku,
    round(sale_sku/(datediff(last_day(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd')),trunc(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd'),'MM'))+1),0) as  avg_sku,
    datediff(last_day(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd')),trunc(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd'),'MM'))+1 days
from (
select province_code,province_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku
from csx_dw.dws_sale_r_d_detail where sdt>='20190101'
and channel_code in ('1','7','9')
group by province_code,province_name,concat(substr(sdt,1,4),'-',substr(sdt,5,2))
) a 
union all 
select '00' province_code,'全国'province_name,mon,
    sale_sku,
    round(sale_sku/(datediff(last_day(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd')),trunc(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd'),'MM'))+1),0) as  avg_sku,
    datediff(last_day(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd')),trunc(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd'),'MM'))+1 days
from (
select 
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku
from csx_dw.dws_sale_r_d_detail where sdt>='20190101'
and channel_code in ('1','7','9')
group by concat(substr(sdt,1,4),'-',substr(sdt,5,2))
) a 
)a 

order by 

case when province_name like '福建%' then 1
when province_name like '广东%' then 2
when province_name like '重庆%' then 3
when province_name like '四川%' then 4
when province_name like '贵州%' then 5
when province_name like '湖北%' then 6
when province_name like '北京%' then 7
when province_name like '河北%' then 8
when province_name like '陕西%' then 9
when province_name like '河南%' then 10
when province_name like '安徽%' then 11
when province_name like '上海%' then 12
when province_name like '江苏%' then 13
when province_name like '浙江%' then 14
else 15
end ,
mon
;



select 
  mon,province_code,province_name,classify_large_code,classify_large_name ,
   no_tax_sale/10000 as sale,
    no_tax_profit/10000 as profit,
    no_tax_profit/no_tax_sale as no_tax_profit_rate,
    sale_sku,
    avg_sku,
    days
from (
select province_code,province_name,'00' classify_large_code,'全国'classify_large_name,mon,
    no_tax_sale,
    no_tax_profit,
    no_tax_profit/no_tax_sale as no_tax_profit_rate,
    sale_sku,
    round(sale_sku/(datediff(last_day(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd')),trunc(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd'),'MM'))+1),0) as  avg_sku,
    datediff(last_day(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd')),trunc(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd'),'MM'))+1 days
from (
select province_code,province_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku,
    sum(excluding_tax_sales) as no_tax_sale,
    sum(excluding_tax_profit) as no_tax_profit
from csx_dw.dws_sale_r_d_detail a
left join
(select goods_id,classify_large_code,classify_large_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
where sdt>='20190101'
and channel_code in ('1','9','7')
group by concat(substr(sdt,1,4),'-',substr(sdt,5,2)),province_code,province_name
) a
union all 
select province_code,province_name,classify_large_code,classify_large_name ,mon,
    no_tax_sale,
    no_tax_profit,
    no_tax_profit/no_tax_sale as no_tax_profit_rate,
    sale_sku,
    round(sale_sku/(datediff(last_day(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd')),trunc(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd'),'MM'))+1),0) as  avg_sku,
    datediff(last_day(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd')),trunc(from_unixtime(unix_timestamp(concat(mon,'-01'),'yyyy-MM-dd'),'yyyy-MM-dd'),'MM'))+1 days
    
from (
select province_code,province_name,b.classify_large_code,b.classify_large_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku,
    sum(excluding_tax_sales) as no_tax_sale,
    sum(a.excluding_tax_profit) as no_tax_profit
from csx_dw.dws_sale_r_d_detail a 
left join
(select goods_id,classify_large_code,classify_large_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
where sdt>='20190101'
and channel_code in ('1','9','7')
group by classify_large_code,classify_large_name,concat(substr(sdt,1,4),'-',substr(sdt,5,2)),province_code,province_name
) a 

)a 
where province_code in ('15','32','11')
order by 
province_code,
classify_large_code,
mon
;


---日均动销SKU

select case when province_name like '福建%' then 1
when province_name like '广东%' then 2
when province_name like '重庆%' then 3
when province_name like '四川%' then 4
when province_name like '贵州%' then 5
when province_name like '湖北%' then 6
when province_name like '北京%' then 7
when province_name like '河北%' then 8
when province_name like '陕西%' then 9
when province_name like '河南%' then 10
when province_name like '安徽%' then 11
when province_name like '上海%' then 12
when province_name like '江苏%' then 13
when province_name like '浙江%' then 14
else 15
end as aa,
province_code,province_name ,mon,
    sale_sku
from (
select province_code,province_name ,mon,
   avg(sale_sku)sale_sku
from (
select province_code,province_name,
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    sdt,
    count(distinct goods_code) as sale_sku
from csx_dw.dws_sale_r_d_detail where sdt>='20190101'
and channel_code in ('1','7','9')
group by province_code,province_name,concat(substr(sdt,1,4),'-',substr(sdt,5,2)),sdt
) a 
group by province_code,province_name ,mon
union all 
select '00' province_code,
    '全国'province_name,
    mon,
    avg(sale_sku)sale_sku
   
from (
select 
    concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    sdt,
    count(distinct goods_code) as sale_sku
from csx_dw.dws_sale_r_d_detail where sdt>='20190101'
and channel_code in ('1','7','9')
group by concat(substr(sdt,1,4),'-',substr(sdt,5,2)),sdt
) a 
group by mon
)a 

order by 

case when province_name like '福建%' then 1
when province_name like '广东%' then 2
when province_name like '重庆%' then 3
when province_name like '四川%' then 4
when province_name like '贵州%' then 5
when province_name like '湖北%' then 6
when province_name like '北京%' then 7
when province_name like '河北%' then 8
when province_name like '陕西%' then 9
when province_name like '河南%' then 10
when province_name like '安徽%' then 11
when province_name like '上海%' then 12
when province_name like '江苏%' then 13
when province_name like '浙江%' then 14
else 15
end ,
mon
;



