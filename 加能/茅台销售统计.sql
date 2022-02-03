
-- 茅台销售统计
select province_code,
    province_name,
    city_group_code,
    city_group_name,
    goods_code,
    goods_name,
    customer_no,
    customer_name,
    sum(sales_cost)/sum(sales_qty) as cost,
    sum(sales_value)/sum(sales_qty) as price,
    sum(sales_cost) sales_cost,
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(profit) profit
from csx_dw.dws_sale_r_d_detail
where sdt>='20220101' 
    and goods_code='8708' 
group by  province_code,
    province_name,
    city_group_code,
    city_group_name,
    goods_code,
    customer_no,
    customer_name,
    goods_name
    order by 
    (case when city_group_name='福州市' then 1 
          when city_group_name='厦门市' then 2 
          when city_group_name='泉州市' then 3 
          when city_group_name='莆田市' then 4 
          when city_group_name='南平市' then 5 
          when city_group_name='三明市' then 6 
          when city_group_name='宁德市' then 7 
          when city_group_name='龙岩市' then 8 

          when city_group_name='深圳市' then 9 

          when city_group_name='北京市' then 10 
          when city_group_name='吉林市' then 11 
          when city_group_name='哈尔滨市' then 12 
          when city_group_name='天津市' then 13 

          when city_group_name='石家庄市' then 14 

          when city_group_name='西安市' then 15 

          when city_group_name='重庆主城' then 16 
          when city_group_name='万州区' then 17 
          when city_group_name='黔江区' then 18 

          when city_group_name='成都市' then 19 

          when city_group_name='贵阳市' then 20 

          when city_group_name='宝山区' then 21 
          when city_group_name='松江区' then 22 
          when city_group_name='南京市' then 23 
          when city_group_name='苏州市' then 24  

          when city_group_name='杭州市' then 25  
          when city_group_name='宁波市' then 26  
          when city_group_name='舟山市' then 27  
          when city_group_name='台州市' then 28  

          when city_group_name='合肥市' then 29  

          when city_group_name='新乡市' then 30  
          when city_group_name='郑州市' then 30 

          when city_group_name='武汉市' then 31 

          end 
          )
;


select distinct  dist_code ,dist_name 
from csx_dw.csx_shop  
where sdt='current' 
 and dist_code !=''
order by dist_code