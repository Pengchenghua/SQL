
SET hive.execution.engine=spark; 
--全国渠道省区名称
drop table csx_tmp.temp_top_goods;
create temporary table csx_tmp.temp_top_goods as 
SELECT  
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sales_cost,
       sales_value,
       profit,
       sales_qty,
       row_num
from (
SELECT  
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sales_cost,
       sales_value,
       profit,
       sales_qty,
       row_number()over( order by sales_value desc) as row_num
from 
(SELECT 
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sum(sales_cost) as sales_cost,
       sum(sales_value) as sales_value,
       sum(profit) as profit,
       sum(sales_qty) as sales_qty
FROM csx_dw.dws_sale_r_d_detail
WHERE sdt>='20200101'
  AND sdt<'20210101'
  AND (classify_large_code IN ('B04','B05','B06','B07','B08') or classify_middle_code ='B0102') 
group by 
      
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name
)a
)a
where row_num<101
;



--渠道省区名称
insert overwrite directory '/tmp/pengchenghua/top/111' row format delimited fields terminated by '\t'
SELECT mon,channel_name,
       province_code,
       province_name,
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sales_cost/sales_qty as cost,
       sales_value/sales_qty as price,
       sales_cost,
       sales_value,
       profit,
       sales_qty,
       row_number()over(PARTITION BY channel_name,province_code,mon order by sales_value desc) as row_num
from 
(SELECT substr(sdt,1,6)mon,
        channel_name,
       province_code,
       province_name,
       a.goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sum(sales_cost) as sales_cost,
       sum(sales_value) as sales_value,
       sum(profit) as profit,
       sum(sales_qty) as sales_qty
FROM csx_dw.dws_sale_r_d_detail a 
join 
(select goods_code from csx_tmp.temp_top_goods)b  on a.goods_code=b.goods_code
WHERE sdt>='20200101'
  AND sdt<'20210101'
  AND (classify_large_code IN ('B04','B05','B06','B07','B08')
       OR classify_middle_code ='B0102') 
group by 
      channel_name,
       province_code,
       province_name,
       a.goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       substr(sdt,1,6)
)a
;




--全国渠道省区名称
insert overwrite directory '/tmp/pengchenghua/top/112' row format delimited fields terminated by '\t'
SELECT mon,
        channel_name,
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sales_cost,
       sales_value,
       profit,
       sales_qty,
       row_num
from (
SELECT mon,channel_name,
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sales_cost,
       sales_value,
       profit,
       sales_qty,
       row_number()over(PARTITION BY channel_name,mon order by sales_value desc) as row_num
from 
(SELECT substr(sdt,1,6)mon,
        channel_name,
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sum(sales_cost) as sales_cost,
       sum(sales_value) as sales_value,
       sum(profit) as profit,
       sum(sales_qty) as sales_qty
FROM csx_dw.dws_sale_r_d_detail
WHERE sdt>='20200101'
  AND sdt<'20210101'
  AND (classify_large_code IN ('B04','B05','B06','B07','B08')
       OR classify_middle_code!='B0102') 
group by 
      channel_name,
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       substr(sdt,1,6)
)a
)a
where row_num<101;




--全国渠道省区名称
SELECT mon,
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sales_cost,
       sales_value,
       profit,
       sales_qty,
       row_num
from (
SELECT mon,
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sales_cost,
       sales_value,
       profit,
       sales_qty,
       row_number()over(PARTITION BY mon order by sales_value desc) as row_num
from 
(SELECT substr(sdt,1,6)mon,
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sum(sales_cost) as sales_cost,
       sum(sales_value) as sales_value,
       sum(profit) as profit,
       sum(sales_qty) as sales_qty
FROM csx_dw.dws_sale_r_d_detail
WHERE sdt>='20200101'
  AND sdt<'20210101'
  AND (classify_large_code IN ('B04','B05','B06','B07','B08')
       OR classify_middle_code!='B0102') 
group by 
      
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       substr(sdt,1,6)
)a
)a
where row_num<101;




--渠道省区名称
SELECT mon,
       province_code,
       province_name,
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sales_cost/sales_qty as cost,
       sales_value/sales_qty as price,
       sales_cost,
       sales_value,
       profit,
       sales_qty,
       row_num
from (
SELECT mon,
       province_code,
       province_name,
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sales_cost,
       sales_value,
       profit,
       sales_qty,
       row_number()over(PARTITION BY province_code,mon order by sales_value desc) as row_num
from 
(SELECT substr(sdt,1,6)mon,
        
       province_code,
       province_name,
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       sum(sales_cost) as sales_cost,
       sum(sales_value) as sales_value,
       sum(profit) as profit,
       sum(sales_qty) as sales_qty
FROM csx_dw.dws_sale_r_d_detail
WHERE sdt>='20200101'
  AND sdt<'20210101'
  AND (classify_large_code IN ('B04','B05','B06','B07','B08')
       OR classify_middle_code!='B0102') 
group by 
      
       province_code,
       province_name,
       goods_code,
       goods_name,
       unit,
       spec,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       substr(sdt,1,6)
)a
)a
where row_num<101;
