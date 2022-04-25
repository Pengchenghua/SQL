
select mon,
    province_code ,
    province_name ,
    case when sales_dist_code ='310000' then '云创' 
         when ascription_type ='29' then 'MINI' 
         when (ascription_type='17' or customer_no in ('103903','103097','108955','104842','103862','103833')) then '中百/红旗'
         else '云超'
         end as  shop_type,
    fact_type,
    case when upper(trim(unit))='KG' then 'KG' 
        else '盒/份' 
        end unit_note ,
    sum(sale) AS  factory_sale
    ,sum(qty)qty
from 
(select substring(sdt,1,6) as mon,
    province_code ,
    province_name ,
    customer_no,
    workshop_code ,
    workshop_name ,
    goods_code ,
    unit ,
    case when is_factory_goods_code=1 then '加工' else '转配' end as fact_type,
    sum(sales_value  ) AS  sale,
    sum(sales_qty )as qty
   
from
    csx_dw.dws_sale_r_d_customer_sale 
where
    sdt >= '20190901'
    and sdt <= '20191231'
  --  and province_code = '32'
  --  and is_factory_goods_code = 1
  and channel ='2'
group by  substring(sdt,1,6),
    province_code ,
    province_name ,
    customer_no,
    workshop_code ,
    workshop_name ,
    goods_code ,
    unit,
    case when is_factory_goods_code=1 then '加工' else '转配' end
) as a 
left join 
(select  sales_dist_code,ascription_type ,location_code from csx_dw.csx_shop where sdt='current' and table_type =2)as b 
on a.customer_no=concat('S',location_code )
group by 
     province_code ,
     province_name ,
    case when sales_dist_code ='310000' then '云创' 
         when ascription_type ='29' then 'MINI' 
         when (ascription_type='17' or customer_no in ('103903','103097','108955','104842','103862','103833')) then '中百/红旗'
         else '云超'
         end ,
    fact_type,
    case when upper(trim(unit))='KG' then 'KG' 
        else '盒/份' 
        end ,
        mon
;