select
    province_code ,
    province_name ,
    department_code ,
    department_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    goods_code ,
    goods_name ,
    unit ,
    qty,
    sale,
    profit,
    category_middle_sale,
    rank()over(partition by province_code,category_middle_code order by category_middle_sale desc)as cate_rank,
    category_middle_profit
from
    (select
    province_code ,
    province_name ,
    department_code ,
    department_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    goods_code ,
    goods_name ,
    unit ,
    qty,
    sale,
    profit,
    sum(sale)over(partition by province_code ,
province_name ,
department_code ,
department_name ,
category_large_code ,
category_large_name ,
category_middle_code ,
category_middle_name) as category_middle_sale,
    sum(profit) over(partition by province_code ,
province_name ,
department_code ,
department_name ,
category_large_code ,
category_large_name ,
category_middle_code ,
category_middle_name) as category_middle_profit
from
    (select
    province_code ,
    province_name ,
    department_code ,
    department_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    goods_code ,
    goods_name ,
    unit ,
    sum(sales_qty) qty,
    sum(sales_value) sale,
    sum(profit) profit
from
    csx_dw.dws_sale_r_d_customer_sale
where
    sdt >= '20200101'
    and division_code in ('12', '13', '14')
group by
    province_code ,
    province_name ,
    department_code ,
    department_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name,
    goods_code ,
    goods_name ,
    unit)a
    --四川和重庆的食百销售TOP前50个中类，另这50个中类的TOP前5单品明细

) a