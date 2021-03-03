select
province_code,province_name,
    goods_code         ,
    goods_name         ,
    unit,
    division_code      ,
    division_name      ,
     department_code ,
                    department_name ,
    category_large_code,
    category_large_name,
    sale_sku,
    sale               ,
    profit             ,
    profit/sale as prorate,
    sales_qty          ,
    NUM,
    prov_ratio,categ_ratio
from
    (
        select
province_code,province_name,
            goods_code         ,
            goods_name         ,
            unit,
            division_code      ,
            division_name      ,
             department_code ,
                    department_name ,
            category_large_code,
            category_large_name,
            sale_sku,
            sale               ,
            profit             ,
            sales_qty          ,
            rank()over(PARTITION by department_code ,  province_code  order by  sale desc) as NUM,
            sale/sum(sale)over( partition by province_code) prov_ratio,
            sale/sum(sale)over(partition by  province_code,department_code ) categ_ratio
        from
            (
                select
province_code,province_name,
                    goods_code             ,
                    goods_name             ,
                    unit,
                    division_code          ,
                    division_name          ,
                    department_code ,
                    department_name ,
                    category_large_code    ,
                    category_large_name    ,
                    count(DISTINCT sdt ) as sale_sku,
                    sum(sales_value)sale   ,
                    sum(profit)     profit ,
                    sum(sales_qty)  sales_qty
                from
                    csx_dw.dws_sale_r_d_customer_sale a
                    join
                        (
                            select
                                customer_no,
                                cm.`attribute`
                            from
                                csx_dw.dws_crm_w_a_customer_m cm
                            where
    sdt  =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
    and customer_no   !=''
-- ${if (len(attribute_id)==0,"","and cm.`attribute` in ('"+attribute_id+"')")}
)as b on        a.customer_no=b.customer_no
where
                    sdt    >=regexp_replace(to_date('${sdate}'),'-','')
                    and sdt<=regexp_replace(to_date('${edate}'),'-','')
                    and channel ='1'
group by
province_code ,province_name ,
 department_code ,
                    department_name ,
                    goods_code         ,
                    goods_name         ,
                    unit,
                    division_code      ,
                    division_name      ,
                    category_large_code,                    
                    category_large_name
            )
            a
    )
    a
where
1=1
and num<31
order by 
province_code,
category_large_code,num
;