

select goods_code,goods_name,division_code,division_name,category_large_code,category_large_name,sale,profit ,sales_qty,NUM
from (
select goods_code,goods_name,division_code,division_name,category_large_code,category_large_name,sale,profit ,sales_qty,rank()over(PARTITION by category_large_code order by sale desc) as NUM
from (
select goods_code,goods_name,division_code,division_name,category_large_code,category_large_name,sum(sales_value)sale,sum(profit)profit ,sum(sales_qty)sales_qty
from csx_dw.dws_sale_r_d_customer_sale a 
join 
(select customer_no,cm.`attribute` from csx_dw.customer_m cm where sdt='20200305' and cm.`attribute`!='5' and customer_no!='') as b on  a.customer_no=b.customer_no 
where sdt>='20200401' and sdt<='20200430'  and province_name not like '%平台%'
group by goods_code,goods_name,division_code,division_name,category_large_code,category_large_name
) a 
) a where num <31 and division_code in ('10','11')
;

-- 省区
select
	province_code,
	province_name,
	goods_code,
	goods_name,
	division_code,
	division_name,
	category_large_code,
	category_large_name,
	sale,
	profit ,
	sales_qty,
	NUM
from
	(
	select
		province_code,
		province_name,
		goods_code,
		goods_name,
		division_code,
		division_name,
		category_large_code,
		category_large_name,
		sale,
		profit ,
		sales_qty,
		rank() over(PARTITION by category_large_code,province_code
	order by
		sale desc) as NUM
	from
		(
		select
			province_code,
			province_name,
			goods_code,
			goods_name,
			division_code,
			division_name,
			category_large_code,
			category_large_name,
			sum(sales_value)sale,
			sum(profit)profit ,
			sum(sales_qty)sales_qty
		from
			csx_dw.customer_sale_m a
		join (
			select
				customer_no,
				cm.`attribute`
			from
				csx_dw.customer_m cm
			where
				sdt = '20200506'
				and cm.`attribute` != '5'
				and customer_no != '') as b on
			a.customer_no = b.customer_no
		where
			sdt >= '20200401'
			and sdt <= '20200430'
			and province_name not like '%平台%'
		group by
			goods_code,
			goods_name,
			division_code,
			division_name,
			category_large_code,
			category_large_name,
			province_code,
			province_name ) a ) a
where
	num <31
	and division_code in ('10',
	'11') ;
	
-- 省区tOP	100 剔除合伙人
select
province_code,province_name,
    goods_code         ,
    goods_name         ,
    unit,
    division_code      ,
    division_name      ,
    category_large_code,
    category_large_name,
    sale               ,
    profit             ,
    profit/sale as prorate,
    sales_qty          ,
    NUM,
    prov_ratio,categ_ratio
from
    (
        select
            province_code,
            province_name,
            goods_code         ,
            goods_name         ,
            unit,
            division_code      ,
            division_name      ,
            category_large_code,
            category_large_name,
            sale               ,
            profit             ,
            sales_qty          ,
            rank()over(PARTITION by  province_code order by  sale desc) as NUM,
            sale/sum(sale)over( partition by province_code) prov_ratio,
            sale/sum(sale)over(partition by  province_code, category_large_code) categ_ratio
        from
            (
                select
                    province_code,
                    province_name,
                    goods_code             ,
                    goods_name             ,
                    unit,
                    division_code          ,
                    division_name          ,
                    category_large_code    ,
                    category_large_name    ,
                    sum(sales_value)sale   ,
                    sum(profit)     profit ,
                    sum(sales_qty)  sales_qty
                from
                    csx_dw.dws_sale_r_d_customer_sale a
                  left   join (select DISTINCT customer_no from csx_dw.csx_partner_list where sdt='202006') b on a.customer_no =b.customer_no

where 
--b.customer_no is null and 
                    sdt    >=regexp_replace(to_date('${sdate}'),'-','')
                    and sdt<=regexp_replace(to_date('${edate}'),'-','')
and a.channel in ('1')
group by
province_code,province_name,
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
and num<101
order by 
 province_code,
num
;

-- 省区tOP    100 剔除合伙人
select
province_code,province_name,
    goods_code         ,
    goods_name         ,
    unit,
    division_code      ,
    division_name      ,
    department_code ,
    department_name ,
    sale               ,
    profit             ,
    profit/sale as prorate,
    sales_qty          ,
    NUM,
    prov_ratio,categ_ratio
from
    (
        select
            province_code,
            province_name,
            goods_code         ,
            goods_name         ,
            unit,
            division_code      ,
            division_name      ,
            department_code,
            department_name ,
            sale               ,
            profit             ,
            sales_qty          ,
            rank()over(PARTITION by  province_code,department_code order by  sale desc) as NUM,
            sale/sum(sale)over( partition by province_code) prov_ratio,
            sale/sum(sale)over(partition by  province_code, department_code ) categ_ratio
        from
            (
                select
                    province_code,
                    province_name,
                    goods_code             ,
                    goods_name             ,
                    unit,
                    division_code          ,
                    division_name          ,
                    department_code    ,
                    department_name    ,
                    sum(sales_value)sale   ,
                    sum(profit)     profit ,
                    sum(sales_qty)  sales_qty
                from
                    csx_dw.dws_sale_r_d_customer_sale a
               left   join (select DISTINCT customer_no from csx_dw.csx_partner_list where sdt='202006') b on a.customer_no =b.customer_no

        where 
                    b.customer_no is null and 
                    sdt    >=regexp_replace(to_date('${sdate}'),'-','')
                    and sdt<=regexp_replace(to_date('${edate}'),'-','')
and a.channel in ('1')
group by
province_code,province_name,
                    goods_code         ,
                    goods_name         ,
                    unit,
                    division_code      ,
                    division_name      ,
                    department_code,                    
                    department_name
            )
            a
    )
    a
where
1=1
and num<31
order by 
 province_code,
 department_code,
num
;

-- 省区tOP    100 剔除合伙人
select
province_code,province_name,
    goods_code         ,
    goods_name         ,
    unit,
    division_code      ,
    division_name      ,
    department_code ,
    department_name ,
    sale               ,
    profit             ,
    profit/sale as prorate,
    sales_qty          ,
    NUM,
    prov_ratio,categ_ratio
from
    (
        select
            province_code,
            province_name,
            goods_code         ,
            goods_name         ,
            unit,
            division_code      ,
            division_name      ,
            department_code,
            department_name ,
            sale               ,
            profit             ,
            sales_qty          ,
            rank()over(PARTITION by  province_code,department_code order by  sale desc) as NUM,
            sale/sum(sale)over( partition by province_code) prov_ratio,
            sale/sum(sale)over(partition by  province_code, department_code ) categ_ratio
        from
            (
                select
                    province_code,
                    province_name,
                    goods_code             ,
                    goods_name             ,
                    unit,
                    division_code          ,
                    division_name          ,
                    department_code    ,
                    department_name    ,
                    sum(sales_value)sale   ,
                    sum(profit)     profit ,
                    sum(sales_qty)  sales_qty
                from
                    csx_dw.dws_sale_r_d_customer_sale a
--               left   join (select DISTINCT customer_no from csx_dw.csx_partner_list where sdt='202006') b on a.customer_no =b.customer_no
--
       where 
--                    b.customer_no is null and 
                    sdt    >=regexp_replace(to_date('${sdate}'),'-','')
                    and sdt<=regexp_replace(to_date('${edate}'),'-','')
-- and a.channel in ('1')
group by
province_code,province_name,
                    goods_code         ,
                    goods_name         ,
                    unit,
                    division_code      ,
                    division_name      ,
                    department_code,                    
                    department_name
            )
            a
    )
    a
where
1=1
and num<31
order by 
 province_code,
 department_code,
num
;
refresh  csx_dw.supply_turnover;
SELECT * FROM
    csx_dw.supply_turnover WHERE sdt='20200719';