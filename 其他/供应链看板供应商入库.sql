select
    location_type,
    dist_code,
    dist_name,
    dc_code ,
    dc_name ,
    goods_code ,
    goods_name,
    unit_name,
    dept_id,
    dept_name,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    category_small_code ,
    category_small_name,
    sales_cost,
    sales_qty,
    sales_value ,
    proft,
   profit_rate,
    period_amt,
    period_qty,
    final_qty,
    final_amt,
    days_turnover
    from (
select
    location_type,
    dist_code,
    dist_name,
    dc_code ,
    dc_name ,
    goods_code ,
    goods_name,
    unit_name,
    dept_id,
    dept_name,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    category_small_code ,
    category_small_name,
    sum(sales_cost )sales_cost,
    sum(sales_qty )sales_qty,
    sum(sales_value )sales_value ,
    sum(profit )proft,
  case when sum(sales_value )!=0 then  round(sum(profit )/sum(sales_value ),4) else 0 end  as profit_rate,
    sum(inventory_amt )as period_amt,
    sum(inventory_qty )as period_qty,
    sum(case when a.sdt=regexp_replace(to_date('${edate}'),'-','')  then inventory_qty end ) as final_qty,
     sum(case when a.sdt=regexp_replace(to_date('${edate}'),'-','')  then inventory_amt end ) as final_amt,
    case when sum(sales_cost )!=0 then    round(sum(inventory_amt )/sum(sales_cost ),0) else 999 end  as days_turnover
from
    csx_dw.dc_sale_inventory a
join (
    select
        *
    from
        csx_dw.csx_shop
    where
        sdt = 'current' and location_type ='仓库')b on
    a.dc_code = b.location_code
where
    a.sdt<=regexp_replace(to_date('${edate}'),'-','') 
    and a.sdt>=regexp_replace(to_date('${sdate}'),'-','') 
     ${if(len(dc)==0,"","and dc_code in ('"+SUBSTITUTE(dc,",","','")+"')")}
${if(len(dc)==0  ,"","and dc_code in ('"+dc+"') ") }
${if(len(dept_c)==0,"","and dept_id in ('"+dept_c+"')")}
${if(len(text)==0,"","and goods_code in ('"+REPLACE(text,",","','")+"')")}
group by dist_code,
    dist_name,
    dc_code ,
    dc_name ,
    goods_code ,
    goods_name,
    unit_name,
    dept_id,
    dept_name,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    category_small_code ,
    category_small_name,location_type
    ) a 
order by 
dist_code     ,
dc_code      ,
final_amt DESC;
