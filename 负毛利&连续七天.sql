--连续7天负毛利

SELECT b.sdt,
          province_code,
          province_name,
          customer_no,
          customer_name,
          goods_code,
          bar_code,
          goods_name,
          unit,
          avg_cost,
          avg_price,
          sales_value,
          profit,
          front_profit,
          rn,
          cn
FROM
  (SELECT sdt,
          province_code,
          province_name,
          customer_no,
          customer_name,
          goods_code,
          bar_code,
          goods_name,
          unit,
          cost/qty as avg_cost,
          cost,
          sales_value/qty avg_price,
          qty,
          sales_value,
          profit,
          front_profit,
          row_number() over( partition BY province_code, customer_no, goods_code
                            ORDER BY sdt ) rn,
                       count(sdt) over( partition BY province_code, customer_no, goods_code ) cn
   FROM
     ( SELECT sdt,
              province_code,
              province_name,
              customer_no,
              customer_name,
              goods_code,
              bar_code,
              goods_name,
              unit,
              sum(sales_cost)cost,
              sum(sales_qty)qty,
              sum(sales_value)sales_value,
              sum(profit)profit,
              sum(front_profit)front_profit
      FROM csx_dw.customer_sale_m
      WHERE sdt <= regexp_replace(to_date(date_sub(current_timestamp(), 1)), '-', '')
        AND sdt >= regexp_replace(to_date(date_sub(current_timestamp(), 8)), '-', '')
        AND channel IN ('1',
                        '7')
        and order_kind !='WELFARE'
      GROUP BY sdt,
               province_code,
               province_name,
               customer_no,
               customer_name,
               goods_code,
               bar_code,
               goods_name,
               unit ) as a
   WHERE profit<-100 )  as b
WHERE cn >=7
ORDER BY b.province_code,
         b.customer_no,
         b.goods_code,
         b.sdt
        ;
-- 每日负毛利
select sdt,
              province_code,
              province_name,
              customer_no,
              customer_name,
              goods_code,
              bar_code,
              goods_name,
              unit,
              cost/qty as avg_cost,
              sales_value/qty avg_price,
              cost,
              qty,
              sales_value,
              profit,
              front_profit
from (
 SELECT sdt,
              province_code,
              province_name,
              customer_no,
              customer_name,
              goods_code,
              bar_code,
              goods_name,
              unit,
              sum(sales_cost)cost,
              sum(sales_qty)qty,
              sum(sales_value)sales_value,
              sum(profit)profit,
              sum(front_profit)front_profit
      FROM csx_dw.customer_sale_m
      WHERE sdt = regexp_replace(to_date(date_sub(current_timestamp(), 1)), '-', '')
       -- AND sdt >= regexp_replace(to_date(date_sub(current_timestamp(), 8)), '-', '')
        AND channel IN ('1',
                        '7')
        and order_kind !='WELFARE'
      GROUP BY sdt,
               province_code,
               province_name,
               customer_no,
               customer_name,
               goods_code,
               bar_code,
               goods_name,
               unit 
        --HAVING sum(profit)<-200
        )as a where profit<-200
        order by 
        province_code,
        customer_no,
        goods_code,
        sdt
        ;
