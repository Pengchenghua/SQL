SELECT sales_province_code,
       sales_province,
       a.customer_no,
       customer_name,
       sign_time,
       sales_name,
       sale,profit,c.sdt,c.min_sale ,max_sdt,d.max_sale
FROM
  (SELECT *
   FROM csx_dw.customer_m a
   WHERE channel LIKE '%¶ÔÍâ%'
     AND sdt='20191112'
     AND customer_no!='')a
LEFT OUTER JOIN
(SELECT customer_no,
          sum(sales_value)sale,
          sum(profit)profit
   FROM csx_dw.customer_sales
   WHERE sdt>='20190101'
     AND sdt<='20191112'
   GROUP BY customer_no) b on  a.customer_no=b.customer_no
LEFT OUTER JOIN
  (SELECT c.customer_no,
          sdt,
          sum(c.sales_value)min_sale
   FROM csx_dw.customer_sales c
   join (SELECT d.customer_no,min(d.sdt)min_sdt
        FROM csx_dw.customer_sales d
         group BY d.customer_no
        )d  ON d.customer_no=c.customer_no and c.sdt=d.min_sdt
   GROUP BY c.customer_no,c.sdt )c on a.customer_no=c.customer_no
LEFT OUTER JOIN 
 (SELECT c.customer_no,
          sdt as max_sdt,
          sum(sales_value)max_sale
   FROM csx_dw.customer_sales c
   join (SELECT d.customer_no,max(d.sdt)max_sdt
        FROM csx_dw.customer_sales d
         group BY d.customer_no
        )d  ON d.customer_no=c.customer_no and c.sdt=d.max_sdt
   group by c.customer_no,c.sdt )d on a.customer_no=d.customer_no