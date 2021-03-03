create table csx_dw.csx_sale_provice_pdf
as 
(
  no  int comment '序号',
  province_code string comment'省区编码',
  province_name string comment'省区名称',
  category_code string comment'部类编码',
  day_sale      decimal (26,6) comment'昨日销售额',
  day_profit    decimal(26,6) comment'昨日毛利额',
  day_profit_rate decimal(26,6) comment'昨日毛利率',
  sales_value   decimal(26,6) comment'累计销售额',
  ring_sales_value   decimal(26,6) comment'上期销售额',
  ring_sales_rate  decimal(26,6) comment'环期增长率',
  profit        decimal(26,6) comment'累计毛利额',
  profit_rate   decimal(26,6) comment'累计毛利率',
  negative_sku  string comment'负库存数',
  sale_sku      string comment'动销SKU数',
  all_sku       string comment'SKU数',
  pin_rate      string comment'动销率',
  final_amt     decimal(26,6) comment'期末库存额',
  day_turnover  string comment'周转天数',
  receive_amt   decimal(26,6) comment'入库金额',
  shop_dept_cust string comment'商超客户数',
  big_dept_cust string comment'大客户客户数',
  sale_cust_ratio   decimal(26,6) comment'大客户渗透率',
)
comment'省区销售日报表'
partitioned by (sdt string comment'日期分区')
stored as parquet
;

create table csx_dw.csx_sale_factory_pdf
as 
(
  no  int comment '序号',
  province_code string comment'省区编码',
  province_name string comment'省区名称',
  workshop_code string comment'车间编码',
  workshop_name string comment'车间名称',
  day_sale      decimal (26,6) comment'昨日销售额',
  day_profit    decimal(26,6) comment'昨日毛利额',
  day_profit_rate decimal(26,6) comment'昨日毛利率',
  sales_value   decimal(26,6) comment'累计销售额',
  ring_sales_value   decimal(26,6) comment'上期销售额',
  ring_sales_rate  decimal(26,6) comment'环期增长率',
  profit        decimal(26,6) comment'累计毛利额',
  profit_rate   decimal(26,6) comment'累计毛利率',
  negative_sku  string comment'负库存数',
  sale_sku      string comment'动销SKU数',
  all_sku       string comment'SKU数',
  pin_rate      string comment'动销率',
  final_amt     decimal(26,6) comment'期末库存额',
  day_turnover  string comment'周转天数',
  receive_amt   decimal(26,6) comment'入库金额',
  shop_dept_cust string comment'商超客户数',
  big_dept_cust string comment'大客户客户数',
  sale_cust_ratio   decimal(26,6) comment'大客户渗透率',
  fact_qty decimal(26,6) comment'产品量',
  fact_amt decimal(26,6) comment'产品额',
  product_rate decimal(26,6) comment'出品率 plan_user/fact_qty ',
  precision_rate decimal(26,6) comment'计划达成率'

)
comment'省区工厂日报表'
partitioned by (sdt string comment'日期分区')
stored as parquet
;

            sum(fact_qty)/sum(user_qty) AS product_rate,--出品率
            sum(plan_user)plan_qty,
            sum(fact_qty)/sum(plan_user) AS precision_rate --计划达成率
no,
 a.province_code,
       a.province_name,
       a.workshop_code,
       a.workshop_name,
       a.day_sale/10000 day_sale,
       a.day_profit/10000 day_profit,
       day_profit_rate,
       a.sales_value/10000 sales_value,
       a.profit/10000 profit,
       profit_rate,
       a.negative_sku,
       a.sale_sku,
       a.all_sku,
       pin_rate,
       a.final_amt/10000 final_amt,
       a.day_turnover,
       a.receive_amt/10000 receive_amt,
       a.shop_dept_cust,
       a.big_dept_cust,
       a.sale_cust_ratio,
       fact_qty/10000 fact_qty,
       fact_amt/10000 fact_amt,
       product_rate,
       precision_rate;



'105514',
'105693',
'106439',
'106684',
'103058',
'104172',
'104664',
'104791',
'105575',
${if(len(地区)=0,"","and 货主城市 in ('"+SUBSTITUTE(地区,",","','")+"')")} 

                   "and             ('"+SUBSTITUTE(tree01,",","','")+"')")}
 

