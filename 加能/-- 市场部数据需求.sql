-- 市场部数据需求
-- 1、 重庆市分行业数、2021年销售额、销售占比（重点是教育行业、政府机关单位、部队、团餐企业、事业单、金融、商超渠道等）
-- 2、 重庆市按产品分类销售占比，蔬菜、水果、肉禽、冻品、调味料、食百等
-- 3、 重庆预制菜（调理品、沙拉、半成品菜、净菜）2021年销售渠道（商超、餐饮、团餐等）及销售占比；前10大名称及销售额

select classify_large_name,classify_middle_name,sum(sales_value) sales,count(distinct customer_no) cn from csx_dw.dws_sale_r_d_detail
where sdt>='20210101'
    and sdt<'20220101'
    and province_code='32'
group by classify_large_name,classify_middle_name

;


select first_category_name,case when channel_code='2' then '商超' else second_category_name end second_category_name,
sum(sales_value) sales,count(distinct customer_no) cn 
from csx_dw.dws_sale_r_d_detail
where sdt>='20210101'
    and sdt<'20220101'
    and province_code='32'
    
group by  second_category_name  ,first_category_name

;

select b.classify_small_name,b.classify_small_code,business_type_name,a.customer_name,
    a.first_category_name,
case when channel_code='2' then '商超' else second_category_name end second_category_name,
sum(sales_value) sales,count(distinct customer_no) cn 
from csx_dw.dws_sale_r_d_detail a 
join
(select goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
from csx_dw.dws_basic_w_a_csx_product_m where sdt='current'
    and classify_small_code in (
    'B020210',
    'B020211',
    'B030503',
    'B030209',
    'B030504',
    'B030506',
    'B030505',
    'B030507',
    'B030508',
    'B030502',
    'B030106',
    'B030509',
    'B030501'
    )) b on a.goods_code=b.goods_id
where sdt>='20210101'
    and sdt<'20220101'
    and province_code='32'
    group by  b.classify_small_name,b.classify_small_code,business_type_name,a.customer_name,a.first_category_name,second_category_name
;

select distinct classify_large_code,classify_large_name,classify_middle_code,
classify_middle_name,classify_small_code,classify_small_name from csx_dw.dws_basic_w_a_manage_classify_m where sdt='current'
;



-- 二级行业TOP20

select a.customer_no,a.customer_name, a.first_category_name,second_category_name,sales,rank()over(partition by second_category_name order by sales desc)
from (
select a.customer_no,a.customer_name, a.first_category_name,
case when channel_code='2' then '商超' else second_category_name end second_category_name,
sum(sales_value) sales
from csx_dw.dws_sale_r_d_detail a 
where sdt>='20210101'
    and sdt<'20220101'
    and province_code='32'
    group by  a.customer_no,a.customer_name,a.first_category_name,
case when channel_code='2' then '商超' else second_category_name end
)a ;