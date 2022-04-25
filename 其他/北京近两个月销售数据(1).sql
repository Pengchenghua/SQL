select smm,c.sflag,c.dist,a.shop_id,b.shop_name,shopid_orig,a.cust_id,c.cust_name,c.source_name,c.type_1_name,c.type_2_name,
dept_id,dept_name,catg_l_id,catg_l_name,catg_m_id,catg_m_name,catg_s_id,catg_s_name,a.goodsid,goodsname,
sum(xsl)xsl,sum(xse)xse,
sum(mle)mle,sum(wsxse)wsxse,sum(wsmle)wsmle
from
(select substr(sdt,1,6)smm,shop_id,customer_no cust_id,origin_shop_id shopid_orig,goods_code goodsid,
        sum(sales_qty)xsl
       ,sum(sales_value) xse
       ,sum(profit) mle
       ,sum(excluding_tax_sales) wsxse
       ,sum(excluding_tax_profit) wsmle
from  csx_dw.sale_b2b_item  
where sdt>='20190601'
and sdt<='20190731'
and (substr(customer_no,1,3)<>'S99' or customer_no='S9961')
and sales_type in('qyg','gc','anhui') and shop_id<>'W098'
group by substr(sdt,1,6),shop_id,customer_no,origin_shop_id,goods_code)a
join 
(select shop_id,shop_name,dist from b2b.dim_shops_current where sales_dist_new_name like '%彩食鲜%')b on a.shop_id=b.shop_id
join (select distinct sflag,dist, cust_id,cust_name,source_name,type_1_name,type_2_name
from csx_ods.b2b_customer_new a 
where  csx_dw.customer_m where sdt='20190831'and (sflag<>'M端' or sflag is null))c on lpad(a.cust_id,10,'0')=lpad(c.cust_id,10,'0')
join 
(select goodsid,regexp_replace(regexp_replace(goodsname,'\n',''),'\r','') as goodsname
,dept_id,dept_name,catg_l_id,catg_l_name,catg_m_id,catg_m_name,catg_s_id,catg_s_name
from dim.dim_goods
where edate='9999-12-31')x on a.goodsid=x.goodsid

group by smm,c.sflag,c.dist,a.shop_id,b.shop_name,shopid_orig,a.cust_id,c.cust_name,c.source_name,c.type_1_name,c.type_2_name,
dept_id,dept_name,catg_l_id,catg_l_name,catg_m_id,catg_m_name,catg_s_id,catg_s_name,a.goodsid,goodsname
order by smm,c.sflag,shop_id,a.cust_id;




select c.bd_id,c.bd_name,c.dept_id,c.dept_name,c.catg_l_id,c.catg_l_name,c.catg_m_id,c.catg_m_name,
c.brandname,a.goodsid,c.goodsname,
sum(xsl)xsl,sum(xse)xse,
sum(mle)mle,sum(wsxse)wsxse,sum(wsmle)wsmle
from
(select cust_id,goodsid,
        sum(qty)xsl
       ,sum(tax_salevalue) xse
       ,sum(tax_profit) mle
       ,sum(untax_salevalue) wsxse
       ,sum(untax_profit) wsmle
from  csx_ods.sale_b2b_dtl_fct 
where sdt>='${SDATE}'
and sdt<='${EDATE}'
and (substr(cust_id,1,3)<>'S99' or cust_id='S9961')
and sflag in('qyg','qyg_c','gc') and shop_id<>'W098'
group by cust_id,goodsid)a
join 
(select cust_id,dist from csx_ods.b2b_customer_new
where  (sflag<>'M端' or sflag is null)
${if(len(sf)==0,"","AND dist = '"+sf+"'")})b on lpad(a.cust_id,10,'0')=lpad(b.cust_id,10,'0')
join 
(select goodsid,regexp_replace(regexp_replace(goodsname,'\n',''),'\r','') as goodsname
,regexp_replace(regexp_replace(brand_name,'\n',''),'\r','') as brandname
,bd_id,bd_name,dept_id,dept_name,catg_l_id,catg_l_name,catg_m_id,catg_m_name
from dim.dim_goods
where edate='9999-12-31')x on a.goodsid=x.goodsid
where a.xsl<>0 or a.xse<>0 or a.mle<>0 
group by c.bd_id,c.bd_name,c.dept_id,c.dept_name,c.catg_l_id,c.catg_l_name,c.catg_m_id,c.catg_m_name,c.brandname,a.goodsid,c.goodsname
order by 13 desc;



join 
(select goodsid,regexp_replace(regexp_replace(goodsname,'\n',''),'\r','') as goodsname
,regexp_replace(regexp_replace(brand_name,'\n',''),'\r','') as brandname
,bd_id,bd_name,dept_id,dept_name,catg_l_id,catg_l_name,catg_m_id,catg_m_name
from dim.dim_goods
where edate='9999-12-31')c on a.goodsid=c.goodsid
