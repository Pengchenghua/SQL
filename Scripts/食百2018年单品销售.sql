--ʳ��2018������
select sdt,case when shopid_orig='W0B6' then 'BBC' when source_id_new='22' then 'M��' else c.sflag end qdflag,
case when c.dist is not null and c.sflag<>'M��' then substr(dist,1,6)
else substr(d.prov_name,1,6) end dist,region_no,region_manage,
--manage,manage_no,��ʱ�Ȳ���ӣ�ֱ�Ӱ��ձ����ĸ�����棬Ȫ�ݺ���ˮ
a.cust_id,c.cust_name,type_1_id,type_1_name,type_2_name,
a.vendor_id,vendor_name,a.goodsid,goodsname,unit_name,
firm_g1_id,
firm_g1_name,
catg_l_id,
catg_l_name,
catg_m_id,
catg_m_name,
catg_s_id,
catg_s_name,
sum(qty)qty,
sum(sales_cost)sales_cost,
sum(sale)sale,
sum(profit)profit,
case when g.vendor_id is not null then 'zj' end zj_sflag
from 
(
select substr(a.sdt,1,6)sdt,a.shop_id,shopid_orig,a.vendor_id,a.cust_id,goodsid, 
sum(qty)qty,
sum(a.tax_sales_costvalue)sales_cost,
sum(a.tax_salevalue)sale,
sum(a.tax_profit)profit
from  csx_ods.sale_b2b_dtl_fct	 a
where sdt>='20180101'
and sdt<='20181231'
and shop_id<>'W098'
and sflag in('qyg','qyg_c') 
  and a.catg_s_id between '12000000' and '14999999'
group by shop_id,cust_id,shopid_orig,substr(a.sdt,1,6),a.vendor_id,goodsid
union all
select substr(a.sdt,1,6)sdt,a.shop_id,shopid_orig,a.vendor_id,a.cust_id,goodsid, 
sum(qty)qty,
sum(a.tax_sales_costvalue)sales_cost,
sum(a.tax_salevalue)sale,
sum(a.tax_profit)profit
from  csx_ods.sale_b2b_dtl_fct a
left join 
(select concat('S',shop_id)cust_id from dim.dim_shop where edate='9999-12-31' and sales_dist_new between '600000' and '690000' ) c
    on a.cust_id=c.cust_id where c.cust_id is null
       and a.sflag in ('gc') and a.catg_s_id between '12000000' and '14999999'
   and sdt>='20180101'
and sdt<='20181231'
and shop_id<>'W098'
 group by substr(a.sdt,1,6),a.shop_id,a.vendor_id,goodsid,a.cust_id,shopid_orig

)a
left join csx_ods.b2b_customer_new c on lpad(a.cust_id,10,'0')=lpad(c.cust_id,10,'0')
left join (select shop_id,case when shop_id in ('W055','W056') then '�Ϻ���' else prov_name end prov_name 
            from dim.dim_shop where edate='9999-12-31')d on a.shop_id=d.shop_id
left join (select vendor_id,vendor_name from dim.dim_vendor where edate='9999-12-31' )f 
    on regexp_replace(a.vendor_id,'(^0*)','')=regexp_replace(f.vendor_id,'(^0*)','')
left join
    (select goodsid,goodsname,unit_name,
    a.firm_g1_id,
    a.firm_g1_name,a.catg_l_id,a.catg_l_name,a.catg_m_id,a.catg_m_name,a.catg_s_id,a.catg_s_name from dim.dim_goods a where edate='9999-12-31') j 
    on a.goodsid=j.goodsid
left join
    (select distinct a.vendor_id from  csx_dw.zj_vendor a) g on regexp_replace(a.vendor_id,'(^0*)','')=regexp_replace(g.vendor_id,'(^0*)','')
group by sdt,case when shopid_orig='W0B6' then 'BBC' when source_id_new='22' then 'M��' else c.sflag end ,
case when c.dist is not null and c.sflag<>'M��' then substr(dist,1,6)
else substr(d.prov_name,1,6) end ,region_no,region_manage,
--manage,manage_no,��ʱ�Ȳ���ӣ�ֱ�Ӱ��ձ����ĸ�����棬Ȫ�ݺ���ˮ
a.cust_id,c.cust_name,type_1_id,type_1_name,type_2_name,
a.vendor_id,vendor_name,a.goodsid,goodsname,unit_name,
firm_g1_id,
firm_g1_name,
catg_l_id,
catg_l_name,
catg_m_id,
catg_m_name,
catg_s_id,
catg_s_name,
g.vendor_id