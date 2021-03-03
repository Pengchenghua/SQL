-- CONNECTION: name=Hadoop - HIVE


select sdt,case when shopid_orig='W0B6' then 'BBC' when source_id_new='22' then 'M��' 
when a.shop_id='W0H4' and a.cust_id like 'S%' then '��Ӧ��(S��)' else c.sflag end qdflag,
case when c.dist is not null and c.sflag<>'M��' then substr(dist,1,2)
when a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('����','�Ĵ�','����','����','�Ϻ�','�㽭','����','����','�㶫') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end dist,region_no,region_manage,
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
select substr(a.sdt,1,6)sdt,a.shop_id,origin_shop_id shopid_orig,vendor_code vendor_id,customer_no cust_id,goods_code goodsid, 
sum(sales_qty )qty,
sum(a.sales_sales_cost )sales_cost,
sum(a.sales_value )sale,
sum(a.profit )profit
from  csx_dw.sale_b2b_item 	 a
where sdt>=${hiveconf:sdate} AND SDT<=${hiveconf:edate}
and shop_id<>'W098'
and sales_type in('qyg','gc','anhui') 
  and a.category_small_code between '12000000' and '14999999'
group by shop_id,customer_no ,origin_shop_id ,substr(a.sdt,1,6),a.vendor_code ,goods_code 
)a
left join csx_ods.b2b_customer_new c on lpad(a.cust_id,10,'0')=lpad(c.cust_id,10,'0')
left join
(select shop_id,case when shop_id in ('W055','W056') then '�Ϻ���' else prov_name end prov_name,case when prov_name like '%��' then prov_name else city_name end city_name from dim.dim_shop where edate='9999-12-31' )b 
on a.cust_id=concat('S',b.shop_id)
left join (select shop_id, prov_name 
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
group by sdt,case when shopid_orig='W0B6' then 'BBC' when source_id_new='22' then 'M��' 
when a.shop_id='W0H4' and a.cust_id like 'S%' then '��Ӧ��(S��)' else c.sflag end  ,
case when c.dist is not null and c.sflag<>'M��' then substr(dist,1,2)
when a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('����','�Ĵ�','����','����','�Ϻ�','�㽭','����','����','�㶫') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end,region_no,region_manage,
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
g.vendor_id;



SELECT DISTINCT sales_date FROM CSX_DW.sale_goods_m where channel='-1';