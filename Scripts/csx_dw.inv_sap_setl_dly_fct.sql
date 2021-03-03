-- CONNECTION: name= HVIE
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.dynamic.partition = true;
insert overwrite table csx_dw.inv_sap_setl_dly_fct partition(sdt)
select
a.`calday`,
a.`calmonth`,
a.`month`,
a.`comp_code`,
a.`currency_code`,
a.`goodsid`,
a.`goods_uid`,
a.`shop_id`,
a.`shop_uid`,
a.`price_unit`,
b.`sales_dist_new`,
a.`inv_place`,
a.`vendor_id`,
a.`goods_sts`,
a.`operation_type`,
a.`goods_main_id`,
a.`div_id`,
a.`dept_id`,
a.`prod_area`,
a.`area_id`,
a.`bd_id`,
a.`catg_l_id`,
a.`catg_m_id`,
a.`catg_s_id`,
a.`in_vat_rare`,
a.`inv_val`,
a.`inv_qty`,
a.`period_inv_amt`,
a.`cycle_unit_price`,
a.`logistics_pattern`,
a.`unit`,
a.`insert_time`,
a.sdt from 
(
 select * from dw.inv_sap_setl_dly_fct 
 where sdt = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
) a join dim.dim_shop_latest b on a.shop_id=b.shop_id
where b.sales_dist_new_name like '%��ʳ��%';