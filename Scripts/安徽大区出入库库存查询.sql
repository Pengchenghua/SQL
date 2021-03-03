-- select * from csx_ods.wms_frmloss_order_item_ods where sdt='20191011' 
-- �ص㡢��Ʒ���롢��Ʒ���ơ����顢������롢�������ơ���λ���ڳ���������ڳ���������������������������̵������̵���������������ĩ���������ĩ����;
-- move_type �����ȡ�ƶ�����;

set mapreduce.job.queuename=caishixian;

-- �ڳ���ĩ���
drop table if EXISTS temp.p_wms_01
;
create TEMPORARY TABLE if not EXISTS temp.p_wms_01 as
select
	a.product_code                    ,
	goodsname                         ,
	bar_code                          ,
	bd_id                             ,
	bd_name                           ,
	unit_name                         ,
	catg_l_id                         ,
	catg_l_name                       ,
	dept_id                           ,
	dept_name                         ,
	a.location_code                   ,
	a.shipper_code                    ,
	sum( qc_qty)             qc_qty   ,
	sum(qc_amt)              qc_amt   ,
	sum(qc_amt)/sum( qc_qty) qc_price ,
	sum(final_qty)              final_qty   ,
	sum(qm_amt)              qm_amt   ,
	sum(qm_amt) /sum(final_qty) qm_price ,
	is_self_product
from
	(
		select
			a.product_code             ,
			a.location_code            ,
			a.shipper_code             ,
			sum( after_qty)   qc_qty   ,
			sum(after_amt)    qc_amt   ,
			sum( after_price) qc_price ,
			0                 final_qty   ,
			0                 qm_amt   ,
			0                 qm_price
		from
			(
				select
					product_code                      ,
					location_code                     ,
					shipper_code                      ,
					after_qty                         ,
					after_amt                         ,
					after_price                       ,
					to_date(posting_time)posting_time ,
					id                                ,
					reservoir_area_code
				from
					csx_ods.wms_accounting_stock_detail_ods
				where
					sdt='20191011'
			)
			a
			join
				(
					select
						product_code        ,
						location_code       ,
						shipper_code        ,
						min(id)max_id       ,
						reservoir_area_code ,
						to_date(posting_time)posting_time
					from
						csx_ods.wms_accounting_stock_detail_ods
					where
						regexp_replace(to_date(posting_time),'-','')='20190901'
						and sdt              ='20191011'
					group by
						product_code          ,
						location_code         ,
						shipper_code          ,
						to_date(posting_time) ,
						reservoir_area_code
				)
				b
				on
					a.product_code      = b.product_code
					and a.location_code = b.location_code
					and a.shipper_code  = b.shipper_code
					and a.posting_time  = b.posting_time
					and a.id            = b.max_id
		group by
			a.product_code  ,
			a.location_code ,
			a.shipper_code
		union all
		select
			a.product_code  ,
			a.location_code ,
			a.shipper_code  ,
			0                 qc_qty        ,
			0                 qc_amt        ,
			0                 qc_price      ,
			sum( after_qty)   final_qty        ,
			sum(after_amt)    qm_amt        ,
			sum( after_price) qm_price
		from
			(
				select
					product_code                      ,
					location_code                     ,
					shipper_code                      ,
					after_qty                         ,
					after_amt                         ,
					after_price                       ,
					regexp_replace(to_date(posting_time),'-','')posting_time ,
					id                                ,
					reservoir_area_code
				from
					csx_ods.wms_accounting_stock_detail_ods
				where
					sdt='20191011'
			)
			a
			join
				(
					select
						product_code  ,
						location_code ,
						shipper_code  ,
						max(id)max_id ,
						reservoir_area_code
					from
						csx_ods.wms_accounting_stock_detail_ods
					where
						regexp_replace(to_date(posting_time),'-','')    >='20190901'
						and regexp_replace(to_date(posting_time),'-','')<='20190930'
						and sdt                   ='20191011'
					group by
						product_code  ,
						location_code ,
						shipper_code  ,
						reservoir_area_code
				)
				b
				on
					a.product_code           = b.product_code
					and a.location_code      = b.location_code
					and a.shipper_code       = b.shipper_code
					AND A.reservoir_area_code=b.reservoir_area_code
					and a.id                 = b.max_id
		group by
			a.product_code  ,
			a.location_code ,
			a.shipper_code
	)a	
	join
		(
			select
				goodsid       ,
				goodsname     ,
				a.bar_code    ,
				a.bd_id       ,
				a.bd_name     ,
				a.unit_name   ,
				a.catg_l_id   ,
				a.catg_l_name ,
				a.dept_id     ,
				a.dept_name   ,
				if(goods_code is null, '��', '��'	)	as is_self_product
			from
				dim.dim_goods_latest a
				left join
					(
						select distinct
							goods_code
						from
							csx_dw.factory_bom
						where
							sdt = '20191011'
					)
					b
					on
						a.goodsid=b.goods_code
		)
		c
		on
			a.product_code = c.goodsid
group by
	a.product_code  ,
	goodsname       ,
	bar_code        ,
	bd_id           ,
	bd_name         ,
	unit_name       ,
	catg_l_id       ,
	catg_l_name     ,
	dept_id         ,
	dept_name       ,
	a.location_code ,
	a.shipper_code  ,
	is_self_product
;
-- ��⡢�̵㣨���ʣ������𡢳���
drop table if EXISTS  temp.p_wms_02;
create TEMPORARY TABLE if not EXISTS  temp.p_wms_02
as 
select
	a.product_code                    ,
	a.location_code                   ,
	sum(case when move_type in ('101A','102A','105A','108A','120A') then  txn_qty end )as enter_qty,-- �������
	sum(case when move_type in ('101A','102A','105A','108A','120A') then  txn_amt end )as enter_amt,-- �����
	sum(case when move_type in ('104A','106A','103A','107A')then  txn_qty end )as out_qty,-- ������
	sum(case when move_type in ('104A','106A','103A','107A') then  txn_amt end )as out_amt,-- ����
	sum(case when move_type in ('117A') then  txn_qty end )as loss_qty,-- ������
	sum(case when move_type in ('117A')  then  txn_amt end )as loss_amt,-- �����
	sum(case when move_type in ('116A') then  txn_qty end  )stock_loss_qty,-- �̿���
	sum(case when move_type in ('116A') then  txn_amt end  )stock_loss_amt,-- �̿���
	sum(case when move_type in ('115A') then  txn_qty end  )stock_profit_qty,-- ��ӯ��
	sum(case when move_type in ('115A') then  txn_amt end  )stock_profit_amt   -- ��ӯ��
from 
	csx_ods.wms_accounting_stock_detail_view_ods a
where sdt='20191011'
and regexp_replace(to_date(posting_time),'-','') >='20190902'
and regexp_replace(to_date(posting_time),'-','') <='20190930'
group by a.product_code                    ,
		a.location_code
;
--  δ�����̵�����
drop table if EXISTS  temp.p_wms_03;
create TEMPORARY TABLE if not EXISTS  temp.p_wms_03
as 
select
	warehouse_code ,
	product_code,
	sum(inventory_qty_diff)inventory_qty_diff,
	sum(inventory_amount_diff)inventory_amt_diff
from
	csx_ods.wms_inventory_product_detail_ods a
where
	sdt = '20191011'
	and posting_flag ='0'
	and regexp_replace(to_date(update_time),'-','')>='20190902' 
	and regexp_replace(to_date(update_time),'-','')<='20190930'
group by warehouse_code ,
	product_code;


select
	a.product_code                    ,
	goodsname                         ,
	bar_code                          ,
	bd_id                             ,
	bd_name                           ,
	unit_name                         ,
	catg_l_id                         ,
	catg_l_name                       ,
	dept_id                           ,
	dept_name                         ,
	a.location_code                   ,
	a.shipper_code                    ,
	qc_price ,
	qc_qty   ,
	qc_amt   ,
	qm_price ,
	final_qty   ,
	qm_amt   ,
	enter_qty,
	enter_amt,
	out_qty,
	out_amt,
	loss_qty,
	loss_amt,
	stock_loss_qty,
	stock_loss_amt,
	stock_profit_qty,
	stock_profit_amt,
	stock_profit_qty-stock_loss_qty as diff_stock_qty,
	stock_profit_amt-stock_loss_amt as diff_stock_amt,
	inventory_qty_diff,
	inventory_amt_diff,
	is_self_product
from temp.p_wms_01 a
left join 
temp.p_wms_02 b 
on a.product_code=b.product_code
and a.location_code=b.location_code
left join 
temp.p_wms_03 c 
on a.product_code=c.product_code
and a.location_code=c.warehouse_code;


select
	*
from
	csx_ods.wms_inventory_product_detail_ods a
where
	sdt = '20191011'
	and posting_flag ='0'
	and regexp_replace(to_date(update_time),'-','')>='20190902' 
	and regexp_replace(to_date(update_time),'-','')<='20190930'
;
--select * from temp.p_wms_03 c 
--
--select * from temp.p_wms_02 where product_code='4534';
--
--select * from dw.catg_change_dtl_fct as ccdf as ds 
