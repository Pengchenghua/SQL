-- CONNECTION: name=Hadoop - HIVE


--销售员数据
drop table b2b_tmp.p_cust_sale;
CREATE TEMPORARY TABLE b2b_tmp.p_cust_sale
as 
select  qdflag,a.dist ,sales_user_name,work_no,sales_user_id,sales_supervisor_name,sales_supervisor_work_no,
sum(xse_7)xse_7,sum(mle_7)mle_7,
sum(xse )sale,sum(mle )mle ,COUNT (DISTINCT a.cust_id )cust_cn,sum(pc)pc,MIN (a.sdt )min_sdt
from 
(
select sdt, qdflag ,dist ,regexp_replace(cust_id,'(^0*)','')cust_id ,
	cust_name,
	case when sdt>='20190701' and sdt <='20190731' then xse end xse_7,
	case when sdt>='20190701' and sdt <='20190731' then mle end mle_7,
	xse ,mle ,
	case when xse !=0 then 1 end pc 
	from csx_dw.sale_warzone02_detail_dtl  WHERE   sdt>='20190101' and sdt<='20190731'
)a
left JOIN 
csx_ods.b2b_customer_new b on a.cust_id=b.cust_id 
left JOIN 
(select
		customer_number ,
		b.customer_name ,
		regexp_replace(to_date(b.sign_time),'-','') sign_time,written_amount,sdt,sales_user_id ,
		sales_user_name,sales_supervisor_name,work_no,sales_supervisor_work_no
	from
		csx_dw.customer_simple_info_v2 b
	 JOIN 
	(SELECT sales_id,sales_supervisor_name,work_no,sales_supervisor_work_no from csx_dw.sale_org_m  WHERE sdt='20190731' and POSITION ='SALES') c
	on cast(b.sales_user_id as string)=sales_id
		and sdt ='20190731' )c
		on a.cust_id=c.customer_number
group by qdflag,a.dist ,sales_user_name,work_no,sales_user_id,sales_supervisor_work_no,sales_supervisor_name;



select '1' qt,qdflag,dist,sales_user_name,work_no,xse_7,mle_7,	sale,	mle,	cust_cn,	pc,	min_sdt from b2b_tmp.p_cust_sale
union all
select '2' qt,qdflag,dist,sales_supervisor_name sales_user_name,sales_supervisor_work_no work_no,
sum(xse_7)xse_7,sum(mle_7)mle_7,sum(sale)sale,sum(mle)mle,sum(cust_cn)cust_cn,sum(pc)pc,min(min_sdt)min_sdt from b2b_tmp.p_cust_sale
group by qdflag,dist,sales_supervisor_name,sales_supervisor_work_no

;
