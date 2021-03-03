
select * from data_center_report.account_age_dtl_fct_new where sdt='20210103';
select * from csx_b2b_data_center.usr_user uu ;
select * from csx_data_market.ads_bbc_s_d_customer_summary absdcs ;

select phone_no,employee_name,email,position_name,position_code ,org,ee.title_id ,ee.title_name 
from csx_data_market.temp_employee ee
left join csx_b2b_data_center.usr_user uu on ee.job_no =uu.user_work_no 
and uu.is_able='1';

select * from FINE_RECORD_EXECUTE fre ;

select version();