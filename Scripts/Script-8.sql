select phone_no,employee_name,email,position_name,position_code ,org,ee.title_id ,ee.title_name 
from csx_data_market.temp_employee ee
left join csx_b2b_data_center.usr_user uu on ee.job_no =uu.user_work_no 
and uu.is_able='1'
where employee_name like '陈化勇'