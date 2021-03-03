select * from csx_crm_report.report_all_channel_statistics;
select id,customer_number,customer_name,sales_province,sales_user_id,sales_user_name,region_province_name,update_by ,update_time
from csx_b2b_crm.customer where customer_number 
in ('103715','103715','104350','102130','102130','104779','104981','104981','104090','104350','104779','104350','104350','104981','104779','104779');