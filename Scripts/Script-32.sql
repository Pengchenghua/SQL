select * from csx_b2b_crm.customer where customer_number='104644';
select * from csx_b2b_crm.customer_sales_log where date_format(update_time,'%Y-%m-%d')  ='2019-08-01' and customer_number='104644';