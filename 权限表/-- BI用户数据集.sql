 -- BI用户数据集
 select id,
 employee_name,
    employee_code job_no,
    mobile  phone_no ,    
    case when  (emp_sub_name LIKE '永辉彩食鲜%'or emp_sub_name like '彩食鲜共享%' or emp_sub_name like '彩食鲜-富平%') then SUBSTRING_INDEX(SUBSTRING_INDEX(org_name,'_',2),'_',-1)
   		   when  emp_sub_name LIKE '%大区%' and   emp_sub_name regexp '[A-Z]' then substr(emp_sub_name,1,char_length (emp_sub_name )-2)
   		   when  emp_sub_name regexp '[\'\'(]'   then substring(emp_sub_name,1, char_length(emp_sub_name )-6)
   		   when  emp_sub_name regexp '[2-9]' then  REPLACE(substring(emp_sub_name,1, char_length(emp_sub_name )-4),'-','')
   		  else replace(emp_sub_name,'-','') end  org,
           case when   (emp_sub_name LIKE '永辉彩食鲜%'or emp_sub_name like '彩食鲜共享%' or emp_sub_name like '彩食鲜-富平%') then SUBSTRING_INDEX(SUBSTRING_INDEX(org_name,'_',2),'_',-1)
   		  when  SUBSTRING_INDEX(replace(replace(org_name,'永辉彩食鲜发展有限公司_大区_',''),'省区_',''),'_',3) regexp('部|人')
   		  then  SUBSTRING_INDEX(replace(replace(org_name,'永辉彩食鲜发展有限公司_大区_',''),'省区_',''),'_',2) 
   		  else SUBSTRING_INDEX(replace(replace(org_name,'永辉彩食鲜发展有限公司_大区_',''),'省区_',''),'_',3)
   		  end  org_short,
    case when parent_org_name regexp('^[0-Z]') then parent_org_name_3 else parent_org_name end  as title_name,      
    pos_position_name position_name,
    email,
	orgunit_code,
	parent_org_name,
    org_name,
    emp_sub_name
	from 
	(select DISTINCT ee.id,  employee_code ,employee_name ,cost_center ,emp_sub_name , mobile ,ee.email,pos_title_name,pos_title_id,
	orgunit_code,
          pos_position_name
from     csx_basic_data.erp_employee ee 
	where employee_status ='3'	
	and ee.card_type ='0'
	-- and ee.emp_sub_name LIKE '%彩食鲜%'
	 and ee.cost_center LIKE '%彩食鲜%'
	)erp 
	left join (
 select org_code,org_unit_name , parent_org_code,parent_org_name
 	 , parent_org_code_3,parent_org_name_3 
 	 ,parent_org_code_4,parent_org_name_4
 	 ,parent_org_name_5
 	 ,parent_org_name_6,
 	 parent_org_name_7,
 	 parent_org_name_8,
 	 parent_org_name_9,
 	 parent_org_name_10,
 	 parent_org_name_11,
 	 parent_org_name_12,
 	 parent_org_name_13,
 	 case when parent_org_name ='永辉彩食鲜发展有限公司'  then  concat(parent_org_name,'_',org_unit_name)
	 	when parent_org_name_3 ='永辉彩食鲜发展有限公司'  then  concat(parent_org_name_3,'_',parent_org_name,'_',org_unit_name)
 	 	when parent_org_name_4 ='永辉彩食鲜发展有限公司'   then concat(parent_org_name_4 ,'_',parent_org_name_3,'_',parent_org_name,'_',org_unit_name)
 	 	when parent_org_name_5 ='永辉彩食鲜发展有限公司'   then concat(parent_org_name_5 ,'_',parent_org_name_4 ,'_',parent_org_name_3,'_',parent_org_name,'_',org_unit_name)
 	 	when parent_org_name_6 ='永辉彩食鲜发展有限公司'   then concat(parent_org_name_6 ,'_',parent_org_name_5 ,'_',parent_org_name_4 ,'_',parent_org_name_3,'_',parent_org_name,'_',org_unit_name)
 	 	when parent_org_name_7 ='永辉彩食鲜发展有限公司'   then concat(parent_org_name_7 ,'_',parent_org_name_6 ,'_',parent_org_name_5 ,'_',parent_org_name_4 ,'_',parent_org_name_3,'_',parent_org_name,'_',org_unit_name)
 	 	when parent_org_name_8 ='永辉彩食鲜发展有限公司'   then concat(parent_org_name_8 ,'_',parent_org_name_7 ,'_',parent_org_name_6 ,'_',parent_org_name_5 ,'_',parent_org_name_4 ,'_',parent_org_name_3,'_',parent_org_name,'_',org_unit_name)
 	 	when parent_org_name_9 ='永辉彩食鲜发展有限公司'   then concat(parent_org_name_9 ,'_',parent_org_name_8 ,'_',parent_org_name_7 ,'_',parent_org_name_6 ,'_',parent_org_name_5 ,'_',parent_org_name_4 ,'_',parent_org_name_3,'_',parent_org_name,'_',org_unit_name)
 	 	when parent_org_name_10 ='永辉彩食鲜发展有限公司'   then concat(parent_org_name_10 ,'_',parent_org_name_9 ,'_',parent_org_name_8 ,'_',parent_org_name_7 ,'_',parent_org_name_6 ,'_',parent_org_name_5 ,'_',parent_org_name_4 ,'_',parent_org_name_3,'_',parent_org_name,'_',org_unit_name)
 	 	when parent_org_name_11 ='永辉彩食鲜发展有限公司'   then concat(parent_org_name_11 ,'_',parent_org_name_10 ,'_',parent_org_name_9 ,'_',parent_org_name_8 ,'_',parent_org_name_7 ,'_',parent_org_name_6 ,'_',parent_org_name_5 ,'_',parent_org_name_4 ,'_',parent_org_name_3,'_',parent_org_name,'_',org_unit_name) 
 	 when parent_org_name_12 ='永辉彩食鲜发展有限公司'   then concat(parent_org_name_12 ,'_',parent_org_name_11 ,'_',parent_org_name_10 ,'_',parent_org_name_9 ,'_',parent_org_name_8 ,'_',parent_org_name_7 ,'_',parent_org_name_6 ,'_',parent_org_name_5 ,'_',parent_org_name_4 ,'_',parent_org_name_3,'_',parent_org_name,'_',org_unit_name)
 	 when parent_org_name_13 ='永辉彩食鲜发展有限公司'   then concat(parent_org_name_13 ,'_',parent_org_name_12 ,'_',parent_org_name_11 ,'_',parent_org_name_10 ,'_',parent_org_name_9 ,'_',parent_org_name_8 ,'_',parent_org_name_7 ,'_',parent_org_name_6 ,'_',parent_org_name_5 ,'_',parent_org_name_4 ,'_',parent_org_name_3,'_',parent_org_name,'_',org_unit_name)
 	 	end org_name
  from 
(
select a.id ,a.org_unit_code as org_code,a.org_unit_name,eo.parent_code  as parent_org_code ,eo.org_unit_name  parent_org_name   from 
(select  id ,org_unit_code,org_unit_name  ,parent_code  
    from csx_basic_data.erp_organization  eo
    where   status=1 
        and end_date='99991231')a 
left join 
(select  id ,org_unit_code,org_unit_name  ,parent_code     
    from csx_basic_data.erp_organization  
    where  status=1 
        and end_date='99991231') eo on  a.parent_code =eo.org_unit_code
 ) x 
 left join 
 (select    id ,org_unit_code,org_unit_name  parent_org_name_3,parent_code  as parent_org_code_3 
 from csx_basic_data.erp_organization ) eo on  x.parent_org_code =eo.org_unit_code
  left join 
 (select id ,org_unit_code,org_unit_name parent_org_name_4 ,parent_code ,parent_code as parent_org_code_4
 from csx_basic_data.erp_organization ) b on  eo.parent_org_code_3 =b.org_unit_code
   left join 
 (select   id ,org_unit_code,org_unit_name parent_org_name_5 ,parent_code ,parent_code as parent_org_code_5
 from csx_basic_data.erp_organization ) c on  b.parent_org_code_4 =c.org_unit_code
  left join 
 (select   id ,org_unit_code,org_unit_name parent_org_name_6 ,parent_code ,parent_code as parent_org_code_6
 from csx_basic_data.erp_organization ) d on  c.parent_org_code_5 =d.org_unit_code
   left join 
 (select   id ,org_unit_code,org_unit_name parent_org_name_7 ,parent_code ,parent_code as parent_org_code_7
 from csx_basic_data.erp_organization ) j on  d.parent_org_code_6 =j.org_unit_code
  left join 
 (select   id ,org_unit_code,org_unit_name parent_org_name_8 ,parent_code ,parent_code as parent_org_code_8
 from csx_basic_data.erp_organization ) h on  j.parent_org_code_7 =h.org_unit_code
 left join 
 (select   id ,org_unit_code,org_unit_name parent_org_name_9 ,parent_code ,parent_code as parent_org_code_9
 from csx_basic_data.erp_organization ) k on  h.parent_org_code_8 =k.org_unit_code
  left join 
 (select  id ,org_unit_code,org_unit_name parent_org_name_10 ,parent_code ,parent_code as parent_org_code_10
 from csx_basic_data.erp_organization ) p on  k.parent_org_code_9 =p.org_unit_code
  left join 
 (select  id ,org_unit_code,org_unit_name parent_org_name_11 ,parent_code ,parent_code as parent_org_code_11
 from csx_basic_data.erp_organization ) e on  p.parent_org_code_10 =e.org_unit_code
  left join 
 (select   id ,org_unit_code,org_unit_name parent_org_name_12 ,parent_code ,parent_code as parent_org_code_12
 from csx_basic_data.erp_organization ) g on  e.parent_org_code_11 =g.org_unit_code
  left join 
 (select   id ,org_unit_code,org_unit_name parent_org_name_13 ,parent_code ,parent_code as parent_org_code_13
 from  csx_basic_data.erp_organization   ) l on  g.parent_org_code_12 =l.org_unit_code
 )org on org.org_code =erp.orgunit_code
  where org_code  is not null 
  	 and orgunit_code !='15046965'
     and  pos_title_name != '残疾人岗'
   