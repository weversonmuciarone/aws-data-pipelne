select distinct 
lower(concat(loa_workday_id,'-',coalesce(loa_absence_type,''),'-',coalesce(date_format(substring(loa_start_date,0,10),'yyyyMMdd'),''),'-',coalesce(date_format(substring(replace(loa_approval_date,'T',' '),0,19),'yyyyMMddHHmmss'),''))) as hudi_key,
coalesce(loa_workday_id,'') as employee_id, 
"full_time" as employee_type,
"absence" as leave_type,
coalesce(date_format(to_timestamp(substring(loa_start_date,0,10)) ,'yyyy-MM-dd HH:mm:ss'),'') as start_date,
case when loa_end_date is null then ''
          else date_format(to_timestamp(substring(loa_end_date,0,10)) ,'yyyy-MM-dd HH:mm:ss')
          end
          as end_date,
'' as duration_type,
cast(0.0 as double) as duration,
coalesce(loa_absence_type,'') as absence_reason,
coalesce( date_format(to_timestamp(substring(loa_rescinded_date,0,10)) ,'yyyy-MM-dd'),'') as rescinded_date,
cast(0 as int) as employee_payroll_number,
'' as action,
'' as absence_id,
coalesce(substring(replace(loa_approval_date,'T',' '),0,23),'') as approval_date,
cast(source_system_id as int) as source_system_id,
dh_load_ts as dh_load_ts,
effective_moment
from {{cleansed_db}}{{stage_}}.employee_leave_of_absence