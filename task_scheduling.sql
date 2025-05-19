-- change context
use role sysadmin;
use schema dev_db.stage_sch;
use warehouse adhoc_wh;

create or replace table completed_orders(
order_id int,
product varchar(20),
quantity int, 
order_status varchar(30),
order_date date
);


select * from completed_orders;

create or replace Task target_table_ingestion
warehouse = COMPUTE_WH
SCHEDULE = 'USING CRON */2 * * * * UTC' -- every 2 mins
as 
INSERT INTO completed_orders SELECT * FROM orders_data_lz WHERE order_status = 'Shipped';
ON_ERROR = ABORT_STATEMENT; 


alter task target_table_ingestion suspend; -- by default a task is automatically set to false

select * from table (information_schema.task_history(task_name=>'target_table_ingestion'))
order by scheduled_time;

create or replace Task target_table_ingestion_next
warehouse = COMPUTE_WH
after target_table_ingestion
as 
delete from  completed_orders WHERE ORDER_DATE < current_date();


use role accountadmin; -- to be able to manage tasks 
grant execute task, execute managed task on account to role sysadmin;
use role sysadmin;


alter task target_table_ingestion_next suspend; 




