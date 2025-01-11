use role accountadmin;

create or replace database snowpipe_demo;

create or replace table orders_data_lz(
order_id int,
product varchar(20),
quantity int, 
order_status varchar(30),
order_date date
);


--create a cloud storage integration in snowflake (creating config based secure access)
create or replace storage integration gcs_bucket_read_int
type =  external_stage
storage_provider = gcs 
enabled = true
storage_allowed_locations = ('gcs://snow_pipe_raw_data_test/');


--retrieve the cloud storage service Account for your snowflake account
desc storage integration gcs_bucket_read_int;


--service accont information for storage integration
-- k2fi00000@gcpuscentral1-1dfa.iam.gserviceaccount.com

--create stage
create or replace stage gcs_stage
url = 'gcs://snow_pipe_raw_data_test/'
storage_integration = gcs_bucket_read_int;

--to view stages 
show stages;

-- gsutil notification create -t snowpipe_pub_sub_topic -f json gs://snow_pipe_raw_data_test/


--create integration for notification with pubsub
create or replace notification integration notification_from_pubsub
type = queue
notification_provider = gcp_pubsub
enabled = true
gcp_pubsub_subscription_name = 'projects/august-water-444722-a3/subscriptions/snowpipe_pub_sub_topic-sub';

desc integration notification_from_pubsub;
-- k3fi00000@gcpuscentral1-1dfa.iam.gserviceaccount.com

create or replace pipe gcs_to_snowflake_pipe
auto_ingest = true
integration = notification_from_pubsub
as 
copy into orders_data_lz
from @gcs_stage
file_format = (type = 'CSV');

show pipes;

select system$pipe_status ('GCS_TO_SNOWFLAKE_PIPE');

select * from table (information_schema.copy_history(table_name=>'orders_data_lz', start_time=> dateadd(hours,-1,
current_timestamp())));

select * from orders_data_lz;

ALTER PIPE GCS_TO_SNOWFLAKE_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;

ALTER PIPE GCS_TO_SNOWFLAKE_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;


SELECT *
FROM INFORMATION_SCHEMA.PIPE_USAGE_HISTORY
WHERE pipe_name = 'GCS_TO_SNOWFLAKE_PIPE';






