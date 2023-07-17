Create schema chapter13;

create table chapter13.local_inf_ml_model_components
(model_name varchar(500),  
schema_name varchar(500),
automlJobName varchar(500),
functionName varchar(500),
inputs_data_type varchar(500),
target_column varchar(50),
returns_data_type varchar(50), 
model_arn varchar (500),
S3_Bucket varchar (200) );


-- Initialize local_inf_ml_components table  

insert into chapter13.local_inf_ml_model_components
values
(
'predict_ticket_price_auto',
'chapter7_regressionmodel',
' ',' ',' ',' ','float8',
'<arn of your IAM ROLE>'
'<your S3 Bucket>)';

