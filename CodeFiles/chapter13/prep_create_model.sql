drop table chapter13.model_info;

CREATE TABLE chapter13.model_info (
    key character varying(5000) ENCODE lzo,
    value character varying(40000) ENCODE lzo
)
DISTSTYLE AUTO;

copy  chapter13.model_info from 's3://<<your-s3-bucket>>/create_model.txt' iam_role default format as csv delimiter '|' quote '"' blanksasnull emptyasnull fillrecord maxerror 50 region as 'us-east-1';



drop table chapter13.create_model_sql;


CREATE TABLE chapter13.create_model_sql (
    sql_stmnt character varying(50000) ENCODE lzo
)
DISTSTYLE AUTO;


update chapter13.local_inf_ml_model_components
set schema_name = (
select value from chapter13.model_info
where key like '%Schema Name%'),
automljobname = (
select value from chapter13.model_info
where key like '%AutoML Job Name%'),
functionname = (select value||'_'||replace ( replace (replace(getdate(),' ',''),'-',''),':','')   from chapter13.model_info
where key like '%Function Name%'),
query = (select value from chapter13.model_info
where key like '%Query%'),
target_column = (select value from chapter13.model_info
where key like '%Target Column%'),
inputs_data_type = (select trim (both ',' from replace (value, ' ',',') ) from chapter13.model_info
where key like '%Function Parameter Types%');

truncate chapter13.create_model_sql;

insert into chapter13.create_model_sql
select 'create model '||schema_name||'.'|| trim (both from model_name) ||
' from '|| quote_literal(trim (both from automljobname)) ||
' FUNCTION '|| functionname||' (' || inputs_data_type || ')'
|| ' RETURNS ' || returns_data_type ||
' IAM_ROLE '|| quote_literal(trim (both from model_arn))  || ' ' ||
'SETTINGS ( S3_BUCKET ' || quote_literal(trim (both from S3_BUCKET)) ||');'
from chapter13.local_inf_ml_model_components as sql_stmnt;
