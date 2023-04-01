create schema chapter7_RegressionModel;

--create table to load data
DROP TABLE chapter7_RegressionModel.height_weight;
CREATE TABLE chapter7_RegressionModel.height_weight
(
    Id integer,
  	HeightInches decimal(9,2),
    weightPounds decimal(9,2)
)
;


COPY chapter7_RegressionModel.height_weight
FROM 's3://packt-serverless-ml-redshift/chapter07/heightweight/HeightWeight.csv'
IAM_ROLE default
CSV
IGNOREHEADER 1
REGION AS 'eu-west-1';

SELECT * FROM
chapter7_RegressionModel.height_weight
ORDER BY 2,3;

DROP  MODEL chapter7_RegressionModel.predict_weight;
CREATE MODEL chapter7_RegressionModel.predict_weight
FROM (select heightinches, cast(round(weightpounds,0) as integer) weightpounds from chapter7_RegressionModel.height_weight where id%8!=0 )
TARGET weightpounds
FUNCTION predict_weight
IAM_ROLE default
MODEL_TYPE xgboost
PROBLEM_TYPE regression
OBJECTIVE 'mse'
SETTINGS (s3_bucket '<<your-S3-bucket>>',
          s3_garbage_collect off,
          max_runtime 3600);

  SHOW MODEL chapter7_RegressionModel.predict_weight;

  SELECT heightinches, CAST(chapter7_RegressionModel.predict_weight(CAST(ROUND(heightinches,0) as integer)) as INTEGER) as Predicted_Weightpounds, 
  CAST(ROUND(weightpounds,0) as INTEGER) Original_Weightpounds ,
  Predicted_Weightpounds - Original_Weightpounds  as Difference
  FROM chapter7_RegressionModel.height_weight WHERE id%8=0
  AND Predicted_Weightpounds - Original_Weightpounds = 0;


  -- multi-input regression models


CREATE TABLE chapter7_RegressionModel.sporting_event_ticket_info (
ticket_id double precision ,
event_id bigint,
sport character varying(500),
event_date_time timestamp without time zone,
home_team character varying(500),
away_team character varying(500),
location character varying(500),
city character varying(500),
seat_level bigint,
seat_section bigint,
seat_row character varying(500),
seat bigint ENCODE az64,
list_ticket_price double precision,
final_ticket_price double precision ,
ticketholder character varying(500)
)
DISTSTYLE AUTO;

COPY chapter7_RegressionModel.sporting_event_ticket_info 
FROM 's3://packt-serverless-ml-redshift/chapter07/ticketinfo' IAM_ROLE default FORMAT AS CSV DELIMITER ',' QUOTE '"' 
REGION AS 'eu-west-1';


Select extract(month from event_date_time) as month, 
sum(cast (final_ticket_price as decimal(8,2))) as ticket_revenue
From chapter7_RegressionModel.sporting_event_ticket_info
where event_date_time < '2019-10-27'
group by 1
order by 1;

CREATE TABLE chapter7_RegressionModel.sporting_event_ticket_info_training (
    ticket_id double precision ,
    event_id bigint,
    sport character varying(500),
    event_date_time timestamp without time zone,
    home_team character varying(500),
    away_team character varying(500),
    location character varying(500),
    city character varying(500),
    seat_level bigint,
    seat_section bigint,
    seat_row character varying(500),
    seat bigint ENCODE az64,
    list_ticket_price double precision,
    final_ticket_price double precision ,
    ticketholder character varying(500)
)
DISTSTYLE AUTO;

--insert ~70% of data into training_set

insert into   chapter7_RegressionModel.sporting_event_ticket_info_training
(  ticket_id ,event_id ,sport , event_date_time,  home_team , away_team , location , city , seat_level, seat_section,  
    seat_row ,  seat, list_ticket_price, final_ticket_price, ticketholder )
 select  
 ticket_id ,event_id ,sport , event_date_time,  home_team , away_team , location , city , seat_level, seat_section,  
    seat_row ,  seat, list_ticket_price, final_ticket_price, ticketholder
 from chapter7_RegressionModel.sporting_event_ticket_info
 where event_date_time < '2019-10-20';

 
CREATE TABLE chapter7_RegressionModel.sporting_event_ticket_info_validation (
    ticket_id double precision ,
    event_id bigint,
    sport character varying(500),
    event_date_time timestamp without time zone,
    home_team character varying(500),
    away_team character varying(500),
    location character varying(500),
    city character varying(500),
    seat_level bigint,
    seat_section bigint,
    seat_row character varying(500),
    seat bigint ENCODE az64,
    list_ticket_price double precision,
    final_ticket_price double precision ,
    ticketholder character varying(500)
)
DISTSTYLE AUTO;

-- insert ~10% of data into validation set
  
insert into  chapter7_RegressionModel.sporting_event_ticket_info_validation
(  ticket_id ,event_id ,sport , event_date_time,  home_team , away_team , location , city , seat_level, seat_section,  
    seat_row ,  seat, list_ticket_price, final_ticket_price, ticketholder )
 select  
 ticket_id ,event_id ,sport , event_date_time,  home_team , away_team , location , city , seat_level, seat_section,  
    seat_row ,  seat, list_ticket_price, final_ticket_price, ticketholder
 from chapter7_RegressionModel.sporting_event_ticket_info
 where event_date_time between '2019-10-20' and '2019-10-22' ;
 
 

CREATE TABLE chapter7_RegressionModel.sporting_event_ticket_info_testing (
    ticket_id double precision ,
    event_id bigint,
    sport character varying(500),
    event_date_time timestamp without time zone,
    home_team character varying(500),
    away_team character varying(500),
    location character varying(500),
    city character varying(500),
    seat_level bigint,
    seat_section bigint,
    seat_row character varying(500),
    seat bigint ENCODE az64,
    list_ticket_price double precision,
    final_ticket_price double precision ,
    ticketholder character varying(500)
)
DISTSTYLE AUTO;

 -- insert ~20% of data into test set
insert into   chapter7_RegressionModel.sporting_event_ticket_info_testing 
(  ticket_id ,event_id ,sport , event_date_time,  home_team , away_team , location , city , seat_level, seat_section,  
    seat_row ,  seat, list_ticket_price, final_ticket_price, ticketholder )
select  
 ticket_id ,event_id ,sport , event_date_time,  home_team , away_team , location , city , seat_level, seat_section,  
    seat_row ,  seat, list_ticket_price, final_ticket_price, ticketholder
 from chapter7_RegressionModel.sporting_event_ticket_info
 where event_date_time >  '2019-10-22'  
 ;

-- create model

CREATE MODEL chapter7_RegressionModel.predict_ticket_price_linlearn from
chapter7_RegressionModel.sporting_event_ticket_info_training
TARGET final_ticket_price
FUNCTION predict_ticket_price_linlearn
IAM_ROLE default
MODEL_TYPE LINEAR_LEARNER
PROBLEM_TYPE regression
OBJECTIVE 'mse'
SETTINGS (s3_bucket '<<your-S3-Bucket>>',
s3_garbage_collect off,
max_runtime 9600);


SELECT
      ROUND(AVG(POWER(( actual_price_revenue - predicted_price_revenue ),2)),2) mse
    , ROUND(SQRT(AVG(POWER(( actual_price_revenue - predicted_price_revenue ),2))),2) rmse
FROM  
    (select home_team, chapter7_RegressionModel.predict_ticket_price_linlearn (ticket_id, event_id, sport, event_date_time, home_team, away_team,
Location, city, seat_level, seat_section, seat_row, seat,
list_ticket_price ,ticketholder ) as predicted_price_revenue,
 final_ticket_price  as actual_price_revenue
From chapter7_RegressionModel.sporting_event_ticket_info_validation 
     );  


Select home_team, 
sum(cast(chapter7_RegressionModel.predict_ticket_price_linlearn (ticket_id, event_id, sport, 
event_date_time, home_team, away_team,
Location, city, seat_level, seat_section, seat_row, seat,
list_ticket_price ,ticketholder ) as decimal(8,2) )) as predicted_price_revenue,
sum(cast (final_ticket_price as decimal(8,2))) as actual_price_revenue,
(predicted_price_revenue - actual_price_revenue) as diff,
abs((predicted_price_revenue - actual_price_revenue)/actual_price_revenue) * 100  as pct_diff
From chapter7_RegressionModel.sporting_event_ticket_info_validation
group by 1  
order by 5 desc ; 


CREATE MODEL Chapter7_RegressionModel.predict_ticket_price_auto
from
chapter7_RegressionModel.sporting_event_ticket_info_training
TARGET final_ticket_price
FUNCTION predict_ticket_price_auto
IAM_ROLE default
PROBLEM_TYPE regression
OBJECTIVE 'mse'
SETTINGS (s3_bucket '<<your-S3-bucket>>',
s3_garbage_collect off,
max_runtime 9600);


Show model Chapter7_RegressionModel.predict_ticket_price_auto;


SELECT
      ROUND(AVG(POWER(( actual_price_revenue - predicted_price_revenue ),2)),2) mse
    , ROUND(SQRT(AVG(POWER(( actual_price_revenue - predicted_price_revenue ),2))),2) rmse
FROM  
    (select home_team, chapter7_RegressionModel.predict_ticket_price_auto (ticket_id, event_id, sport, event_date_time, home_team, away_team,
Location, city, seat_level, seat_section, seat_row, seat,
list_ticket_price ,ticketholder ) as predicted_price_revenue,
 final_ticket_price  as actual_price_revenue
From chapter7_RegressionModel.sporting_event_ticket_info_validation
     );   



Select home_team, 
sum(cast(chapter7_RegressionModel.predict_ticket_price_auto (ticket_id, event_id, sport, event_date_time, home_team, away_team,
Location, city, seat_level, seat_section, seat_row, seat,
list_ticket_price ,ticketholder ) as decimal(8,2) )) as predicted_price_revenue,
sum(cast (final_ticket_price as decimal(8,2))) as actual_price_revenue,
(predicted_price_revenue - actual_price_revenue) as diff,
((predicted_price_revenue - actual_price_revenue)/actual_price_revenue) * 100  as pct_diff
From chapter7_RegressionModel.sporting_event_ticket_info_validation
group by 1  
order by 5 desc ;


select json_table.report.explanations.kernel_shap.label0.global_shap_values from
 (select explain_model('predict_ticket_price_auto') as report) as json_table;

select t1.home_team, predicted_price_revenue
from
(Select home_team,
sum(cast(chapter7_RegressionModel.predict_ticket_price_auto (ticket_id, event_id, sport, event_date_time, home_team, away_team,
Location, city, seat_level, seat_section, seat_row, seat,
list_ticket_price ,ticketholder ) as decimal (8,2) ) ) as predicted_price_revenue
From chapter7_RegressionModel.sporting_event_ticket_info_testing
group by 1) t1
where predicted_price_revenue < 200000; 



        
