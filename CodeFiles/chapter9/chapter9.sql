CREATE SCHEMA chapter9_deeplearning;

CREATE TABLE chapter9_deeplearning.robot_navigation (
     Id  bigint identity(0,1),
      US1 float,      US2 float,       US3 float,      US4 float,      US5 float,      US6 float,      US7 float,
      US8 float,      US9 float,      US10 float,      US11 float,      US12 float,      US13 float,   US14 float,
      US15 float,      US16 float,   US17 float,      US18 float,      US19 float,      US20 float,
      US21 float,      US22 float,      US23 float,      US24 float,    DIRECTION VARCHAR(256)
)
DISTSTYLE AUTO;

COPY chapter9_deeplearning.robot_navigation FROM 's3://packt-serverless-ml-redshift/chapter09/sensor_readings_24.data' 
IAM_ROLE  default
FORMAT AS CSV 
DELIMITER ',' 
QUOTE '"' 
REGION AS 'eu-west-1'
;


select * from
chapter9_deeplearning.robot_navigation
limit 10;

select direction, count(*)
from chapter9_deeplearning.robot_navigation
group by 1;


select direction, count(*)
from chapter9_deeplearning.robot_navigation
where mod(id,5) <> 0
group by 1;


select direction, count(*) from chapter9_deeplearning.robot_navigation
where mod(id,5) = 0
group by 1;


CREATE MODEL chapter9_deeplearning.predict_robot_direction
FROM 
 (select 
  US1,US2,US3,US4,
  US5,US6,US7,US8,
  US9,US10,US11,US12,
  US13,US14,US15,US16,
  US17,US18,US19,US20,
  US21,US22,US23,US24,
  DIRECTION
  FROM chapter9_deeplearning.robot_navigation 
  Where MOD(id,5) !=0)
TARGET DIRECTION	
FUNCTION predict_robot_direction_fn
IAM_ROLE default
MODEL_TYPE MLP
SETTINGS (s3_bucket '<<your-S3-bucket>>', 
MAX_RUNTIME 5400);

SHOW MODEL chapter9_deeplearning.predict_robot_direction;


-- prediction query for model accuracy

select correct, count(*)
from 
(select  DIRECTION as actual, chapter9_deeplearning.predict_robot_direction_fn (
US1,US2,US3,US4,US5,US6,US7,US8,US9,US10,US11,US12,
US13,US14,US15,US16,US17,US18,US19,US20,US21,US22,US23,US24
 ) as  predicted,
  CASE WHEN actual = predicted THEN 1::INT
         ELSE 0::INT END AS correct 
from chapter9_deeplearning.robot_navigation
where MOD(id,5) =0
) t1
group by 1;

-- prediction query to sample predicted direction

select  id, chapter9_deeplearning.predict_robot_direction_fn (
US1,US2,US3,US4,US5,US6,US7,US8,US9,US10,US11,US12,
US13,US14,US15,US16,US17,US18,US19,US20,US21,US22,US23,US24
 ) as  predicted_direction
from chapter9_deeplearning.robot_navigation
where MOD(id,5) = 0
limit 10;

-- prediction query to show count of predicted directions

select chapter9_deeplearning.predict_robot_direction_fn (
US1,US2,US3,US4,US5,US6,US7,US8,US9,US10,US11,US12,
US13,US14,US15,US16,US17,US18,US19,US20,US21,US22,US23,US24
 ) as  predicted_direction, count(*)
from chapter9_deeplearning.robot_navigation
where MOD(id,5) = 0
group by 1;



