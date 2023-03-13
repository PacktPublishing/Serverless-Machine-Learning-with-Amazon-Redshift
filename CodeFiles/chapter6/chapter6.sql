
Create schema chapter6_supervisedclassification;

CREATE TABLE chapter6_supervisedclassification.bank_details_training(
   age numeric,
   "job" varchar,
   marital varchar,
   education varchar,
   "default" varchar,
   housing varchar,
   loan varchar,
   contact varchar,
   month varchar,
   day_of_week varchar,
   duration numeric,
   campaign numeric,
   pdays numeric,
   previous numeric,
   poutcome varchar,
   emp_var_rate numeric,
   cons_price_idx numeric,     
   cons_conf_idx numeric,     
   euribor3m numeric,
   nr_employed numeric,
   y boolean ) ;

--create table to store data for running predictions

CREATE TABLE chapter6_supervisedclassification.bank_details_inference(
   age numeric,
   "job" varchar,
   marital varchar,
   education varchar,
   "default" varchar,
   housing varchar,
   loan varchar,
   contact varchar,
   month varchar,
   day_of_week varchar,
   duration numeric,
   campaign numeric,
   pdays numeric,
   previous numeric,
   poutcome varchar,
   emp_var_rate numeric,
   cons_price_idx numeric,     
   cons_conf_idx numeric,     
   euribor3m numeric,
   nr_employed numeric,
   y boolean ) ;

--load data into  bank_details_inference
TRUNCATE chapter6_supervisedclassification.bank_details_inference;   

 COPY chapter6_supervisedclassification.bank_details_inference 
 from 's3://packt-serverless-ml-redshift/chapter06/bank-marketing-data/inference-data/inference.csv' 
 REGION 'eu-west-1' IAM_ROLE default CSV IGNOREHEADER 1 delimiter ';';

-- load data into bank_details_training
TRUNCATE chapter6_supervisedclassification.bank_details_training;
 COPY chapter6_supervisedclassification.bank_details_training 
 from 's3://packt-serverless-ml-redshift/chapter06/bank-marketing-data/training-data/training.csv'  
 REGION 'eu-west-1' IAM_ROLE default CSV IGNOREHEADER 1 delimiter ';';

 SELECT y, COUNT(*) Customer_Count FROM chapter6_supervisedclassification.bank_details_training
GROUP BY y;

DROP MODEL chapter6_supervisedclassification.banking_termdeposit;

CREATE  MODEL chapter6_supervisedclassification.banking_termdeposit 
FROM (
SELECT    
   age ,
   "job" ,
   marital ,
   education ,
   "default" ,
   housing ,
   loan ,
   contact ,
   month ,
   day_of_week ,
   duration,
   campaign ,
   pdays ,
   previous ,
   poutcome ,
   emp_var_rate ,
   cons_price_idx ,     
   cons_conf_idx ,     
   euribor3m ,
   nr_employed ,
   y
FROM
    chapter6_supervisedclassification.bank_details_training )
    TARGET y
FUNCTION predict_term_deposit
IAM_ROLE default
MODEL_TYPE XGBoost
PROBLEM_TYPE BINARY_CLASSIFICATION
SETTINGS (
  S3_BUCKET '<<your-S3-Bucket',
  MAX_RUNTIME 9600
  )
;



show MODEL chapter6_supervisedclassification.banking_termdeposit;

WITH infer_data
 AS (
    SELECT  y as actual, chapter6_supervisedclassification.predict_term_deposit(
   age ,   "job" ,   marital ,   education ,   "default" ,   housing ,   loan ,   contact ,   month ,   day_of_week ,   duration ,   campaign ,   pdays ,   previous ,   poutcome ,   emp_var_rate ,   cons_price_idx ,        cons_conf_idx ,        euribor3m ,   nr_employed  
) AS predicted,
     CASE WHEN actual = predicted THEN 1::INT
         ELSE 0::INT END AS correct
    FROM chapter6_supervisedclassification.bank_details_training
    ),
 aggr_data AS (
     SELECT SUM(correct) as num_correct, COUNT(*) as total FROM infer_data
 )
 SELECT (num_correct::float/total::float) AS accuracy FROM aggr_data;

WITH term_data AS ( SELECT chapter6_supervisedclassification.predict_term_deposit( age,"job" ,marital,education,"default",housing,loan,contact,month,day_of_week,duration,campaign,pdays,previous,poutcome,emp_var_rate,cons_price_idx,cons_conf_idx,euribor3m,nr_employed) AS predicted
FROM chapter6_supervisedclassification.bank_details_inference )
SELECT
CASE WHEN predicted = 'Y'  THEN 'Yes-will-do-a-term-deposit'
     WHEN predicted = 'N'  THEN 'No-term-deposit'
     ELSE 'Neither' END as deposit_prediction,
COUNT(1) AS count
from term_data GROUP BY 1;


SELECT
age,"job" ,marital ,
chapter6_supervisedclassification.predict_term_deposit_prob( age,"job" ,marital,education,"default",housing,loan,contact,month,day_of_week,duration,campaign,pdays,previous,poutcome,emp_var_rate,cons_price_idx,cons_conf_idx,euribor3m,nr_employed) AS predicted
FROM chapter6_supervisedclassification.bank_details_inference 
where marital = 'married'
  and "job" = 'management'
  and age between 35 and 40;


SELECT age, "job", marital, predicted.labels[0], predicted.probabilities[0]
from
 (select
age,"job" ,marital ,
chapter6_supervisedclassification.predict_term_deposit_prob( age,"job" ,marital,education,"default",housing,loan,contact,month,day_of_week,duration,campaign,pdays,previous,poutcome,emp_var_rate,cons_price_idx,cons_conf_idx,euribor3m,nr_employed) AS predicted
FROM chapter6_supervisedclassification.bank_details_inference 
where marital = 'married'
  and "job" = 'management'
  and age between 35 and 40) t1
where predicted.labels[0] = 't';

select json_table.report.explanations.kernel_shap.label0.global_shap_values from
 (select explain_model('chapter6_supervisedclassification.banking_termdeposit') as report) as json_table;


--multi-class classification

CREATE TABLE chapter6_supervisedclassification.cust_segmentation_train (
    id numeric,
    gender varchar,  
    ever_married  varchar, 
    age numeric, 
    graduated varchar,  
    profession varchar, 
    work_experience numeric, 
    spending_score  varchar, 
    family_size numeric,
    var_1 varchar,
    segmentation varchar
)
DISTSTYLE AUTO;	

COPY chapter6_supervisedclassification.cust_segmentation_train
 FROM 's3://packt-serverless-ml-redshift/chapter06/Train.csv' 
 IAM_ROLE DEFAULT FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'eu-west-1';

CREATE TABLE chapter6_supervisedclassification.cust_segmentation_test (
    id numeric,
    gender varchar,  
    ever_married  varchar, 
    age numeric, 
    graduated varchar,  
    profession varchar, 
    work_experience numeric, 
    spending_score  varchar, 
    family_size numeric,
    var_1 varchar
)
DISTSTYLE AUTO;	 


COPY chapter6_supervisedclassification.cust_segmentation_test
 FROM 's3://packt-serverless-ml-redshift/chapter06/Test.csv' 
 IAM_ROLE DEFAULT FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'eu-west-1';

select segmentation, count(*)  from chapter6_supervisedclassification.cust_segmentation_train
group by 1;

CREATE  MODEL chapter6_supervisedclassification.cust_segmentation_model_ll
FROM (
SELECT
    id, gender, ever_married, age, graduated,profession,  
    work_experience, spending_score,family_size,  
    var_1,segmentation
FROM chapter6_supervisedclassification.cust_segmentation_train
)
TARGET segmentation
FUNCTION predict_cust_segment_ll   IAM_ROLE default
MODEL_TYPE LINEAR_LEARNER
PROBLEM_TYPE MULTICLASS_CLASSIFICATION
OBJECTIVE 'accuracy'
SETTINGS (
  S3_BUCKET '<<your-s3-bucket>>',
  S3_GARBAGE_COLLECT OFF,
  MAX_RUNTIME 9600
  );

SHOW MODEL chapter6_supervisedclassification.cust_segmentation_model_ll;

select 
 cast(sum(t1.match)as decimal(7,2)) as predicted_matches
,cast(sum(t1.nonmatch) as decimal(7,2)) as predicted_non_matches
,cast(sum(t1.match + t1.nonmatch) as decimal(7,2))  as total_predictions
,predicted_matches / total_predictions as pct_accuracy
from 
(SELECT
    id, 
    gender,  
    ever_married,   
    age,   
    graduated, 
    profession,  
    work_experience,  
    spending_score,  
    family_size,  
    var_1,  
    segmentation as actual_segmentation,
    chapter6_supervisedclassification.predict_cust_segment_ll
(id,gender,ever_married,age,graduated,profession,work_experience,
spending_score,family_size,var_1) as predicted_segmentation,
    case when actual_segmentation = predicted_segmentation then 1
      else 0 end as match,
  case when actual_segmentation <> predicted_segmentation then 1
    else 0 end as nonmatch 
    
    FROM chapter6_supervisedclassification.cust_segmentation_train
) t1;

SELECT
id, 
chapter6_supervisedclassification.predict_cust_segment_ll
(id,gender,ever_married,age,graduated,profession,work_experience,spending_score,family_size,var_1) as  segmentation 
FROM chapter6_supervisedclassification.cust_segmentation_test;

SELECT 
    chapter6_supervisedclassification.predict_cust_segment_ll
    (id,gender,ever_married,age,graduated,profession,work_experience,spending_score,family_size,var_1) as  segmentation,
    count(*) 
    FROM chapter6_supervisedclassification.cust_segmentation_test 
   group by 1;


CREATE MODEL chapter6_supervisedclassification.cust_segmentation_model
FROM (
SELECT
    id, 
    gender,  
    ever_married,   
    age,   
    graduated, 
    profession,  
    work_experience,  
    spending_score,  
    family_size,  
    var_1,  
    segmentation
FROM chapter6_supervisedclassification.cust_segmentation_train
)
TARGET segmentation
FUNCTION predict_cust_segment  IAM_ROLE default
SETTINGS (
  S3_BUCKET '<<your S3 Bucket>>',
  S3_GARBAGE_COLLECT OFF,
  MAX_RUNTIME 9600
);


CREATE MODEL chapter6_supervisedclassification.cust_segmentation_model
FROM (
SELECT
    id, 
    gender,  
    ever_married,   
    age,   
    graduated, 
    profession,  
    work_experience,  
    spending_score,  
    family_size,  
    var_1,  
    segmentation
FROM chapter6_supervisedclassification.cust_segmentation_train
)
TARGET segmentation
FUNCTION predict_cust_segment  IAM_ROLE default
PROBLEM_TYPE MULTICLASS_CLASSIFICATION
OBJECTIVE 'accuracy'
SETTINGS (
  S3_BUCKET '<<your S3 Bucket>>',
  S3_GARBAGE_COLLECT OFF,
  MAX_RUNTIME 9600
  );
