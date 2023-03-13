--Serverless-Machine-Learning-with-Amazon-Redshift/Chapter05

-- create schema
Create schema chapter5_buildfirstmodel;

-- create table 
CREATE TABLE IF NOT EXISTS chapter5_buildfirstmodel.customer_calls_fact (
state varchar(2), 
account_length int, 
area_code int,
phone varchar(8), 
intl_plan varchar(3), 
vMail_plan varchar(3),
vMail_message int, 
day_mins float, 
day_calls int, 
day_charge float,
total_charge float,
eve_mins float, 
eve_calls int, 
eve_charge float, 
night_mins float,
night_calls int, 
night_charge float, 
intl_mins float, 
intl_calls int,
intl_charge float, 
cust_serv_calls int, 
churn varchar(6),
record_date date) 
Diststyle AUTO;

--load data using copy command.  Make sure 

COPY DEMO_ML.customer_activity 
FROM 's3://redshift-downloads/redshift-ml/customer_activity/' 
IAM_ROLE default
delimiter ',' IGNOREHEADER 1  
region 'us-east-1';


-- Analyze customer churn segements
SELECT churn, count(*) Customer_Count FROM chapter5_buildfirstmodel.customer_calls_fact 
GROUP BY churn
;

--replace <your account id> with you account number. Make sure s3 bucket is created before command in run 
CREATE MODEL chapter5_buildfirstmodel.customer_churn_model
FROM (SELECT state,
              account_length,
              area_code,
              phone,
              intl_plan,
              vMail_plan,
              vMail_message,
              day_mins,
              day_calls,
              day_charge,
              total_charge,
              eve_mins,
              eve_calls,
              eve_charge,
              night_mins,
              night_calls,
              night_charge,
              intl_mins,
              intl_calls,
              intl_charge,
              cust_serv_calls,
             replace(churn,'.','') as churn
      FROM chapter5_buildfirstmodel.customer_calls_fact 
         WHERE record_date < '2020-01-01' 

     )
TARGET churn
FUNCTION predict_customer_churn
IAM_ROLE default
SETTINGS (
  S3_BUCKET 'serverlessmachinelearningwithredshift-<your account id>',
  MAX_RUNTIME 1800
)
;

--showing model status

SHOW MODEL chapter5_buildfirstmodel.customer_churn_model;

--check which predictions are not correct based existing labels. 

WITH infer_data AS (
  SELECT area_code ||phone  accountid, replace(churn,'.','') as churn,
    chapter5_buildfirstmodel.predict_customer_churn( 
          state,
              account_length,
              area_code,
              phone,
              intl_plan,
              vMail_plan,
              vMail_message,
              day_mins,
              day_calls,
              day_charge,
              total_charge,
              eve_mins,
              eve_calls,
              eve_charge,
              night_mins,
              night_calls,
              night_charge,
              intl_mins,
              intl_calls,
              intl_charge,
              cust_serv_calls) AS predicted
  FROM chapter5_buildfirstmodel.customer_calls_fact
WHERE record_date > '2020-01-01'

)
SELECT *  FROM infer_data where churn!=predicted
;


--check accuracy of the model
WITH infer_data AS (
  SELECT area_code ||phone  accountid, replace(churn,'.','') as churn,
    chapter5_buildfirstmodel.predict_customer_churn( 
          state,
              account_length,
              area_code,
              phone,
              intl_plan,
              vMail_plan,
              vMail_message,
              day_mins,
              day_calls,
              day_charge,
              total_charge,
              eve_mins,
              eve_calls,
              eve_charge,
              night_mins,
              night_calls,
              night_charge,
              intl_mins,
              intl_calls,
              intl_charge,
              cust_serv_calls) AS predicted
  FROM chapter5_buildfirstmodel.customer_calls_fact
WHERE record_date > '2020-01-01'

)
SELECT *  FROM infer_data where churn!=predicted
;

--confusion matrix

WITH infer_data AS (
  SELECT area_code ||phone  accountid, replace(churn,'.','') as churn,
    chapter5_buildfirstmodel.predict_customer_churn( 
          state,
              account_length,
              area_code,
              phone,
              intl_plan,
              vMail_plan,
              vMail_message,
              day_mins,
              day_calls,
              day_charge,
              total_charge,
              eve_mins,
              eve_calls,
              eve_charge,
              night_mins,
              night_calls,
              night_charge,
              intl_mins,
              intl_calls,
              intl_charge,
              cust_serv_calls) AS predicted
  FROM chapter5_buildfirstmodel.customer_calls_fact
WHERE record_date > '2020-01-01'

),
confusionmatrix as 
(
SELECT case when churn  ='True' and predicted = 'True' then 1 else 0 end TruePositives,
case when churn ='False' and predicted = 'False' then 1 else 0 end TrueNegatives,
case when churn ='False' and predicted = 'True' then 1 else 0 end FalsePositives,
case when churn ='True' and predicted = 'False' then 1 else 0 end FalseNegatives
  FROM infer_data  
 )

select 
sum(TruePositives+TrueNegatives)*1.00/(count(*)*1.00) as Accuracy,--accuracy of the model, 

sum(FalsePositives+FalseNegatives)*1.00/count(*)*1.00 as Error_Rate, --how often model is wrong,

sum(TruePositives)*1.00/sum (TruePositives+FalseNegatives) *1.00 as True_Positive_Rate, --or recall how often corrects are rights,

sum(FalsePositives)*1.00/sum (FalsePositives+TrueNegatives )*1.00 False_Positive_Rate, --or fall-out how often model said yes when it is no,

sum(TrueNegatives)*1.00/sum (FalsePositives+TrueNegatives)*1.00 True_Negative_Rate, --or specificity, how often model said no when it is yes, 

sum(TruePositives)*1.00 / (sum (TruePositives+FalsePositives)*1.00) as Precision, -- when said yes how it is correct,

2*((True_Positive_Rate*Precision)/ (True_Positive_Rate+Precision) ) as F_Score --weighted avg of TPR & FPR

From confusionmatrix
;




