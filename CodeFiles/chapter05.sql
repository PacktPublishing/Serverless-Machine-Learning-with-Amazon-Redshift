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

COPY  chapter5_buildfirstmodel.customer_calls_fact
FROM 's3://packt-serverless-ml-redshift/chapter05/customerdime/'
IAM_ROLE default
delimiter ',' IGNOREHEADER 1
region 'eu-west-1';


-- Analyze customer churn segements
SELECT churn, count(*) Customer_Count FROM chapter5_buildfirstmodel.customer_calls_fact
GROUP BY churn
;

select sum(case when record_date <'2020-08-01' then 1 else 0
end) as Training_Data_Set,
sum(case when record_date >'2020-07-31' then 1 else 0 end) as
Test_Data_Set
from chapter5_buildfirstmodel.customer_calls_fact;


--replace <your account id> with you account number. Make sure s3 bucket is created before command is run

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
         WHERE record_date < '2020-08-01'
     )
TARGET churn
FUNCTION predict_customer_churn
IAM_ROLE default
SETTINGS (
  S3_BUCKET 'serverlessmachinelearningwithredshift-<your account
id>',
  MAX_RUNTIME 3600
)
;


--showing model status

SHOW MODEL chapter5_buildfirstmodel.customer_churn_model;

--check which predictions are not correct based existing labels. 

WITH infer_data AS (
 SELECT area_code ||phone accountid, replace(churn,'.','') as churn,
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
SELECT * FROM infer_data where churn!=predicted
;


--running predictions
SELECT area_code ||phone  accountid, replace(churn,'.','') as Actual_
churn_class,
    chapter5_buildfirstmodel.predict_customer_churn(
      state,account_length,area_code, phone,intl_plan,
      vMail_plan, vMail_message, day_mins, day_calls,
      day_charge, total_charge, eve_mins, eve_calls,
      eve_charge, night_mins, night_calls,
      night_charge, intl_mins, intl_calls, intl_charge,
      cust_serv_calls) AS predicted_class,
      chapter5_buildfirstmodel.predict_customer_churn_prob(
      state, account_length, area_code, phone, intl_plan,
      vMail_plan, vMail_message, day_mins, day_calls,
      day_charge, total_charge, eve_mins, eve_calls,
      eve_charge, night_mins, night_calls,night_charge,
      intl_mins, intl_calls, intl_charge, cust_serv_calls)
      AS probability_score
  FROM chapter5_buildfirstmodel.customer_calls_fact
WHERE record_date > '2020-07-31'
;

--comparing ground truth to predictions

WITH infer_data AS (
  SELECT area_code ||phone  ccounted,
    replace(churn,'.','') as churn,
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
Evaluating model performance 17
 
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
WHERE record_date > '2020-07-31'
)
SELECT *  FROM infer_data where churn!=predicted;


--feature importance
Select jsondata.featureimp.explanations.kernel_shap.label0.global_
shap_values as value
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_
model')as featureimp) jsondata ;

select t1.feature_imp, t1.value from 
(
Select 
'account_length' as feature_imp,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.account_length as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'area_code' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.area_code as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'cust_serv_calls' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.cust_serv_calls as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'day_calls' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.day_calls as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'day_charge' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.day_charge as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'day_mins' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.day_mins as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'eve_calls' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.eve_calls  as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'eve_charge' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.eve_charge  as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'eve_mins' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.eve_mins  as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'intl_calls' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.intl_calls   as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'intl_charge' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.intl_charge   as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'intl_mins' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.intl_mins    as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'intl_plan' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.intl_plan     as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'inight_calls' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.night_calls     as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'night_charge' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.night_charge    as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'night_mins' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.night_mins   as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'phone' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.phone   as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'state' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.state   as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'total_charge' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.total_charge   as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'vmail_message' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.vmail_message   as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 
union 
select 'vmail_plan' as feature_imp ,
jsondata.featureimp.explanations.kernel_shap.label0.global_shap_values.vmail_plan  as value 
from ( select explain_model( 'chapter5_buildfirstmodel.customer_churn_model')as featureimp) jsondata 

) t1
order by value desc;

--model performance

WITH infer_data AS (
  SELECT area_code ||phone  accountid, replace(churn,'.','') as churn,
    chapter5_buildfirstmodel.predict_customer_churn(
          state,  account_length, area_code, phone,
          intl_plan, vMail_plan, vMail_message, day_mins,
          day_calls,  day_charge, total_charge, eve_mins,
          eve_calls, eve_charge, night_mins, night_calls,
          night_charge, intl_mins, intl_calls,
          intl_charge,   cust_serv_calls) AS predicted
  FROM chapter5_buildfirstmodel.customer_calls_fact
WHERE record_date > '2020-07-31'),
confusionmatrix as
(
SELECT case when churn  ='True' and predicted = 'True' then 1 else 0
end TruePositives,
case when churn ='False' and predicted = 'False' then 1 else 0 end
TrueNegatives,
case when churn ='False' and predicted = 'True' then 1 else 0 end
FalsePositives,
case when churn ='True' and predicted = 'False' then 1 else 0 end
FalseNegatives
  FROM infer_data
 )
select
sum(TruePositives+TrueNegatives)*1.00/(count(*)*1.00) as Accuracy,
--accuracy of the model,
sum(FalsePositives+FalseNegatives)*1.00/count(*)*1.00 as Error_Rate,
--how often model is wrong,
sum(TruePositives)*1.00/sum (TruePositives+FalseNegatives) *1.00 as
True_Positive_Rate, --or recall how often corrects are rights,
sum(FalsePositives)*1.00/sum (FalsePositives+TrueNegatives )*1.00 
False_Positive_Rate, --or fall-out how often model said yes when it is no
sum(TrueNegatives)*1.00/sum (FalsePositives+TrueNegatives)*1.00 
True_Negative_Rate, 
--or specificity, how often model said no when it is yes
sum(TruePositives)*1.00 / (sum (TruePositives+FalsePositives)*1.00) as
Precision, -- when said yes how it is correct,
2*((True_Positive_Rate*Precision)/ (True_Positive_Rate+Precision) ) as
F_Score --weighted avg of TPR & FPR
From confusionmatrix
;

 



