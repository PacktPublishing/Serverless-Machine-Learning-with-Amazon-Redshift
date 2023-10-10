Create Schema chapter10_XGBoost;

Create table chapter10_XGBoost.cust_payment_tx_history
(
TRANSACTION_ID integer,
TX_DATETIME timestamp,
CUSTOMER_ID integer,
TERMINAL_ID integer,
TX_AMOUNT decimal(9,2),
TX_TIME_SECONDS integer,
TX_TIME_DAYS integer,
TX_FRAUD integer,
TX_FRAUD_SCENARIO integer,
TX_DURING_WEEKEND integer,
TX_DURING_NIGHT integer,
CUSTOMER_ID_NB_TX_1DAY_WINDOW decimal(9,2),
CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW decimal(9,2),
CUSTOMER_ID_NB_TX_7DAY_WINDOW decimal(9,2),
CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW decimal(9,2),
CUSTOMER_ID_NB_TX_30DAY_WINDOW decimal(9,2),
CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW decimal(9,2),
TERMINAL_ID_NB_TX_1DAY_WINDOW decimal(9,2),
TERMINAL_ID_RISK_1DAY_WINDOW decimal(9,2),
TERMINAL_ID_NB_TX_7DAY_WINDOW decimal(9,2),
TERMINAL_ID_RISK_7DAY_WINDOW decimal(9,2),
TERMINAL_ID_NB_TX_30DAY_WINDOW decimal(9,2),
TERMINAL_ID_RISK_30DAY_WINDOW decimal(9,2)
)
;
copy chapter10_xgboost.cust_payment_tx_history
FROM 's3://packt-serverless-ml-redshift/chapter10/credit_card_transactions_transformed_balanced.csv'
iam_role default
ignoreheader 1
csv region 'eu-west-1';

select * from
chapter10_XGBoost.cust_payment_tx_history
limit 10;

select tx_fraud, count(*)
from chapter10_XGBoost.cust_payment_tx_history
group by 1;

select to_char(tx_datetime, 'YYYYMM') as YearMonth,
sum(case when tx_fraud = 1 then 1 else 0 end) fraud_tx,
sum(case when tx_fraud = 0 then 1 else 0 end) non_fraud_tx,
count(*) as total_tx
FROM chapter10_XGBoost.cust_payment_tx_history
group by YearMonth;

create view chapter10_XGBoost.credit_payment_tx_history_scaled
as
select 
TRANSACTION_ID, TX_DATETIME, CUSTOMER_ID, TERMINAL_ID, 
TX_AMOUNT , 
( (TX_AMOUNT - avg(TX_AMOUNT) over()) /  cast(stddev_pop(TX_AMOUNT) over() as dec(14,2)) ) S_TX_AMOUNT,
TX_TIME_SECONDS ,
  ( (TX_TIME_SECONDS - avg(TX_TIME_SECONDS) over()) /  cast(stddev_pop(TX_TIME_SECONDS) over() as dec(14,2)) ) S_TX_TIME_SECONDS,
TX_TIME_DAYS  ,
  ( (TX_TIME_DAYS - avg(TX_TIME_DAYS) over()) /  cast(stddev_pop(TX_TIME_DAYS) over() as dec(14,2)) ) S_TX_TIME_DAYS,
TX_FRAUD  ,
  TX_DURING_WEEKEND ,
CASE WHEN TX_DURING_WEEKEND = 1 THEN 1 ELSE 0 END AS TX_DURING_WEEKEND_IND, 
CASE WHEN TX_DURING_WEEKEND = 0 THEN 1 ELSE 0 END TX_DURING_WEEKDAY_IND,
TX_DURING_NIGHT,
CASE WHEN TX_DURING_NIGHT = 1 THEN 1 ELSE 0 END AS TX_DURING_NIGHT_IND, 
CASE WHEN TX_DURING_NIGHT = 0 THEN 1 ELSE 0 END AS TX_DURING_DAY_IND,
CUSTOMER_ID_NB_TX_1DAY_WINDOW ,
  ( (CUSTOMER_ID_NB_TX_1DAY_WINDOW - avg(CUSTOMER_ID_NB_TX_1DAY_WINDOW) over()) /  cast(stddev_pop(CUSTOMER_ID_NB_TX_1DAY_WINDOW) over() as dec(14,2)) ) S_CUSTOMER_ID_NB_TX_1DAY_WINDOW,
CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW  ,
  ( (CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW - avg(CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW) over()) /  cast(stddev_pop(CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW) over() as dec(14,2)) ) S_CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW,
CUSTOMER_ID_NB_TX_7DAY_WINDOW ,
  ( (CUSTOMER_ID_NB_TX_7DAY_WINDOW - avg(CUSTOMER_ID_NB_TX_7DAY_WINDOW) over()) /  cast(stddev_pop(CUSTOMER_ID_NB_TX_7DAY_WINDOW) over() as dec(14,2)) ) S_CUSTOMER_ID_NB_TX_7DAY_WINDOW,
CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW  ,
  ( (CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW - avg(CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW) over()) /  cast(stddev_pop(CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW) over() as dec(14,2)) ) S_CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW,
CUSTOMER_ID_NB_TX_30DAY_WINDOW  ,
  ( (CUSTOMER_ID_NB_TX_30DAY_WINDOW - avg(CUSTOMER_ID_NB_TX_30DAY_WINDOW) over()) /  cast(stddev_pop(CUSTOMER_ID_NB_TX_30DAY_WINDOW) over() as dec(14,2)) ) S_CUSTOMER_ID_NB_TX_30DAY_WINDOW,
CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW ,
  ( (CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW - avg(CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW) over()) /  cast(stddev_pop(CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW) over() as dec(14,2)) ) S_CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW,
TERMINAL_ID_NB_TX_1DAY_WINDOW ,
  ( (TERMINAL_ID_NB_TX_1DAY_WINDOW - avg(TERMINAL_ID_NB_TX_1DAY_WINDOW) over()) /  cast(stddev_pop(TERMINAL_ID_NB_TX_1DAY_WINDOW) over() as dec(14,2)) ) S_TERMINAL_ID_NB_TX_1DAY_WINDOW,
TERMINAL_ID_RISK_1DAY_WINDOW  ,
  ( (TERMINAL_ID_RISK_1DAY_WINDOW - avg(TERMINAL_ID_RISK_1DAY_WINDOW) over()) /  cast(stddev_pop(TERMINAL_ID_RISK_1DAY_WINDOW) over() as dec(14,2)) ) S_TERMINAL_ID_RISK_1DAY_WINDOW,
TERMINAL_ID_NB_TX_7DAY_WINDOW ,
  ( (TERMINAL_ID_NB_TX_7DAY_WINDOW - avg(TERMINAL_ID_NB_TX_7DAY_WINDOW) over()) /  cast(stddev_pop(TERMINAL_ID_NB_TX_7DAY_WINDOW) over() as dec(14,2)) ) S_TERMINAL_ID_NB_TX_7DAY_WINDOW,
TERMINAL_ID_RISK_7DAY_WINDOW  ,
  ( (TERMINAL_ID_RISK_7DAY_WINDOW - avg(TERMINAL_ID_RISK_7DAY_WINDOW) over()) /  cast(stddev_pop(TERMINAL_ID_RISK_7DAY_WINDOW) over() as dec(14,2)) ) S_TERMINAL_ID_RISK_7DAY_WINDOW,
TERMINAL_ID_NB_TX_30DAY_WINDOW  ,
  ( (TERMINAL_ID_NB_TX_30DAY_WINDOW - avg(TERMINAL_ID_NB_TX_30DAY_WINDOW) over()) /  cast(stddev_pop(TERMINAL_ID_NB_TX_30DAY_WINDOW) over() as dec(14,2)) ) S_TERMINAL_ID_NB_TX_30DAY_WINDOW,
TERMINAL_ID_RISK_30DAY_WINDOW ,
  ( (TERMINAL_ID_RISK_30DAY_WINDOW - avg(TERMINAL_ID_RISK_30DAY_WINDOW) over()) /  cast(stddev_pop(TERMINAL_ID_RISK_30DAY_WINDOW) over() as dec(14,2)) ) S_TERMINAL_ID_RISK_30DAY_WINDOW
from
chapter10_XGBoost.cust_payment_tx_history;


SELECT * from chapter10_XGBoost.credit_payment_tx_history_scaled limit 10;

drop model chapter10_XGBoost.cust_cc_txn_fd_xg ;                                                
                                                
CREATE MODEL chapter10_xgboost.cust_cc_txn_fd_xg
FROM (
SELECT
s_tx_amount,
tx_fraud,
tx_during_weekend_ind,
tx_during_weekday_ind,
tx_during_night_ind,
tx_during_day_ind,
s_customer_id_nb_tx_1day_window,
s_customer_id_avg_amount_1day_window,
s_customer_id_nb_tx_7day_window,
s_customer_id_avg_amount_7day_window,
s_customer_id_nb_tx_30day_window,
s_customer_id_avg_amount_30day_window,
s_terminal_id_nb_tx_1day_window,
s_terminal_id_risk_1day_window,
s_terminal_id_nb_tx_7day_window,
s_terminal_id_risk_7day_window,
s_terminal_id_nb_tx_30day_window,
s_terminal_id_risk_30day_window
  from chapter10_xgboost.payment_tx_history_scaled
  WHERE cast(tx_datetime as date) between '2022-06-01' and '2022-09-30' 
)
TARGET tx_fraud
FUNCTION fn_customer_cc_fd_xg
IAM_ROLE default
AUTO OFF
MODEL_TYPE XGBOOST
OBJECTIVE 'binary:logistic'
PREPROCESSORS 'none'
HYPERPARAMETERS DEFAULT EXCEPT (NUM_ROUND '100')
SETTINGS (
S3_BUCKET '<<your-S3-bucket>>',
 s3_garbage_collect off,
 max_runtime 1500
);


 

SHOW MODEL chapter10_XGBoost.cust_cc_txn_fd_xg;


SELECT
tx_fraud ,
fn_customer_cc_fd_xg(
s_tx_amount,
tx_during_weekend_ind,
tx_during_weekday_ind,
tx_during_night_ind,
tx_during_day_ind,
s_customer_id_nb_tx_1day_window,
s_customer_id_avg_amount_1day_window,
s_customer_id_nb_tx_7day_window,
s_customer_id_avg_amount_7day_window,
s_customer_id_nb_tx_30day_window,
s_customer_id_avg_amount_30day_window,
s_terminal_id_nb_tx_1day_window,
s_terminal_id_risk_1day_window,
s_terminal_id_nb_tx_7day_window,
s_terminal_id_risk_7day_window,
s_terminal_id_nb_tx_30day_window,
s_terminal_id_risk_30day_window)
FROM chapter10_XGBoost.credit_payment_tx_history_scaled
WHERE cast(tx_datetime as date) >= '2022-10-01'
;


--drop view if exists chapter10_XGBoost.fraud_tx_conf_matrix;
Create or replace view chapter10_XGBoost.fraud_tx_conf_matrix
as
SELECT 
TRANSACTION_ID,TX_DATETIME,CUSTOMER_ID,TX_AMOUNT,TERMINAL_ID, tx_fraud,
  fn_customer_cc_fd_xg(                                              
  s_tx_amount,
tx_during_weekend_ind,
tx_during_weekday_ind,
tx_during_night_ind,
tx_during_day_ind,
s_customer_id_nb_tx_1day_window,
s_customer_id_avg_amount_1day_window,
s_customer_id_nb_tx_7day_window,
s_customer_id_avg_amount_7day_window,
s_customer_id_nb_tx_30day_window,
s_customer_id_avg_amount_30day_window,
s_terminal_id_nb_tx_1day_window,
s_terminal_id_risk_1day_window,
s_terminal_id_nb_tx_7day_window,
s_terminal_id_risk_7day_window,
s_terminal_id_nb_tx_30day_window,
s_terminal_id_risk_30day_window) 
AS Prediction,
case when tx_fraud  =1 and Prediction = 1 then 1 else 0 end TruePositives,
case when tx_fraud =0 and Prediction = 0 then 1 else 0 end TrueNegatives,
case when tx_fraud =0 and Prediction = 1 then 1 else 0 end FalsePositives,
case when tx_fraud =1 and Prediction = 0 then 1 else 0 end FalseNegatives
  FROM chapter10_XGBoost.credit_payment_tx_history_scaled
  WHERE cast(tx_datetime as date) >= '2022-10-01';

Select
sum(TruePositives+TrueNegatives)*1.00/(count(*)*1.00) as Accuracy,--accuracy of the model,
sum(FalsePositives+FalseNegatives)*1.00/count(*)*1.00 as Error_Rate, --how often model is wrong,
sum(TruePositives)*1.00/sum (TruePositives+FalseNegatives) *1.00 as TPR, --or recall how often corrects are rights,
sum(FalsePositives)*1.00/sum (FalsePositives+TrueNegatives )*1.00 FPR, --or fall-out how often model said yes when it is no,
sum(TrueNegatives)*1.00/sum (FalsePositives+TrueNegatives)*1.00 TNR, --or specificity, how often model said no when it is yes,
sum(TruePositives)*1.00 / (sum (TruePositives+FalsePositives)*1.00) as Precision, -- when said yes how it is correct,
2*((TPR*Precision)/ (TPR+Precision) ) as F_Score --weighted avg of TPR & FPR
From chapter10_xgboost.fraud_tx_conf_matrix
;


select tx_fraud,Prediction, count(*)
from chapter10_XGBoost.fraud_tx_conf_matrix
group by tx_fraud,Prediction;

