drop model chapter10_XGBoost.cust_cc_txn_fd_xg ;                                                
                                                
 CREATE MODEL chapter10_XGBoost.cust_cc_txn_fd_xg
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
  from chapter10_XGBoost.payment_tx_history_scaled
  WHERE cast(tx_datetime as date) between '2022-06-01' and '2022-09-30' 
)
TARGET tx_fraud
FUNCTION fn_customer_cc_fd_xg
IAM_ROLE default
AUTO OFF
MODEL_TYPE XGBoost
OBJECTIVE 'binary:logistic'
PREPROCESSORS 'none'
HYPERPARAMETERS DEFAULT EXCEPT (NUM_ROUND '100')
SETTINGS (
  S3_BUCKET 'ant312b-redshifts3bucket-xxiuz0x7euhi',
            s3_garbage_collect off,
            max_runtime 1500
                   );
