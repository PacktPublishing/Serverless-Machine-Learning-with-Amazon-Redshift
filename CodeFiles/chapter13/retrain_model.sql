DROP MODEL chapter7_RegressionMOdel.predict_ticket_price_auto;
CREATE MODEL chapter7_RegressionMOdel.predict_ticket_price_auto from
chapter7_RegressionModel.sporting_event_ticket_info_training
TARGET final_ticket_price
FUNCTION predict_ticket_price_auto
IAM_ROLE default
PROBLEM_TYPE regression
OBJECTIVE 'mse'
SETTINGS (s3_bucket '<your s3 bucket>',
s3_garbage_collect off,
max_runtime 9600);