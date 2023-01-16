DROP MODEL chapter6_supervisedclassification.banking_termdeposit;
CREATE MODEL chapter6_supervisedclassification.banking_termdeposit
FROM (
SELECT    
   age ,
   job ,
   marital ,
   education ,
   "default" ,
   housing ,
   loan ,
   contact ,
   month ,
   day_of_week ,
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
  S3_BUCKET 'serverlessmachinelearningwithredshift-709512860261',
  MAX_RUNTIME 3600
  )
;
