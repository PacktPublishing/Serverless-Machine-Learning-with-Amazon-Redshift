select count(*)  from svv_ml_model_info
where model_name = 'predict_ticket_price_auto_new'
and model_state like '%Ready%';
