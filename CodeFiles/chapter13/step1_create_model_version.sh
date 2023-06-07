#! /bin/bash
./generate_show_model_sql.sh  'chapter7_regressionmodel.predict_ticket_price_auto'
./show_model.sh
./prep_create_model.sh
./generate_create_model_version_sql.sh
./execute_create_model_version.sh                                         
