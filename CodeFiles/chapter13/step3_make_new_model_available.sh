#! /bin/bash
isready=0

while [ $isready != 1 ]
do
   echo 'model state ' $isready
   ./call_model_ready_sql.sh
   isready=$(grep '1' model_status.output)
   echo 'model state ' $isready
   if [ $isready = 1 ]
   then echo 'model is ready' $isready 
   else echo 'getting ready to sleep' sleep 1800
   fi
done
echo 'creating new model ' 
./generate_show_model_sql.sh  'chapter7_regressionmodel.predict_ticket_price_auto_new'
./show_model.sh
aws s3 cp create_model.txt s3://<your-s3-bucket>>
./prep_final_create_model.sh
./generate_create_model_version_sql.sh
./execute_create_model_version.sh              
