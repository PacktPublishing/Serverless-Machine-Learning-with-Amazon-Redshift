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
./create_new_model.sh
