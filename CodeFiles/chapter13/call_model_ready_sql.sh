export PGPASSWORD=Awsuser123 ;psql -h default.999999999999.us-east-1.redshift-serverless.amazonaws.com -p 5439 -U admin -d dev --output=model_status.output -t  --file=check_model_status.sql
