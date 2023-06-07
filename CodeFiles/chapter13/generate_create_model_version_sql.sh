export PGPASSWORD=Awsuserxxx ;psql -h default.999999999999.us-east-1.redshift-serverless.amazonaws.com -p 5439 -U admin -d dev --output=model_version.txt -t  --file=get_model_version.sql
