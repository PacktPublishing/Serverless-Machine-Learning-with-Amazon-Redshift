#! /bin/bash
export PGPASSWORD=Awsuserxxx ;psql -h default.999999999999.us-east-1.redshift-serverless.amazonaws.com -p 5439 -U admin -d dev --file=prep_create_model.sql
