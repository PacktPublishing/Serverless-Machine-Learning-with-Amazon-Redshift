--Follow the instructions found here to create the Amazon
SageMaker model:
https://github.com/PacktPublishing/Serverless-Machine-Learning-with-Amazon-Redshift/blob/main/CodeFiles/Chapter11/xgboost_abalone.ipynb

 --For assistance on setting up the SageMaker notebook follow these instructions:
 https://docs.aws.amazon.com/sagemaker/latest/dg/gs-setup-working-env.html


#Create the schema
 Create schema chapter11_byom;


#Local Inference
#setup the paramaters
#provide your s3 bucket here
S3_BUCKET='Redshift ML s3 bucket name’


#provide model path, this is coming from model_data parameter 
MODEL_PATH=model_data
#Provide Redshift cluster attached role ARN
REDSHIFT_IAM_ROLE = 'Redshift Cluster IAM Role’


#Generate the CREATE MODEL Statement in Jupyter
Execute the code provided below in a Jupyter notebook to automatically generate the "Create Model" statement for you. sql_text=("drop model if exists predict_abalone_age; \
 CREATE MODEL chapter11_byom.predict_abalone_age \
FROM '{}' \
FUNCTION predict_abalone_age ( int, int, float, float,float,float,float,float,float) \
RETURNS int \
IAM_ROLE '{}' \
settings( S3_BUCKET '{}') \
")
print (sql_text.format(model_data,REDSHIFT_IAM_ROLE, S3_BUCKET))


#Create Model in QEV2

CREATE MODEL chapter11_byom.predict_abalone_age 
FROM 's3://redshift-ml-22-redshiftmlbucket-1cckvqgktpfe0/sagemaker/DEMO-xgboost-abalone-default/single-xgboost/DEMO-xgboost-regression-2022-12-31-01-45-30/output/model.tar.gz' 
FUNCTION predict_abalone_age ( int, int, float, float,float,float,float,float,float) RETURNS int IAM_ROLE 'arn:aws:iam::215830312345:role/spectrumrs' 
settings( S3_BUCKET 'redshift-ml-22-redshiftmlbucket-1cckvqgktpfe0') 


#Show Model
show model predict_abalone_age;


#Data preparation

drop table if exists chapter11_byom.abalone_test; 


create table chapter11_byom.abalone_test 
(Rings int, sex int,Length_ float, Diameter float, Height float, 
WholeWeight float, ShuckedWeight float,  VisceraWeight float,  ShellWeight float ); 
copy abalone_test 
from 's3://jumpstart-cache-prod-us-east-1/1p-notebooks-datasets/abalone/text-csv/test/' 
IAM_ROLE 'arn:aws:iam::212330312345:role/spectrumrs' 
csv ; 



#Call prediction function



Select original_age, predicted_age, original_age-predicted_age as Error 
From( 
select chapter11_byom.predict_abalone_age(Rings,sex, 
Length_ , 
Diameter ,  
Height , 
WholeWeight ,
ShuckedWeight ,  
VisceraWeight , 
ShellWeight ) predicted_age, rings as original_age 
from chapter11_byom.abalone_test ) a;


##Remote Inference 

#set up paramaters 
REDSHIFT_IAM_ROLE = 'Redshift Cluster IAM Role'
SAGEMAKER_ENDPOINT = rcf_inference.endpoint
  
## Generating BYOM Remote Inference command 
 
sql_text=("drop model if exists chapter11_byom.remote_random_cut_forest;\
CREATE MODEL chapter11_byom.remote_random_cut_forest\
 FUNCTION remote_fn_rcf (int)\
 RETURNS decimal(10,6)\
 SAGEMAKER'{}'\
 IAM_ROLE'{}'\
")
print(sql_text.format(SAGEMAKER_ENDPOINT,REDSHIFT_IAM_ROLE)) 

#Create remote inference model

  
CREATE MODEL chapter11_byom.remote_random_cut_forest 
FUNCTION remote_fn_rcf (int) RETURNS decimal(10,6) 
SAGEMAKER'randomcutforest-2022-12-31-03-48-13-259' 
IAM_ROLE'arn:aws:iam:: 215830312345:role/spectrumrs'
;
#Retrieve model metadata by running the show model command:
show model chapter11_byom.remote_random_cut_forest;


#Data preparation
COPY chapter11_byom.rcf_taxi_data
FROM 's3://sagemaker-sample-files/datasets/tabular/anomaly_benchmark_taxi/NAB_nyc_taxi.csv'
IAM_ROLE 'aRedshift Cluster IAM Role' ignoreheader 1 csv delimiter ',';


#Sample data

select * from chapter11_byom.rcf_taxi_data limit 10;


#Compute anomoly scores

select ride_timestamp, nbr_passengers, chapter11_byom.remote_fn_rcf(nbr_passengers) as score
from chapter11_byom.rcf_taxi_data;

#show scores > 3 std deviation points

with score_cutoff as
(select stddev(chapter11_byom.remote_fn_rcf(nbr_passengers)) as std, avg(chapter11_byom.remote_fn_rcf(nbr_passengers)) as mean, ( mean + 3 * std ) as score_cutoff_value
From chapter11_byom.rcf_taxi_data)


select ride_timestamp, nbr_passengers, chapter11_byom.remote_fn_rcf(nbr_passengers) as score
from chapter11_byom.3rcf_taxi_data
where score > (select score_cutoff_value from score_cutoff)
;













