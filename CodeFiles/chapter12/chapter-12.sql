-- Create schema
CREATE SCHEMA chapter12_forecasting;

---- Create table to load time series data
Create table chapter12_forecasting.web_retail_sales
(invoice_Date date, item_id varchar(500), quantity int);

---- load time series data
COPY chapter12_forecasting.web_retail_sales
FROM 's3://packt-serverless-ml-redshift/chapter12/web_retail_sales.csv' 
IAM_ROLE default
FORMAT AS CSV 
DELIMITER ',' 
IGNOREHEADER 1 
DATEFORMAT 'YYYY-MM-DD' 
REGION AS 'us-east-1';

--Verify loaded data
select * from web_retail_sales;

---Create Forecast Model
CREATE MODEL forecast_sales_demand  
FROM (select item_id, invoice_date, quantity from  chapter12_forecasting.web_retail_sales where invoice_date <  '2020-10-31')   
TARGET quantity   
IAM_ROLE default  
AUTO ON MODEL_TYPE FORECAST  
OBJECTIVE 'AverageWeightedQuantileLoss'  
SETTINGS (S3_BUCKET '<<bucket name>>',  
 HORIZON 5,  
 FREQUENCY 'D',  
 PERCENTILES '0.25,0.50,0.75,0.90,mean',  
 S3_GARBAGE_COLLECT OFF); 


--Check Model status
show model forecast_sales_demand;

---Create Model output table using CTAS
create table tbl_forecast_sales_demand as SELECT FORECAST(forecast_sales_demand);

--Verify the output
select * from chapter12_forecasting.tbl_forecast_sales_demand;



---Use below SQL query to check data predictions for one of the top selling products

select a.item_id as product,
  a.invoice_date,
  a.quantity as actual_quantity , 
  p90 as p90_forecast,
  p90 - a.quantity as p90_error,mean,
  p50 as p50_forecast
 from   chapter12_forecasting.web_retail_sales_clean_agg a
 inner join chapter12_forecasting.tbl_forecast_sales_demand b
 on upper(a.item_id) = upper(b.id) 
 and a.invoice_date = to_date(b.time, 'YYYY-MM-DD')
 AND a.item_id = 'JUMBO BAG RED RETROSPOT'
   where invoice_date > '2020-10-31'
order by 1,2; 

--iam policy for forecasting 

 {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "forecast:DescribeDataset",
                "forecast:DescribeDatasetGroup",
                "forecast:DescribeAutoPredictor",
                "forecast:CreateDatasetImportJob",
                "forecast:CreateForecast",
                "forecast:DescribeForecast",
                "forecast:DescribeForecastExportJob",
                "forecast:CreateMonitor",
                "forecast:CreateForecastExportJob",
                "forecast:CreateAutoPredictor",
                "forecast:DescribeDatasetImportJob",
                "forecast:CreateDatasetGroup",
                "forecast:CreateDataset",
                "forecast:TagResource",
                "forecast:UpdateDatasetGroup"
            ],
            "Resource": "*"
        } ,
		{
			"Effect": "Allow",
			"Action": [
				"iam:PassRole"
			],
			"Resource":"arn:aws:iam::<aws_account_id>:role/service-role/<Amazon_Redshift_cluster_iam_role_name>"
		}
    ]
}

 
