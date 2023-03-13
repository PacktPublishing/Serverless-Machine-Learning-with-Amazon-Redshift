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
DATEFORMAT 'MM/DD/YYYY' 
REGION AS 'us-east-1';

--Verify loaded data
select * from web_retail_sales;

---Create Forecast Model
CREATE MODEL forecast_sales_demand
FROM (select * from  web_retail_sales where invoice_date <= '2020-10-31') 
TARGET quantity 
IAM_ROLE default
AUTO ON MODEL_TYPE FORECAST
OBJECTIVE 'AverageWeightedQuantileLoss'
SETTINGS (S3_BUCKET 'sj-forecast-ml',
 HORIZON 5,
 FREQUENCY 'D',
 PERCENTILES '0.25,0.50,0.75,0.90,mean',
 S3_GARBAGE_COLLECT OFF); 

--Check Model status
show model forecast_sales_demand;

---Create Model output table using CTAS
create table tbl_forecast_sales_demand as SELECT FORECAST(forecast_sales_demand);

--Verify for one of the product
select * from tbl_forecast_sales_demand where upper(id) = 'WHITE HANGING HEART T-LIGHT HOLDER';

---Use below SQL query to check data predictions for the two most popular products
 select td.item_id as "Product", td.invoice_Date, td.quantity as "Actual_Quantity", psp.p90 as "P90 Forecast",
 psp.p90-td.quantity as "P90 Error", abs(td.quantity-psp.mean) as "Mean Absolute Error", power((td.quantity - psp.mean),2) as "Mean Squared Error",psp.p50 as "P50 Forecast"
 from web_retail_sales td, tbl_forecast_sales_demand psp 
 where td.invoice_date = date(psp."time") and td.item_id=upper(id)
 and upper(id) in ('WORLD WAR 2 GLIDERS ASSTD DESIGNS','WHITE HANGING HEART T-LIGHT HOLDER') order by 1,2; 
