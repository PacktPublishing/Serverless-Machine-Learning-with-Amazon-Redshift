CREATE SCHEMA chapter8_kmeans_clustering;

CREATE TABLE chapter8_kmeans_clustering.housing_prices (
    longitude decimal(10,2),
    latitude decimal(10,2),
    housing_median_age integer,
    total_rooms integer,
    total_bedrooms integer,
    population integer,
    households integer,
    median_income decimal(10,6),
    median_house_value integer,
    ocean_proximity character varying (50)
)
DISTSTYLE AUTO;


COPY chapter8_kmeans_clustering.housing_prices FROM 's3://packt-serverless-ml-redshift/chapter08/kmeans/housing_prices.csv'
IAM_ROLE default FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'eu-west-1';

select * from chapter8_kmeans_clustering.housing_prices limit 10;


CREATE model chapter8_kmeans_clustering.housing_segments_k2
FROM(Select
            median_income,
            latitude,
            longitude
From chapter8_kmeans_clustering.housing_prices)
FUNCTION  get_housing_segment_k2
IAM_ROLE default
AUTO OFF
MODEL_TYPE KMEANS
PREPROCESSORS '[
      {
        "ColumnSet": [ "median_income", "latitude","longitude" ],
        "Transformers": [ "StandardScaler" ]
      }
    ]'
HYPERPARAMETERS DEFAULT EXCEPT (K '2')

SETTINGS (S3_BUCKET '<your S3 Bucket>');



show MODEL chapter8_kmeans_clustering.housing_segments_k2;

 select chapter8_kmeans_clustering.get_housing_segment_k2 (median_income, latitude, longitude) as cluster, count(*) as count
  from chapter8_kmeans_clustering.housing_prices group by 1 order by 1;





CREATE model chapter8_kmeans_clustering.housing_segments_k3
FROM(Select
            median_income,
            latitude,
            longitude
From chapter8_kmeans_clustering.housing_prices)
FUNCTION  get_housing_segment_k3
IAM_ROLE default
AUTO OFF
MODEL_TYPE KMEANS
PREPROCESSORS '[
      {
        "ColumnSet": [ "median_income", "latitude","longitude" ],
        "Transformers": [ "StandardScaler" ]
      }
    ]'
HYPERPARAMETERS DEFAULT EXCEPT (K '3')

SETTINGS (S3_BUCKET '<your S3 Bucket>');



CREATE model chapter8_kmeans_clustering.housing_segments_k4
FROM(Select
            median_income,
            latitude,
            longitude
From chapter8_kmeans_clustering.housing_prices)
FUNCTION  get_housing_segment_k4
IAM_ROLE default
AUTO OFF
MODEL_TYPE KMEANS
PREPROCESSORS '[
      {
        "ColumnSet": [ "median_income", "latitude","longitude" ],
        "Transformers": [ "StandardScaler" ]
      }
    ]'
HYPERPARAMETERS DEFAULT EXCEPT (K '4')

SETTINGS (S3_BUCKET '<your S3 Bucket>');


CREATE model chapter8_kmeans_clustering.housing_segments_k5
FROM(Select
            median_income,
            latitude,
            longitude
From chapter8_kmeans_clustering.housing_prices)
FUNCTION  get_housing_segment_k5
IAM_ROLE default
AUTO OFF
MODEL_TYPE KMEANS
PREPROCESSORS '[
      {
        "ColumnSet": [ "median_income", "latitude","longitude" ],
        "Transformers": [ "StandardScaler" ]
      }
    ]'
HYPERPARAMETERS DEFAULT EXCEPT (K '5')

SETTINGS (S3_BUCKET '<your S3 Bucket>');


CREATE model chapter8_kmeans_clustering.housing_segments_k6
FROM(Select
            median_income,
            latitude,
            longitude
From chapter8_kmeans_clustering.housing_prices)
FUNCTION  get_housing_segment_k6
IAM_ROLE default
AUTO OFF
MODEL_TYPE KMEANS
PREPROCESSORS '[
      {
        "ColumnSet": [ "median_income", "latitude","longitude" ],
        "Transformers": [ "StandardScaler" ]
      }
    ]'
HYPERPARAMETERS DEFAULT EXCEPT (K '6')

SETTINGS (S3_BUCKET '<your S3 Bucket>');


SHOW MODEL chapter8_kmeans_clustering.housing_segments_k2;
SHOW MODEL chapter8_kmeans_clustering.housing_segments_k3;
SHOW MODEL chapter8_kmeans_clustering.housing_segments_k4;
SHOW MODEL chapter8_kmeans_clustering.housing_segments_k5;
SHOW MODEL chapter8_kmeans_clustering.housing_segments_k6;  


Select 2 as K, 1.088200  as MSD
Union
Select 3 as K,  0.775993 as MSD
Union
Select 4 as K,  0.532355as MSD
Union
Select 5 as K,  0.437294 as MSD
Union
Select 6 as K,  0.373781 as MSD;

select chapter8_kmeans_clustering.get_housing_segment_k3 (median_income, latitude, longitude) as cluster,
 count(*) as count from chapter8_kmeans_clustering.housing_prices group by 1 order by 1;

select avg(median_house_value) as avg_median_house_value,
chapter8_kmeans_clustering
.get_housing_segment_k3(median_income, latitude, longitude) as cluster
from chapter8_kmeans_clustering
.housing_prices
group by 2
order by 1;

select avg(median_income) as median_income,
chapter8_kmeans_clustering.get_housing_segment_k3(median_income, latitude, longitude) as cluster
from chapter8_kmeans_clustering.housing_prices
group by 2
order by 1;



