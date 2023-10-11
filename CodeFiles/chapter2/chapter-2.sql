
CREATE SCHEMA CHAPTER 2;

CREATE TABLE chapter2.supplier (
s_suppkey integer NOT NULL ENCODE raw distkey,
s_name character(25) NOT NULL ENCODE lzo,
s_address character varying(40) NOT NULL ENCODE lzo,
s_nationkey integer NOT NULL ENCODE az64,
s_phone character(15) NOT NULL ENCODE lzo,
s_acctbal numeric(12, 2) NOT NULL ENCODE az64,
s_comment character varying(101) NOT NULL ENCODE lzo,
PRIMARY KEY (s_suppkey)
) DISTSTYLE KEY
SORTKEY (s_suppkey);


--Verify data loaded by Load data wizard using query editor v2
Select * from chapter2.supplier limit 10;



--Section COPY command 
-- Create lineitem table
CREATE TABLE chapter2.lineitem
(l_orderkey     bigint,
l_partkey       bigint,
l_suppkey       integer,
l_linenumber    integer,
l_quantity      numeric(12,2),
l_extendedprice numeric(12,2),
l_discount      numeric(12,2),
l_tax           numeric(12,2),
l_returnflag    character(1),
l_linestatus    character(1),
l_shipdate      date,
l_commitdate    date,
l_receiptdate   date,
l_shipinstruct  character(25),
l_shipmode      character(10),
l_comment       varchar(44))
distkey(l_orderkey) compound sortkey(l_orderkey,l_shipdate);

--Load data using COPY command

COPY chapter2.lineitem
FROM 's3://packt-serverless-ml-redshift/chapter02/lineitem.parquet/'
IAM_ROLE default
REGION AS 'eu-west-1'
FORMAT AS PARQUET;



-- Verify the data using below query
select count(1) from chapter2.lineitem;

select * from chapter2.lineitem limit 10;


--Data api

aws redshift-data execute-statement
--WorkgroupName redshift-workgroup-name
--database dev
--sql 'select * from redshift_table';

--Juypyter Notebook

import boto3
import time
import pandas as pd
import numpy as np
session = boto3.session.Session()
region = session.region_name
REDSHIFT_WORKGROUP = '<workgroup name>'
S3_DATA_FILE='s3://packt-serverless-ml-redshift/chapter2/orders.parquet'


table_ddl = """
DROP TABLE IF EXISTS chapter2.orders CASCADE;
CREATE TABLE chapter2.orders
(o_orderkey bigint NOT NULL,
o_custkey bigint NOT NULL encode az64,
o_orderstatus character(1) NOT NULL encode lzo,
o_totalprice numeric(12,2) NOT NULL encode az64,
o_orderdate date NOT NULL,
o_orderpriority character(15) NOT NULL encode lzo,
o_clerk character(15) NOT NULL encode lzo,
o_shippriority integer NOT NULL encode az64,
o_comment character varying(79) NOT NULL encode lzo
)
distkey(o_orderkey) compound sortkey(o_orderkey,o_orderdate);"""


client = boto3.client("redshift-data")
res = client.execute_statement(Database='dev', Sql=table_
ddl, WorkgroupName=REDSHIFT_
WORKGROUP)

query_id = res["Id"]
print(query_id)

load_data = f"""COPY chapter2.orders
FROM '{S3_DATA_FILE}'
IAM_ROLE default
FORMAT AS PARQUET;"""


res = client.execute_statement(Database='dev', Sql=load_data,
WorkgroupName=REDSHIFT_
WORKGROUP)
query_id = res["Id"]
print(query_id)


cnt = client.execute_statement(Database='dev', Sql='Select
count(1) from chapter2.orders ;', WorkgroupName=REDSHIFT_
WORKGROUP)
query_id = cnt["Id"]


