--Verify data loaded by Load data wizard using query editor v2
Select * from chapter2.customer limit 10;

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
FROM 's3://chapter2-load/lineitem.parquet'
IAM_ROLE default
FORMAT AS PARQUET;

-- Verify the data using below query
select count(1) from chapter2.lineitem;

select * from chapter2.lineitem limit 10;