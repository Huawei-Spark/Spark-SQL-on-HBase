## Example 3: Similar to example 2, but with larger sample file
In this example, we create a new SparkSQL table and map it to a new HBase table with multiple column in rowkey.

(1) Create table in SparkSQL and in HBase 
```
$SPARK_HBASE_HOME/bin/hbase-sql
CREATE TABLE sales1m(id STRING, product STRING, region STRING, sales INTEGER, quantity INTEGER, PRIMARY KEY (id, product, region)) MAPPED BY (hbase_sales1m, COLS=[sales=f.sales, quantity=f.quantity]);
CREATE TABLE sales1m_onekey(id STRING, product STRING, region STRING, sales INTEGER, quantity INTEGER, PRIMARY KEY (id)) MAPPED BY (hbase_sales1m_onekey, COLS=[product=f.product, region=f.region, sales=f.sales, quantity=f.quantity]);
```

(2) Load data :
```
LOAD DATA INPATH './examples/sales1m.csv' INTO TABLE sales1m FIELDS TERMINATED BY "," ;
LOAD DATA INPATH './examples/sales1m.csv' INTO TABLE sales1m_onekey FIELDS TERMINATED BY "," ;
```

(3) Query:
```
   // test count *
   (1) select count(*) from sales1m

   // test group by
   (2) select product, region, avg(sales) from sales1m where product="product4" group by product, region;
```
