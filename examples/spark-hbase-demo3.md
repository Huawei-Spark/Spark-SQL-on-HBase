## Create spark sql table map to existing hbase (only single column mapping to hbase rowkey is supported)
(1) Create table in hbase, populate data
```


(2) Map hbase table with sparksql table
```
CREATE TABLE sales1m(id STRING, product STRING, region STRING, sales INTEGER, quantity INTEGER, PRIMARY KEY (id, product, region)) MAPPED BY (hbase_sales1m, COLS=[sales=f.sales, quantity=f.quantity]);
CREATE TABLE sales1m_onekey(id STRING, product STRING, region STRING, sales INTEGER, quantity INTEGER, PRIMARY KEY (id)) MAPPED BY (hbase_sales1m_onekey, COLS=[product=f.product, region=f.region, sales=f.sales, quantity=f.quantity]);

CREATE TABLE sales10m(id STRING, product STRING, region STRING, sales INTEGER, quantity INTEGER, PRIMARY KEY (id, product, region)) MAPPED BY (hbase_sales10m, COLS=[sales=f.sales, quantity=f.quantity]);
CREATE TABLE sales10m_onekey(id STRING, product STRING, region STRING, sales INTEGER, quantity INTEGER, PRIMARY KEY (id)) MAPPED BY (hbase_sales10m_onekey, COLS=[product=f.product, region=f.region, sales=f.sales, quantity=f.quantity]);
```

(4) Load data :
```
LOAD DATA INPATH './examples/sales1m.csv' INTO TABLE sales1m FIELDS TERMINATED BY "," ;
LOAD DATA INPATH './examples/sales1m.csv' INTO TABLE sales1m_onekey FIELDS TERMINATED BY "," ;

LOAD DATA INPATH './examples/sales10m.csv' INTO TABLE sales10m FIELDS TERMINATED BY "," ;
LOAD DATA INPATH './examples/sales10m.csv' INTO TABLE sales10m_onekey FIELDS TERMINATED BY "," ;
```


(3) Query:
```
   // test count *
   (1) select count(*) from sales1m

   // test group by
   (2) select product, region, avg(sales) from sales1m where product="product4" group by product, region;
```
