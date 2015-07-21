## Example 2: Create and query SparkSQL table map to existing Hbase table
In this example, we create SparkSQL table and map it to a existing HBase table. (a single column map to hbase rowkey)

Steps:
(1) Create table and populate data in HBase shell
```
$HBase_Home/bin/hbase shell
create 'hbase10k', 'f'
for i in '1'..'10000' do for j in '1'..'2' do put 'hbase10k', "row#{i}", "f:c#{j}", "#{i}#{j}" end end
```   

(2) Map hbase table with sparksql table in hbase-sql shell
```
$SPARK_HBASE_Home/bin/hbase-sql
CREATE TABLE spark10k(rowkey STRING, a INTEGER, b INTEGER, PRIMARY KEY (rowkey)) MAPPED BY (hbase10k, COLS=[a=f.c1, b=f.c2]);
```

(3) Query:
```
   // test count *
   (1) select count(*) from spark10k

   // test group by
   (2) select avg(a), b from spark10k group by b
```
