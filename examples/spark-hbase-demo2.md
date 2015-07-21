## Example 2: Create and query SparkSQL table map to existing Hbase table
In this example, we create SparkSQL table and map it to a existing HBase table. (a single column map to hbase rowkey)

Steps:
(1) Create table and populate data in HBase shell
```
$HBase_Home/bin/hbase shell
create 'hbase1k', 'f'
for i in '1'..'1000' do for j in '1'..'2' do put 'hbase10k', "row#{i}", "f:c#{j}", "#{i}#{j}" end end
```   

(2) Map hbase table with sparksql table in hbase-sql shell
```
$SPARK_HBASE_Home/bin/hbase-sql
CREATE TABLE spark1k(rowkey STRING, a STRING, b STRING, PRIMARY KEY (rowkey)) MAPPED BY (hbase1k, COLS=[a=f.c1, b=f.c2]);
```

(3) Query:
```
   // test count *
   (1) select count(*) from spark1k

   // test group by
   (2) select a, b from spark1k where b > "980"
```
