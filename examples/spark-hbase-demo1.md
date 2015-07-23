## Example 1: Create and query SparkSQL table map to existing Hbase table
In this example, we create SparkSQL table and map it to a existing HBase table. (a single column map to hbase rowkey)

Steps:

(1) Create table and populate data in HBase shell
```
$HBase_Home/bin/hbase shell
create 'hbase_numbers', 'f'
for i in '1'..'100' do for j in '1'..'2' do put 'hbase_numbers', "row#{i}", "f:c#{j}", "#{i}#{j}" end end
```   

(2) Map hbase table with sparksql table in hbase-sql shell
```
$SPARK_HBASE_Home/bin/hbase-sql
CREATE TABLE numbers(rowkey STRING, a STRING, b STRING, PRIMARY KEY (rowkey)) MAPPED BY (hbase_numbers, COLS=[a=f.c1, b=f.c2]);
```

(3) Query:
```
   // test count *
   (1) select count(*) from numbers

   // test group by
   (2) select a, b from numbers where b > "980"
```
